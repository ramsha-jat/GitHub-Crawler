#!/usr/bin/env python3
# scripts/crawl_stars_graphql.py
"""
GraphQL-based GitHub stars crawler.
- Enumerate repos via REST /repositories (cursor 'since')
- Batch GraphQL repository(owner,name) queries to fetch stargazerCount and other fields
- Respect GraphQL rateLimit { cost remaining resetAt }
- Retry transient failures
- Upsert into Postgres and append daily snapshot
"""
from dotenv import load_dotenv
import os
import time
import logging
import random
from datetime import datetime, date, timezone
import json

import requests
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
load_dotenv()
# ---------- CONFIG via env ----------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")        # Locally set a PAT for faster runs; Actions uses default token
TARGET_REPOS = int(os.getenv("TARGET_REPOS", "100"))  # Change to 100000 in workflow
GRAPHQL_BATCH = int(os.getenv("GRAPHQL_BATCH", "20"))
DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")

REST_API = "https://api.github.com"
GRAPHQL_API = "https://api.github.com/graphql"

# Threshold under which we will sleep until reset
RATE_LIMIT_SAFETY = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
session = requests.Session()
session.headers.update({"Accept": "application/vnd.github+json", "User-Agent": "gh-crawler"})
if GITHUB_TOKEN:
    # use Bearer for GraphQL per recommended header
    session.headers.update({"Authorization": f"Bearer {GITHUB_TOKEN}"})

def jitter():
    return random.uniform(0.5, 1.5)

def pg_conn():
    return psycopg2.connect(DB_DSN)

# Ensure tables exist and compatibility columns present
def ensure_schema(conn):
    ddl_path = os.path.join(os.path.dirname(__file__), "..", "sql", "setup_schema.sql")
    try:
        if os.path.exists(ddl_path):
            with open(ddl_path, "r", encoding="utf8") as fh:
                ddl = fh.read()
            with conn.cursor() as cur:
                cur.execute(ddl)
                conn.commit()
    except Exception as e:
        logging.warning("Could not execute DDL file: %s", e)
        conn.rollback()

    # Add is_archived/is_fork columns if missing (keeps compatibility with older schema)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='repos' AND column_name IN ('is_archived','is_fork');
        """)
        existing = {r[0] for r in cur.fetchall()}
        if 'is_archived' not in existing:
            cur.execute("ALTER TABLE repos ADD COLUMN IF NOT EXISTS is_archived BOOLEAN DEFAULT FALSE;")
        if 'is_fork' not in existing:
            cur.execute("ALTER TABLE repos ADD COLUMN IF NOT EXISTS is_fork BOOLEAN DEFAULT FALSE;")
        conn.commit()

# -------------------- REST enumerator --------------------
def rest_list_public_repos(since=None):
    url = f"{REST_API}/repositories"
    params = {"since": since} if since else {}
    r = session.get(url, params=params, timeout=30)
    # handle REST rate limit politely (if 0, sleep until reset)
    try:
        remaining = int(r.headers.get("X-RateLimit-Remaining", "1"))
        reset = int(r.headers.get("X-RateLimit-Reset", "0"))
        if remaining == 0:
            sleep_for = max(1, reset - int(time.time()) + 2)
            logging.warning("REST rate exhausted — sleeping %s seconds", sleep_for)
            time.sleep(sleep_for + jitter())
    except Exception:
        pass
    r.raise_for_status()
    return r.json()

# -------------------- GraphQL utilities --------------------
def graphql_escape(s: str) -> str:
    # Escape double quotes and backslashes for safe interpolation
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')

def build_graphql_query(pairs):
    fields = """
        databaseId
        name
        owner { login }
        stargazerCount
        url
        description
        isArchived
        isFork
        pushedAt
        createdAt
        updatedAt
        primaryLanguage { name }
    """

    repo_blocks = []
    for i, (owner, name) in enumerate(pairs):
        owner_s = graphql_escape(owner)
        name_s = graphql_escape(name)
        repo_blocks.append(
            f'r{i}: repository(owner: "{owner_s}", name: "{name_s}") {{ {fields} }}'
        )

   
    return (
        "query {\n"
        "  rateLimit { cost remaining resetAt }\n"
        + "\n".join(repo_blocks)
        + "\n}"
    )


@retry(stop=stop_after_attempt(6), wait=wait_exponential(multiplier=1, min=2, max=60), retry=retry_if_exception_type(Exception))
def graphql_call(query):
    r = session.post(GRAPHQL_API, json={"query": query}, timeout=60)
    if r.status_code in (502, 503, 504):
        raise Exception(f"gateway {r.status_code}")
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        # raise to retry (tenacity)
        raise Exception(f"GraphQL errors: {data['errors']}")
    return data

# -------------------- DB write ops --------------------
def upsert_repos(conn, rows):
    """
    rows: list of tuples in order:
      github_repo_id, owner, name, stargazers_count, repo_url, description,
      is_archived, is_fork, language, pushed_at, created_at, updated_at, last_crawled_at, meta_dict
    """
    sql = """
    INSERT INTO repos (
        github_repo_id, owner, name, stargazers_count,
        repo_url, description, is_archived, is_fork, language,
        pushed_at, created_at, updated_at, last_crawled_at, meta
    ) VALUES %s
    ON CONFLICT (github_repo_id) DO UPDATE SET
      stargazers_count = EXCLUDED.stargazers_count,
      repo_url = EXCLUDED.repo_url,
      description = EXCLUDED.description,
      is_archived = EXCLUDED.is_archived,
      is_fork = EXCLUDED.is_fork,
      language = EXCLUDED.language,
      pushed_at = EXCLUDED.pushed_at,
      created_at = EXCLUDED.created_at,
      updated_at = EXCLUDED.updated_at,
      last_crawled_at = now(),
      meta = repos.meta || EXCLUDED.meta;
    """
    # convert meta dict to Json adapter
    pg_rows = []
    for r in rows:
        meta = r[-1] if isinstance(r[-1], dict) else {}
        core = tuple(r[:-1]) + (psycopg2.extras.Json(meta),)
        pg_rows.append(core)

    with conn.cursor() as cur:
        execute_values(cur, sql, pg_rows, page_size=100)
        conn.commit()

def insert_history(conn, hist_rows):
    sql = "INSERT INTO repo_stars_history (github_repo_id, snapshot_date, stargazers_count) VALUES %s ON CONFLICT DO NOTHING;"
    with conn.cursor() as cur:
        execute_values(cur, sql, hist_rows, page_size=200)
        conn.commit()

def iso_to_ts(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# -------------------- Orchestration --------------------
def crawl(target_count=100, batch_size=20):
    conn = pg_conn()
    ensure_schema(conn)

    processed = 0
    since = None
    buffer = []  # items: (owner, name, gid, rest_item)

    logging.info("Crawl start: target=%s batch=%s", target_count, batch_size)

    while processed < target_count:
        page = rest_list_public_repos(since)
        if not page:
            logging.info("REST returned empty page — finished early")
            break

        for item in page:
            gid = item.get("id")
            owner = item["owner"]["login"]
            name = item["name"]
            buffer.append((owner, name, gid, item))

            # when enough buffered items, send a GraphQL batch
            if len(buffer) >= batch_size:
                pairs = [(o, n) for (o, n, _, _) in buffer[:batch_size]]
                q = build_graphql_query(pairs)
                resp = None
                try:
                    resp = graphql_call(q)
                except Exception as e:
                    logging.exception("GraphQL call failed after retries: %s", e)
                    # fallback: we will use REST object's stargazers_count where possible
                    resp = {"data": {}}

                rows = []
                hist_rows = []
                for i in range(len(pairs)):
                    key = f"r{i}"
                    repo_obj = resp.get("data", {}).get(key) if resp.get("data") else None
                    owner_, name_, gid_, rest_item = buffer[i]
                    if repo_obj:
                        stargazers = repo_obj.get("stargazerCount", 0)
                        repo_url = repo_obj.get("url")
                        description = repo_obj.get("description")
                        is_archived = repo_obj.get("isArchived", False)
                        is_fork = repo_obj.get("isFork", False)
                        lang = None
                        if repo_obj.get("primaryLanguage"):
                            lang = repo_obj["primaryLanguage"].get("name")
                        pushed_at = iso_to_ts(repo_obj.get("pushedAt"))
                        created_at = iso_to_ts(repo_obj.get("createdAt"))
                        updated_at = iso_to_ts(repo_obj.get("updatedAt"))
                        meta = {"queried_owner": owner_, "queried_name": name_}
                    else:
                        # fallback to REST fields
                        stargazers = rest_item.get("stargazers_count", 0)
                        repo_url = rest_item.get("html_url")
                        description = rest_item.get("description")
                        is_archived = rest_item.get("archived", False)
                        is_fork = rest_item.get("fork", False)
                        lang = rest_item.get("language")
                        pushed_at = iso_to_ts(rest_item.get("pushed_at"))
                        created_at = iso_to_ts(rest_item.get("created_at"))
                        updated_at = iso_to_ts(rest_item.get("updated_at"))
                        meta = {"fallback": True}

                    rows.append((
                        gid_, owner_, name_, stargazers,
                        repo_url, description, is_archived, is_fork, lang,
                        pushed_at, created_at, updated_at, datetime.now(timezone.utc),
                        meta
                    ))
                    hist_rows.append((gid_, date.today(), stargazers))

                # write to DB
                try:
                    upsert_repos(conn, rows)
                    insert_history(conn, hist_rows)
                except Exception as e:
                    logging.exception("DB write failed: %s", e)
                    conn.rollback()

                processed += len(rows)
                logging.info("Processed %d/%d", processed, target_count)

                # check GraphQL rateLimit if present and sleep if low
                rl = resp.get("data", {}).get("rateLimit") if resp.get("data") else None
                if rl:
                    try:
                        remaining = int(rl.get("remaining", 0))
                        reset_at = rl.get("resetAt")
                        logging.info("GraphQL rateLimit: cost=%s remaining=%s resetAt=%s", rl.get("cost"), remaining, reset_at)
                        if remaining < RATE_LIMIT_SAFETY and reset_at:
                            reset_ts = datetime.fromisoformat(reset_at.replace("Z", "+00:00")).timestamp()
                            sleep_for = max(5, reset_ts - time.time() + 5)
                            logging.warning("GraphQL credits low — sleeping %.0f seconds", sleep_for)
                            time.sleep(sleep_for + jitter())
                    except Exception:
                        logging.warning("Could not parse GraphQL rateLimit; sleeping a small amount")
                        time.sleep(10 + jitter())

                # consume processed slice from buffer
                buffer = buffer[batch_size:]
                time.sleep(0.5 + jitter())

            if processed >= target_count:
                break

        # advance since cursor to last REST item id
        last = page[-1]
        since = last.get("id")
        time.sleep(0.5 + jitter())

    logging.info("Crawl complete. processed=%d", processed)
    conn.close()

if __name__ == "__main__":
    crawl(TARGET_REPOS, GRAPHQL_BATCH)
