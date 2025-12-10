-- sql/setup_schema.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- current state (one row per repo)
CREATE TABLE IF NOT EXISTS repos (
    github_repo_id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    full_name TEXT GENERATED ALWAYS AS (owner || '/' || name) STORED,
    stargazers_count INTEGER NOT NULL DEFAULT 0,
    repo_url TEXT,
    description TEXT,
    archived BOOLEAN DEFAULT FALSE,      -- legacy column (kept for backwards compatibility)
    fork BOOLEAN DEFAULT FALSE,          -- legacy column
    is_archived BOOLEAN DEFAULT FALSE,   -- current boolean naming used by code
    is_fork BOOLEAN DEFAULT FALSE,
    language TEXT,
    pushed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    last_crawled_at TIMESTAMPTZ,
    meta JSONB DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_repos_fullname ON repos(full_name);

-- daily snapshot history (one row per repo per date)
CREATE TABLE IF NOT EXISTS repo_stars_history (
    github_repo_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    stargazers_count INTEGER NOT NULL,
    crawled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (github_repo_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_repo_stars_history_date ON repo_stars_history(snapshot_date);
