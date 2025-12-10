# GitHub Stars Crawler

This repository contains a GitHub Stars crawler that fetches metadata and stargazers count for public repositories using REST + GraphQL, and stores snapshots in Postgres.

## Setup

1. Create `.env` with optional GitHub token:

```env
GITHUB_TOKEN=ghp_XXXX
DB_DSN=postgresql://postgres:postgres@localhost:5432/postgres
TARGET_REPOS=100000
GRAPHQL_BATCH=20

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/crawl_stars_graphql.py

