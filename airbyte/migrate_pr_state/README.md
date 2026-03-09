# migrate_pr_state

Migrate Airbyte metadata for Jira pull requests and source actor definition.

This script performs two independent operations:

1. Migrates `pull_requests` stream state from the old flat shape (CDK v6) to the new nested shape (CDK v7).
2. Updates the source actor's definition to `Span Jira cdk7`.

Each operation can be skipped without blocking the other:

- If a connection has no `pull_requests` state row, state migration is skipped and reported.
- If the actor already has the target definition, actor update is skipped and reported.

## Requirements

- Python `>=3.13`
- `psycopg2-binary` (declared in `pyproject.toml`)
- Network access to the Airbyte Postgres database

## Run

From this directory:

```bash
uv run python migrate.py --help
```

You must pass exactly one identifier:

- `--connection-id <uuid>`
- `--source-id <uuid>`

## Examples

Dry run by connection id:

```bash
uv run python migrate.py \
  --connection-id 74af7f57-60a1-422a-a153-f5ba322a9613 \
  --dry-run \
  --db-host localhost \
  --db-port 5532 \
  --db-user docker \
  --db-name airbyte
```

Dry run by source id (applies state migration across all connections for that source):

```bash
uv run python migrate.py \
  --source-id 196ab061-2f2a-4f6b-831b-71522c9e660a \
  --dry-run \
  --db-host localhost \
  --db-port 5532 \
  --db-user docker \
  --db-name airbyte
```

Apply changes:

```bash
uv run python migrate.py --connection-id <connection-uuid> --db-password '<password>'
```

## Authentication

The script resolves password in this order:

1. `--db-password`
2. `AIRBYTE_DB_PASSWORD` environment variable
3. Interactive prompt (TTY only)

For non-interactive usage, provide `--db-password` or set `AIRBYTE_DB_PASSWORD`.

## What gets logged

- Prints a summary of detected changes and skipped items.
- Writes a log file in this folder:
  - `migrate_connection_<id>_<timestamp>.log`
  - `migrate_source_<id>_<timestamp>.log`
