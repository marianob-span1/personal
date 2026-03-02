"""Migrate Airbyte connection metadata for pull requests.

This script can perform two independent updates:
1. pull_requests stream state migration (old flat format -> new nested format)
2. source actor definition migration to the configured target definition

Either update may be skipped without blocking the other:
- If no pull_requests state exists, actor definition update can still run.
- If actor already uses the target definition, state migration can still run.
"""

import argparse
import getpass
import json
import os
import re
import sys
from datetime import datetime, timezone

import psycopg2

TARGET_DEFINITION_NAME = "Span Jira cdk7"


def extract_tz_offset(timestamp: str) -> str:
    match = re.search(r"[+-]\d{4}$", timestamp)
    if not match:
        print(f"Cannot extract timezone offset from: {timestamp}")
        sys.exit(1)
    return match.group()


def build_new_state(date: str, tz_offset: str) -> dict:
    ts = f"{date}T00:00:00.000{tz_offset}"
    return {
        "state": {"updated": ts},
        "parent_state": {
            "__pull_requests_issues_substream": {
                "state": {"updated": ts},
                "states": [{"cursor": {"updated": ts}, "partition": {}}],
                "parent_state": {},
                "lookback_window": 0,
                "use_global_cursor": False,
            }
        },
        "lookback_window": 0,
        "use_global_cursor": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Migrate pull_requests stream state")
    parser.add_argument("connection_id", help="UUID of the Airbyte connection")
    parser.add_argument("--date", help="Date for new state (YYYY-MM-DD). Defaults to date from old state")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without applying")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default="5532", type=int)
    parser.add_argument("--db-user", default="docker")
    parser.add_argument(
        "--db-password",
        default=None,
        help="DB password (prefer AIRBYTE_DB_PASSWORD env var in production)",
    )
    parser.add_argument("--db-name", default="airbyte")
    args = parser.parse_args()

    db_password = args.db_password or os.getenv("AIRBYTE_DB_PASSWORD")
    if not db_password:
        if not sys.stdin.isatty():
            print(
                "Database password not provided. Set AIRBYTE_DB_PASSWORD or pass --db-password "
                "when running non-interactively."
            )
            sys.exit(1)
        db_password = getpass.getpass("Database password: ")

    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=db_password,
        dbname=args.db_name,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, state FROM state WHERE connection_id = %s AND stream_name = 'pull_requests'",
                (args.connection_id,),
            )
            row = cur.fetchone()

            row_id = None
            current_state = None
            new_state = None
            new_state_json = None
            state_update_sql = None
            state_status_message = None

            if not row:
                state_status_message = (
                    f"No pull_requests state found for connection {args.connection_id}; "
                    "skipping state migration and continuing with actor definition update."
                )
            else:
                row_id, current_state = row

                if not isinstance(current_state, dict):
                    current_state = json.loads(current_state)

                if "state" in current_state:
                    print("State already appears to be in new format (has 'state' key). Aborting.")
                    sys.exit(1)

                if "updated" not in current_state:
                    print(f"Unexpected state format (no 'updated' key): {json.dumps(current_state)}")
                    sys.exit(1)

                tz_offset = extract_tz_offset(current_state["updated"])
                date = args.date or current_state["updated"][:10]
                new_state = build_new_state(date, tz_offset)
                new_state_json = json.dumps(new_state)
                state_update_sql = "UPDATE state SET state = %s, updated_at = NOW() WHERE id = %s AND connection_id = %s"

            cur.execute(
                "SELECT id FROM actor_definition WHERE name = %s",
                (TARGET_DEFINITION_NAME,),
            )
            def_rows = cur.fetchall()
            if not def_rows:
                print(f"No actor_definition found with name '{TARGET_DEFINITION_NAME}'")
                sys.exit(1)
            if len(def_rows) > 1:
                ids = [r[0] for r in def_rows]
                print(f"Multiple actor_definitions found for '{TARGET_DEFINITION_NAME}': {ids}")
                sys.exit(1)
            target_def_id = str(def_rows[0][0])

            cur.execute(
                "SELECT a.id, ad.id, ad.name FROM connection c "
                "INNER JOIN actor a ON c.source_id = a.id "
                "INNER JOIN actor_definition ad ON a.actor_definition_id = ad.id "
                "WHERE c.id = %s",
                (args.connection_id,),
            )
            actor_row = cur.fetchone()
            if not actor_row:
                print(f"No actor found for connection {args.connection_id}")
                sys.exit(1)

            actor_id, current_def_id, current_def_name = actor_row
            definition_update_sql = "UPDATE actor SET actor_definition_id = %s, updated_at = NOW() WHERE id = %s"
            actor_update_needed = str(current_def_id) != target_def_id

            output = []
            output.append(f"connection_id: {args.connection_id}")
            if state_status_message:
                output.append(state_status_message)
            if state_update_sql:
                output.append(f"row_id: {row_id}")
                output.append(f"\nOld state:\n{json.dumps(current_state, indent=2)}")
                output.append(f"\nNew state:\n{json.dumps(new_state, indent=2)}")
            output.append(f"\nActor: {actor_id}")
            output.append(f"Current definition: {current_def_id} ({current_def_name})")
            output.append(f"Target definition:  {target_def_id} ({TARGET_DEFINITION_NAME})")
            if not actor_update_needed:
                output.append("Actor already has target definition; skipping actor definition update.")

            output.append("\n[DRY RUN] Would execute:" if args.dry_run else "\nExecuted:")
            if state_update_sql:
                output.append(f"  {state_update_sql}")
                output.append(f"  params: ({new_state_json}, {row_id}, {args.connection_id})")
            else:
                output.append("  [SKIPPED] No pull_requests state row to migrate")

            if actor_update_needed:
                output.append(f"  {definition_update_sql}")
                output.append(f"  params: ({target_def_id}, {actor_id})")
            else:
                output.append("  [SKIPPED] Actor already has target definition id")

            if args.dry_run:
                print("\n".join(output))
                return

            changes_applied = False
            if state_update_sql:
                cur.execute(state_update_sql, (new_state_json, row_id, args.connection_id))
                changes_applied = True
            if actor_update_needed:
                cur.execute(definition_update_sql, (target_def_id, actor_id))
                changes_applied = True

            if changes_applied:
                conn.commit()
                output.append("\nMigration applied successfully")
            else:
                output.append("\nNo changes were required")

            print("\n".join(output))

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            logfile = f"migrate_{args.connection_id}_{ts}.log"
            with open(logfile, "w") as f:
                f.write("\n".join(output) + "\n")
            print(f"\nLog written to {logfile}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
