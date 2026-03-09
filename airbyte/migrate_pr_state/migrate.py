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


def get_target_definition_id(cur) -> str:
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
    return str(def_rows[0][0])


def get_connection_ids(cur, connection_id: str | None, source_id: str | None):
    if connection_id:
        identifier_type = "connection"
        identifier_value = connection_id
        cur.execute(
            "SELECT a.id, ad.id, ad.name FROM connection c "
            "INNER JOIN actor a ON c.source_id = a.id "
            "INNER JOIN actor_definition ad ON a.actor_definition_id = ad.id "
            "WHERE c.id = %s",
            (connection_id,),
        )
        actor_row = cur.fetchone()
        if not actor_row:
            print(f"No actor found for connection {connection_id}")
            sys.exit(1)
        actor_id, current_def_id, current_def_name = actor_row
        connection_ids = [connection_id]
    else:
        identifier_type = "source"
        identifier_value = source_id
        cur.execute(
            "SELECT a.id, ad.id, ad.name FROM actor a "
            "INNER JOIN actor_definition ad ON a.actor_definition_id = ad.id "
            "WHERE a.id = %s",
            (source_id,),
        )
        actor_row = cur.fetchone()
        if not actor_row:
            print(f"No actor found for source {source_id}")
            sys.exit(1)
        actor_id, current_def_id, current_def_name = actor_row
        cur.execute("SELECT id FROM connection WHERE source_id = %s", (source_id,))
        connection_ids = [str(r[0]) for r in cur.fetchall()]

    return identifier_type, identifier_value, actor_id, current_def_id, current_def_name, connection_ids


def update_connections_state(cur, connection_ids: list[str], date: str | None, source_id: str | None, apply_updates: bool):
    state_update_sql = "UPDATE state SET state = %s, updated_at = NOW() WHERE id = %s AND connection_id = %s"
    state_updates = []
    state_skip_messages = []

    if source_id and not connection_ids:
        state_skip_messages.append(
            f"No connections found for source {source_id}; skipping pull_requests state migration."
        )

    for connection_id in connection_ids:
        cur.execute(
            "SELECT id, state FROM state WHERE connection_id = %s AND stream_name = 'pull_requests'",
            (connection_id,),
        )
        row = cur.fetchone()
        if not row:
            state_skip_messages.append(
                f"No pull_requests state found for connection {connection_id}; skipping state migration."
            )
            continue

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
        state_date = date or current_state["updated"][:10]
        new_state = build_new_state(state_date, tz_offset)
        state_updates.append(
            {
                "connection_id": connection_id,
                "row_id": row_id,
                "current_state": current_state,
                "new_state": new_state,
                "new_state_json": json.dumps(new_state),
            }
        )

    state_changes_applied = False
    if apply_updates:
        for state_update in state_updates:
            cur.execute(
                state_update_sql,
                (state_update["new_state_json"], state_update["row_id"], state_update["connection_id"]),
            )
            state_changes_applied = True

    return state_update_sql, state_updates, state_skip_messages, state_changes_applied


def update_actor_definition_id(cur, actor_id, current_def_id, target_def_id: str, apply_update: bool):
    definition_update_sql = "UPDATE actor SET actor_definition_id = %s, updated_at = NOW() WHERE id = %s"
    actor_update_needed = str(current_def_id) != target_def_id

    actor_update_applied = False
    if apply_update and actor_update_needed:
        cur.execute(definition_update_sql, (target_def_id, actor_id))
        actor_update_applied = True

    return definition_update_sql, actor_update_needed, actor_update_applied


def main():
    parser = argparse.ArgumentParser(description="Migrate pull_requests stream state")
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--connection-id", help="UUID of the Airbyte connection")
    id_group.add_argument("--source-id", help="UUID of the Airbyte source actor")
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
            target_def_id = get_target_definition_id(cur)
            identifier_type, identifier_value, actor_id, current_def_id, current_def_name, connection_ids = get_connection_ids(
                cur, args.connection_id, args.source_id
            )
            state_update_sql, state_updates, state_skip_messages, state_changes_applied = update_connections_state(
                cur, connection_ids, args.date, args.source_id, apply_updates=not args.dry_run
            )
            definition_update_sql, actor_update_needed, actor_update_applied = update_actor_definition_id(
                cur, actor_id, current_def_id, target_def_id, apply_update=not args.dry_run
            )

            output = []
            output.append(f"{identifier_type}_id: {identifier_value}")
            output.append(f"Connections checked for state migration: {len(connection_ids)}")
            for state_skip_message in state_skip_messages:
                output.append(state_skip_message)
            for state_update in state_updates:
                output.append(f"\nConnection: {state_update['connection_id']}")
                output.append(f"row_id: {state_update['row_id']}")
                output.append(f"\nOld state:\n{json.dumps(state_update['current_state'], indent=2)}")
                output.append(f"\nNew state:\n{json.dumps(state_update['new_state'], indent=2)}")
            output.append(f"\nActor: {actor_id}")
            output.append(f"Current definition: {current_def_id} ({current_def_name})")
            output.append(f"Target definition:  {target_def_id} ({TARGET_DEFINITION_NAME})")
            if not actor_update_needed:
                output.append("Actor already has target definition; skipping actor definition update.")

            output.append("\n[DRY RUN] Would execute:" if args.dry_run else "\nExecuted:")
            if state_updates:
                output.append(f"  {state_update_sql}")
                for state_update in state_updates:
                    output.append(
                        "  params: "
                        f"({state_update['new_state_json']}, {state_update['row_id']}, {state_update['connection_id']})"
                    )
            else:
                output.append("  [SKIPPED] No pull_requests state row to migrate")
            for state_skip_message in state_skip_messages:
                output.append(f"  [SKIPPED] {state_skip_message}")

            if actor_update_needed:
                output.append(f"  {definition_update_sql}")
                output.append(f"  params: ({target_def_id}, {actor_id})")
            else:
                output.append("  [SKIPPED] Actor already has target definition id")

            if args.dry_run:
                print("\n".join(output))
                return

            changes_applied = state_changes_applied or actor_update_applied

            if changes_applied:
                conn.commit()
                output.append("\nMigration applied successfully")
            else:
                output.append("\nNo changes were required")

            print("\n".join(output))

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            logfile = f"migrate_{identifier_type}_{identifier_value}_{ts}.log"
            with open(logfile, "w") as f:
                f.write("\n".join(output) + "\n")
            print(f"\nLog written to {logfile}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
