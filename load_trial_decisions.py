import argparse
import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path

import requests

try:
    import psycopg
except ImportError as exc:
    raise ImportError(
        "psycopg is required. Install it with: pip install psycopg[binary]"
    ) from exc


OUTPUT_PATH = "trial_decisions_output.json"
SYNC_SOURCE = "uspto_trial_decisions_institution_outcomes"
PAGE_LIMIT = 100


def load_dotenv(path=".env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def parse_date(value):
    if not value:
        return None
    return date.fromisoformat(value)


def parse_datetime(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


def get_connection():
    required_vars = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [name for name in required_vars if not os.environ.get(name)]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Set PostgreSQL env vars in .env before running: {missing_list}")

    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trial_decisions (
                trial_decision_id TEXT PRIMARY KEY,
                appeal_number TEXT,
                trial_number TEXT,
                decision_issue_date DATE,
                trial_outcome_category TEXT,
                issue_type_bag JSONB,
                statute_and_rule_bag JSONB,
                last_modified_datetime TIMESTAMP,
                raw_json JSONB NOT NULL
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE trial_decisions
            ADD COLUMN IF NOT EXISTS issue_type_bag JSONB
            """
        )
        cur.execute(
            """
            ALTER TABLE trial_decisions
            ADD COLUMN IF NOT EXISTS statute_and_rule_bag JSONB
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                source_name TEXT PRIMARY KEY,
                last_seen_modified_datetime TIMESTAMP
            )
            """
        )
    conn.commit()


def get_checkpoint(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_seen_modified_datetime FROM sync_state WHERE source_name = %s",
            (SYNC_SOURCE,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_checkpoint(conn, checkpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_state (source_name, last_seen_modified_datetime)
            VALUES (%s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                last_seen_modified_datetime = EXCLUDED.last_seen_modified_datetime
            """,
            (SYNC_SOURCE, checkpoint),
        )
    conn.commit()


def build_trial_decision_id(record, decision_data):
    appeal_number = record.get("appealNumber") or record.get("trialNumber")
    issue_date = decision_data.get("decisionIssueDate")
    outcome = decision_data.get("trialOutcomeCategory")
    if appeal_number and issue_date and outcome:
        return f"{appeal_number}|{issue_date}|{outcome}"
    digest = hashlib.sha256(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return digest


def flatten_record(record):
    decision_data = record.get("decisionData", {})
    trial_decision_id = build_trial_decision_id(record, decision_data)
    return {
        "trial_decision_id": trial_decision_id,
        "appeal_number": record.get("appealNumber"),
        "trial_number": record.get("trialNumber"),
        "decision_issue_date": parse_date(decision_data.get("decisionIssueDate")),
        "trial_outcome_category": decision_data.get("trialOutcomeCategory"),
        "issue_type_bag": json.dumps(decision_data.get("issueTypeBag"))
        if decision_data.get("issueTypeBag") is not None
        else None,
        "statute_and_rule_bag": json.dumps(decision_data.get("statuteAndRuleBag"))
        if decision_data.get("statuteAndRuleBag") is not None
        else None,
        "last_modified_datetime": parse_datetime(
            record.get("lastModifiedDateTime") or decision_data.get("lastModifiedDateTime")
        ),
        "raw_json": json.dumps(record),
    }


def upsert_record(conn, flattened):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trial_decisions (
                trial_decision_id,
                appeal_number,
                trial_number,
                decision_issue_date,
                trial_outcome_category,
                issue_type_bag,
                statute_and_rule_bag,
                last_modified_datetime,
                raw_json
            ) VALUES (
                %(trial_decision_id)s,
                %(appeal_number)s,
                %(trial_number)s,
                %(decision_issue_date)s,
                %(trial_outcome_category)s,
                %(issue_type_bag)s::jsonb,
                %(statute_and_rule_bag)s::jsonb,
                %(last_modified_datetime)s,
                %(raw_json)s::jsonb
            )
            ON CONFLICT (trial_decision_id) DO UPDATE SET
                appeal_number = EXCLUDED.appeal_number,
                trial_number = EXCLUDED.trial_number,
                decision_issue_date = EXCLUDED.decision_issue_date,
                trial_outcome_category = EXCLUDED.trial_outcome_category,
                issue_type_bag = EXCLUDED.issue_type_bag,
                statute_and_rule_bag = EXCLUDED.statute_and_rule_bag,
                last_modified_datetime = EXCLUDED.last_modified_datetime,
                raw_json = EXCLUDED.raw_json
            """,
            flattened,
        )


def fetch_page(session, api_key, offset):
    url = "https://api.uspto.gov/api/v1/patent/trials/decisions/search"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }
    payload = {
        "q": None,
        "filters": [
            {
                "name": "decisionData.trialOutcomeCategory",
                "value": ["Institution Denied", "Institution Granted"],
            },
            {
                "name": "trialMetaData.trialTypeCode",
                "value": ["IPR"],
            }
        ],
        "rangeFilters": [],
        "pagination": {"offset": offset, "limit": PAGE_LIMIT},
        "sort": [
            {
                "field": "decisionData.decisionIssueDate",
                "order": "Desc",
            }
        ],
    }
    response = session.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_records(page_data):
    for key in (
        "patentTrialDocumentDataBag",
        "patentTrialDecisionDataBag",
        "trialDecisionDataBag",
        "decisionDataBag",
        "data",
    ):
        value = page_data.get(key)
        if isinstance(value, list):
            return value
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the sync checkpoint and re-upsert all matching trial decisions.",
    )
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("USPTO_API_KEY")
    if not api_key:
        raise ValueError("Set USPTO_API_KEY in .env before running this script.")

    session = requests.Session()
    with get_connection() as conn:
        ensure_schema(conn)

        checkpoint = get_checkpoint(conn)
        if args.full_refresh:
            checkpoint = None
        newest_seen = checkpoint
        synced_records = []
        total_count = None
        offset = 0
        pages_fetched = 0
        reached_checkpoint = False

        while True:
            page_data = fetch_page(session, api_key, offset)
            pages_fetched += 1
            if total_count is None:
                total_count = page_data.get("count", 0)

            page_records = extract_records(page_data)
            if not page_records:
                break

            for record in page_records:
                flattened = flatten_record(record)
                modified_at = flattened["last_modified_datetime"] or datetime.combine(
                    flattened["decision_issue_date"], datetime.min.time()
                ) if flattened["decision_issue_date"] else None

                if checkpoint and modified_at and modified_at <= checkpoint:
                    reached_checkpoint = True
                    break

                upsert_record(conn, flattened)
                synced_records.append(record)

                if modified_at and (newest_seen is None or modified_at > newest_seen):
                    newest_seen = modified_at

            conn.commit()
            print(
                f"Fetched page {pages_fetched} at offset {offset}; "
                f"synced {len(synced_records)} of approx {total_count} trial decision records so far",
                flush=True,
            )

            if reached_checkpoint:
                break

            offset += PAGE_LIMIT
            if not checkpoint and offset >= total_count:
                break

        if newest_seen:
            set_checkpoint(conn, newest_seen)

        output = {
            "count": total_count,
            "records_synced_this_run": len(synced_records),
            "last_seen_modified_datetime": newest_seen.isoformat(sep=" ") if newest_seen else None,
            "trialDecisionData": synced_records,
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=2, default=str)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trial_decisions")
            table_count = cur.fetchone()[0]

    print(
        f"Fetched {pages_fetched} pages, synced {len(synced_records)} records, "
        f"trial_decisions table now has {table_count} rows, checkpoint={output['last_seen_modified_datetime']}"
    )


if __name__ == "__main__":
    main()
