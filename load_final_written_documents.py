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


OUTPUT_PATH = "final_written_documents_output.json"
SYNC_SOURCE = "uspto_final_written_decision_documents"
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
            CREATE TABLE IF NOT EXISTS final_written_decision_documents (
                final_written_document_id TEXT PRIMARY KEY,
                trial_number TEXT,
                document_identifier TEXT,
                document_filing_date DATE,
                document_title_text TEXT,
                document_type_description_text TEXT,
                decision_issue_date DATE,
                institution_decision_date DATE,
                last_modified_datetime TIMESTAMP,
                raw_json JSONB NOT NULL
            )
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


def build_document_id(record):
    trial_number = record.get("trialNumber")
    document_identifier = record.get("documentData", {}).get("documentIdentifier")
    if trial_number and document_identifier:
        return f"{trial_number}|{document_identifier}"
    digest = hashlib.sha256(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return digest


def is_final_written_decision_document(record):
    document_data = record.get("documentData", {})
    title = (document_data.get("documentTitleText") or "").lower()
    doc_type = (document_data.get("documentTypeDescriptionText") or "").lower()
    return "final written decision" in title or "final written decision" in doc_type


def flatten_record(record):
    document_data = record.get("documentData", {})
    decision_data = record.get("decisionData", {})
    trial_meta_data = record.get("trialMetaData", {})
    return {
        "final_written_document_id": build_document_id(record),
        "trial_number": record.get("trialNumber"),
        "document_identifier": document_data.get("documentIdentifier"),
        "document_filing_date": parse_date(document_data.get("documentFilingDate")),
        "document_title_text": document_data.get("documentTitleText"),
        "document_type_description_text": document_data.get("documentTypeDescriptionText"),
        "decision_issue_date": parse_date(decision_data.get("decisionIssueDate")),
        "institution_decision_date": parse_date(trial_meta_data.get("institutionDecisionDate")),
        "last_modified_datetime": parse_datetime(record.get("lastModifiedDateTime")),
        "raw_json": json.dumps(record),
    }


def upsert_record(conn, flattened):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO final_written_decision_documents (
                final_written_document_id,
                trial_number,
                document_identifier,
                document_filing_date,
                document_title_text,
                document_type_description_text,
                decision_issue_date,
                institution_decision_date,
                last_modified_datetime,
                raw_json
            ) VALUES (
                %(final_written_document_id)s,
                %(trial_number)s,
                %(document_identifier)s,
                %(document_filing_date)s,
                %(document_title_text)s,
                %(document_type_description_text)s,
                %(decision_issue_date)s,
                %(institution_decision_date)s,
                %(last_modified_datetime)s,
                %(raw_json)s::jsonb
            )
            ON CONFLICT (final_written_document_id) DO UPDATE SET
                trial_number = EXCLUDED.trial_number,
                document_identifier = EXCLUDED.document_identifier,
                document_filing_date = EXCLUDED.document_filing_date,
                document_title_text = EXCLUDED.document_title_text,
                document_type_description_text = EXCLUDED.document_type_description_text,
                decision_issue_date = EXCLUDED.decision_issue_date,
                institution_decision_date = EXCLUDED.institution_decision_date,
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
                "name": "trialMetaData.trialTypeCode",
                "value": ["IPR"],
            },
            {
                "name": "trialMetaData.trialStatusCategory",
                "value": ["Final Written Decision"],
            },
        ],
        "rangeFilters": [],
        "pagination": {"offset": offset, "limit": PAGE_LIMIT},
        "sort": [
            {
                "field": "lastModifiedDateTime",
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
        help="Ignore the sync checkpoint and re-upsert all matching final written decision documents.",
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
                    flattened["document_filing_date"], datetime.min.time()
                ) if flattened["document_filing_date"] else None

                if checkpoint and modified_at and modified_at <= checkpoint:
                    reached_checkpoint = True
                    break

                if not is_final_written_decision_document(record):
                    continue

                upsert_record(conn, flattened)
                synced_records.append(record)

                if modified_at and (newest_seen is None or modified_at > newest_seen):
                    newest_seen = modified_at

            conn.commit()
            print(
                f"Fetched page {pages_fetched} at offset {offset}; "
                f"synced {len(synced_records)} of approx {total_count} final written decision documents so far",
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
            "finalWrittenDecisionDocuments": synced_records,
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=2, default=str)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM final_written_decision_documents")
            table_count = cur.fetchone()[0]

    print(
        f"Fetched {pages_fetched} pages, synced {len(synced_records)} records, "
        f"final_written_decision_documents table now has {table_count} rows, "
        f"checkpoint={output['last_seen_modified_datetime']}"
    )


if __name__ == "__main__":
    main()
