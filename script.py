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


OUTPUT_PATH = "output.json"
SYNC_SOURCE = "uspto_ipr_proceedings"
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


def normalize_text(value):
    if value in (None, ""):
        return None
    return str(value)


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
            CREATE TABLE IF NOT EXISTS proceedings (
                trial_number TEXT PRIMARY KEY,
                last_modified_datetime TIMESTAMP,
                patent_owner_patent_number TEXT,
                patent_owner_real_party_in_interest_name TEXT,
                patent_owner_counsel_name TEXT,
                patent_owner_grant_date DATE,
                patent_owner_technology_center_number TEXT,
                patent_owner_group_art_unit_number TEXT,
                patent_owner_application_number_text TEXT,
                patent_owner_inventor_name TEXT,
                trial_meta_accorded_filing_date DATE,
                trial_meta_termination_date DATE,
                trial_meta_trial_type_code TEXT,
                trial_meta_latest_decision_date DATE,
                trial_meta_file_download_uri TEXT,
                trial_meta_institution_decision_date DATE,
                trial_meta_trial_status_category TEXT,
                trial_meta_trial_last_modified_date DATE,
                trial_meta_petition_filing_date DATE,
                trial_meta_trial_last_modified_datetime TIMESTAMP,
                regular_petitioner_real_party_in_interest_name TEXT,
                regular_petitioner_counsel_name TEXT,
                raw_json JSONB NOT NULL
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE proceedings
            ALTER COLUMN patent_owner_technology_center_number TYPE TEXT
            USING patent_owner_technology_center_number::TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE proceedings
            ALTER COLUMN patent_owner_group_art_unit_number TYPE TEXT
            USING patent_owner_group_art_unit_number::TEXT
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


def flatten_record(record):
    patent_owner = record.get("patentOwnerData", {})
    trial_meta = record.get("trialMetaData", {})
    petitioner = record.get("regularPetitionerData", {})

    return {
        "trial_number": record.get("trialNumber"),
        "last_modified_datetime": parse_datetime(record.get("lastModifiedDateTime")),
        "patent_owner_patent_number": patent_owner.get("patentNumber"),
        "patent_owner_real_party_in_interest_name": patent_owner.get("realPartyInInterestName"),
        "patent_owner_counsel_name": patent_owner.get("counselName"),
        "patent_owner_grant_date": parse_date(patent_owner.get("grantDate")),
        "patent_owner_technology_center_number": normalize_text(
            patent_owner.get("technologyCenterNumber")
        ),
        "patent_owner_group_art_unit_number": normalize_text(
            patent_owner.get("groupArtUnitNumber")
        ),
        "patent_owner_application_number_text": patent_owner.get("applicationNumberText"),
        "patent_owner_inventor_name": patent_owner.get("inventorName"),
        "trial_meta_accorded_filing_date": parse_date(trial_meta.get("accordedFilingDate")),
        "trial_meta_termination_date": parse_date(trial_meta.get("terminationDate")),
        "trial_meta_trial_type_code": trial_meta.get("trialTypeCode"),
        "trial_meta_latest_decision_date": parse_date(trial_meta.get("latestDecisionDate")),
        "trial_meta_file_download_uri": trial_meta.get("fileDownloadURI"),
        "trial_meta_institution_decision_date": parse_date(trial_meta.get("institutionDecisionDate")),
        "trial_meta_trial_status_category": trial_meta.get("trialStatusCategory"),
        "trial_meta_trial_last_modified_date": parse_date(trial_meta.get("trialLastModifiedDate")),
        "trial_meta_petition_filing_date": parse_date(trial_meta.get("petitionFilingDate")),
        "trial_meta_trial_last_modified_datetime": parse_datetime(trial_meta.get("trialLastModifiedDateTime")),
        "regular_petitioner_real_party_in_interest_name": petitioner.get("realPartyInInterestName"),
        "regular_petitioner_counsel_name": petitioner.get("counselName"),
        "raw_json": json.dumps(record),
    }


def upsert_record(conn, flattened):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO proceedings (
                trial_number,
                last_modified_datetime,
                patent_owner_patent_number,
                patent_owner_real_party_in_interest_name,
                patent_owner_counsel_name,
                patent_owner_grant_date,
                patent_owner_technology_center_number,
                patent_owner_group_art_unit_number,
                patent_owner_application_number_text,
                patent_owner_inventor_name,
                trial_meta_accorded_filing_date,
                trial_meta_termination_date,
                trial_meta_trial_type_code,
                trial_meta_latest_decision_date,
                trial_meta_file_download_uri,
                trial_meta_institution_decision_date,
                trial_meta_trial_status_category,
                trial_meta_trial_last_modified_date,
                trial_meta_petition_filing_date,
                trial_meta_trial_last_modified_datetime,
                regular_petitioner_real_party_in_interest_name,
                regular_petitioner_counsel_name,
                raw_json
            ) VALUES (
                %(trial_number)s,
                %(last_modified_datetime)s,
                %(patent_owner_patent_number)s,
                %(patent_owner_real_party_in_interest_name)s,
                %(patent_owner_counsel_name)s,
                %(patent_owner_grant_date)s,
                %(patent_owner_technology_center_number)s,
                %(patent_owner_group_art_unit_number)s,
                %(patent_owner_application_number_text)s,
                %(patent_owner_inventor_name)s,
                %(trial_meta_accorded_filing_date)s,
                %(trial_meta_termination_date)s,
                %(trial_meta_trial_type_code)s,
                %(trial_meta_latest_decision_date)s,
                %(trial_meta_file_download_uri)s,
                %(trial_meta_institution_decision_date)s,
                %(trial_meta_trial_status_category)s,
                %(trial_meta_trial_last_modified_date)s,
                %(trial_meta_petition_filing_date)s,
                %(trial_meta_trial_last_modified_datetime)s,
                %(regular_petitioner_real_party_in_interest_name)s,
                %(regular_petitioner_counsel_name)s,
                %(raw_json)s::jsonb
            )
            ON CONFLICT (trial_number) DO UPDATE SET
                last_modified_datetime = EXCLUDED.last_modified_datetime,
                patent_owner_patent_number = EXCLUDED.patent_owner_patent_number,
                patent_owner_real_party_in_interest_name = EXCLUDED.patent_owner_real_party_in_interest_name,
                patent_owner_counsel_name = EXCLUDED.patent_owner_counsel_name,
                patent_owner_grant_date = EXCLUDED.patent_owner_grant_date,
                patent_owner_technology_center_number = EXCLUDED.patent_owner_technology_center_number,
                patent_owner_group_art_unit_number = EXCLUDED.patent_owner_group_art_unit_number,
                patent_owner_application_number_text = EXCLUDED.patent_owner_application_number_text,
                patent_owner_inventor_name = EXCLUDED.patent_owner_inventor_name,
                trial_meta_accorded_filing_date = EXCLUDED.trial_meta_accorded_filing_date,
                trial_meta_termination_date = EXCLUDED.trial_meta_termination_date,
                trial_meta_trial_type_code = EXCLUDED.trial_meta_trial_type_code,
                trial_meta_latest_decision_date = EXCLUDED.trial_meta_latest_decision_date,
                trial_meta_file_download_uri = EXCLUDED.trial_meta_file_download_uri,
                trial_meta_institution_decision_date = EXCLUDED.trial_meta_institution_decision_date,
                trial_meta_trial_status_category = EXCLUDED.trial_meta_trial_status_category,
                trial_meta_trial_last_modified_date = EXCLUDED.trial_meta_trial_last_modified_date,
                trial_meta_petition_filing_date = EXCLUDED.trial_meta_petition_filing_date,
                trial_meta_trial_last_modified_datetime = EXCLUDED.trial_meta_trial_last_modified_datetime,
                regular_petitioner_real_party_in_interest_name = EXCLUDED.regular_petitioner_real_party_in_interest_name,
                regular_petitioner_counsel_name = EXCLUDED.regular_petitioner_counsel_name,
                raw_json = EXCLUDED.raw_json
            """,
            flattened,
        )


def fetch_page(session, api_key, offset):
    url = "https://api.uspto.gov/api/v1/patent/trials/proceedings/search"
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
            }
        ],
        "rangeFilters": [],
        "pagination": {"offset": offset, "limit": PAGE_LIMIT},
        "sort": [
            {
                "field": "trialMetaData.trialLastModifiedDateTime",
                "order": "Desc",
            }
        ],
    }
    response = session.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    load_dotenv()
    api_key = os.environ.get("USPTO_API_KEY")
    if not api_key:
        raise ValueError("Set USPTO_API_KEY in .env before running this script.")

    session = requests.Session()
    with get_connection() as conn:
        ensure_schema(conn)

        checkpoint = get_checkpoint(conn)
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

            page_records = page_data.get("patentTrialProceedingDataBag", [])
            if not page_records:
                break

            for record in page_records:
                flattened = flatten_record(record)
                modified_at = (
                    flattened["trial_meta_trial_last_modified_datetime"]
                    or flattened["last_modified_datetime"]
                )

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
                f"synced {len(synced_records)} of approx {total_count} records so far",
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
            "patentTrialProceedingDataBag": synced_records,
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=2, default=str)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM proceedings")
            table_count = cur.fetchone()[0]

    print(
        f"Fetched {pages_fetched} pages, synced {len(synced_records)} records, "
        f"table now has {table_count} rows, checkpoint={output['last_seen_modified_datetime']}"
    )


if __name__ == "__main__":
    main()
