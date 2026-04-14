import os
from pathlib import Path

try:
    import psycopg
except ImportError as exc:
    raise ImportError(
        "psycopg is required. Install it with: pip install psycopg[binary]"
    ) from exc

try:
    from google.cloud import bigquery
except ImportError as exc:
    raise ImportError(
        "google-cloud-bigquery is required. Install it with: pip install google-cloud-bigquery"
    ) from exc


BIGQUERY_CREDENTIALS_PATH = "bigquerykeys/language-app-323017-566e94dcd421.json"
BIGQUERY_PROJECT = "language-app-323017"
OUTPUT_TABLE = "patent_family_ids"


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
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
                patent_number TEXT PRIMARY KEY,
                publication_number TEXT,
                family_id TEXT,
                grant_date BIGINT
            )
            """
        )
    conn.commit()


def get_patent_numbers(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT patent_owner_patent_number
            FROM proceedings
            WHERE trial_meta_trial_status_category = 'Discretionary Denial'
              AND patent_owner_patent_number IS NOT NULL
            ORDER BY patent_owner_patent_number
            """
        )
        return [row[0] for row in cur.fetchall()]


def build_query(patent_numbers):
    numbers_sql = ",\n".join(f"    '{number}'" for number in patent_numbers)
    return f"""
WITH dd_patents AS (
  SELECT patent_number
  FROM UNNEST([
{numbers_sql}
  ]) AS patent_number
),
us_grants AS (
  SELECT
    publication_number,
    family_id,
    grant_date,
    SPLIT(publication_number, '-')[OFFSET(1)] AS patent_number,
    ROW_NUMBER() OVER (
      PARTITION BY SPLIT(publication_number, '-')[OFFSET(1)]
      ORDER BY grant_date DESC, publication_number
    ) AS rn
  FROM `patents-public-data.patents.publications`
  WHERE country_code = 'US'
    AND grant_date > 0
)
SELECT
  d.patent_number,
  u.publication_number,
  u.family_id,
  u.grant_date
FROM dd_patents d
LEFT JOIN us_grants u
  ON d.patent_number = u.patent_number
 AND u.rn = 1
ORDER BY d.patent_number
"""


def get_bigquery_client():
    credentials_path = Path(BIGQUERY_CREDENTIALS_PATH)
    if not credentials_path.exists():
        raise ValueError(f"BigQuery credentials file not found: {credentials_path}")

    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(credentials_path.resolve()))
    return bigquery.Client(project=BIGQUERY_PROJECT)


def fetch_family_rows(client, patent_numbers):
    query = build_query(patent_numbers)
    return list(client.query(query).result())


def upsert_rows(conn, rows):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"""
                INSERT INTO {OUTPUT_TABLE} (
                    patent_number,
                    publication_number,
                    family_id,
                    grant_date
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (patent_number) DO UPDATE SET
                    publication_number = EXCLUDED.publication_number,
                    family_id = EXCLUDED.family_id,
                    grant_date = EXCLUDED.grant_date
                """,
                (
                    row["patent_number"],
                    row["publication_number"],
                    row["family_id"],
                    row["grant_date"],
                ),
            )
    conn.commit()


def main():
    load_dotenv()

    with get_connection() as conn:
        ensure_schema(conn)
        patent_numbers = get_patent_numbers(conn)

    if not patent_numbers:
        print("No discretionary-denial patent numbers found.")
        return

    client = get_bigquery_client()
    rows = fetch_family_rows(client, patent_numbers)

    with get_connection() as conn:
        upsert_rows(conn, rows)

    print(f"Loaded {len(rows)} patent family rows into {OUTPUT_TABLE}")


if __name__ == "__main__":
    main()
