import argparse
import json
import os
import time
from pathlib import Path

import requests

try:
    import psycopg
except ImportError as exc:
    raise ImportError(
        "psycopg is required. Install it with: pip install psycopg[binary]"
    ) from exc


MODEL_NAME = "gpt-4.1-mini"
PROMPT_VERSION = "2026-04-26-conflicting-positions-v1"
SECONDS_BETWEEN_ROWS = 1.5
TARGETS = {
    "discretionary_denials_granular": {
        "where": "processed_text IS NOT NULL",
    },
    "issue_analysis_non_discretionary": {
        "where": "processed_text IS NOT NULL AND analysis_status = 'analyzed'",
    },
}
SKIP_LOG_PATH = Path("conflicting_positions_skipped_rows.log")


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
    )


def ensure_columns(conn):
    with conn.cursor() as cur:
        for table_name in TARGETS:
            cur.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS conflicting_positions_314a BOOLEAN
                """
            )
    conn.commit()


def get_candidates(conn, table_name, limit, keyword_only):
    where = TARGETS[table_name]["where"]
    keyword_filter = ""
    if keyword_only:
        keyword_filter = """
          AND (
                processed_text ILIKE '%%revvo%%'
                OR processed_text ILIKE '%%tesla%%'
                OR processed_text ILIKE '%%plain and ordinary meaning%%'
                OR processed_text ILIKE '%%indefinite%%'
                OR processed_text ILIKE '%%claim construction%%'
                OR processed_text ILIKE '%%fast and loose%%'
                OR processed_text ILIKE '%%inconsistent%%'
              )
        """

    query = f"""
        SELECT trial_number, processed_text
        FROM {table_name}
        WHERE {where}
          AND conflicting_positions_314a IS NULL
          {keyword_filter}
        ORDER BY random()
    """
    params = []
    if limit is not None:
        query += "\n        LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def update_flag(conn, table_name, trial_number, value):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET conflicting_positions_314a = %s
            WHERE trial_number = %s
            """,
            [value, trial_number],
        )
    conn.commit()


def system_prompt():
    return """You are a legal issue-classification assistant. Determine whether a patent owner discretionary-denial brief substantively argues that the petitioner took conflicting claim-construction or closely related legal positions across forums or proceedings.

Return valid JSON only.

Mark applies = true only if the brief substantively argues one or more of these:
- the petitioner advanced inconsistent claim construction positions across PTAB and district court
- the petitioner argued a term had one meaning in one forum and a different meaning in another
- the petitioner argued indefiniteness in one forum but plain and ordinary meaning or a definite construction in another
- the brief invokes Revvo, Tesla, or a similar "fast and loose" fairness theory based on inconsistent legal positions

Do NOT mark applies = true if the brief merely:
- mentions claim construction generally
- discusses district court litigation without alleging conflicting positions
- raises Fintiv arguments without any cross-forum inconsistency argument

Return this JSON shape exactly:
{
  "applies": true,
  "reason": "short explanation",
  "evidence_snippet": "short supporting excerpt or null",
  "cited_authorities": ["Revvo", "Tesla"]
}
"""


def call_openai(session, text):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Set OPENAI_API_KEY in .env before running this script.")

    payload = {
        "model": MODEL_NAME,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt()},
            {
                "role": "user",
                "content": f"Classify this patent owner discretionary-denial brief text:\n\n{text}",
            },
        ],
    }
    last_error = None
    for attempt in range(5):
        response = session.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=180,
        )
        try:
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except requests.HTTPError as exc:
            last_error = exc
            if response.status_code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            if response.status_code == 429:
                print(
                    "OpenAI 429 during classification: "
                    f"{response.text[:800]}"
                )
                time.sleep(15 * (attempt + 1))
            else:
                time.sleep(2 * (attempt + 1))
    raise last_error


def repair_json(session, raw_output):
    api_key = os.environ.get("OPENAI_API_KEY")
    payload = {
        "model": MODEL_NAME,
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": "Repair the following into strictly valid JSON only. Do not add commentary.",
            },
            {"role": "user", "content": raw_output},
        ],
    }
    last_error = None
    for attempt in range(5):
        response = session.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=120,
        )
        try:
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except requests.HTTPError as exc:
            last_error = exc
            if response.status_code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            if response.status_code == 429:
                print(
                    "OpenAI 429 during repair: "
                    f"{response.text[:800]}"
                )
                time.sleep(15 * (attempt + 1))
            else:
                time.sleep(2 * (attempt + 1))
    raise last_error


def extract_json_payload(text):
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.removeprefix("```json").removeprefix("```").strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        return cleaned[start : end + 1]
    return cleaned


def parse_analysis(session, raw_output):
    last_error = None
    for candidate in (raw_output,):
        payload = extract_json_payload(candidate)
        if not payload:
            continue
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            last_error = exc

    repaired = repair_json(session, raw_output)
    for candidate in (repaired, repair_json(session, extract_json_payload(repaired) or repaired)):
        payload = extract_json_payload(candidate)
        if not payload:
            continue
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            last_error = exc

    raise ValueError(
        "Could not parse OpenAI analysis as JSON after repair attempts. "
        f"Last parse error: {last_error}. Raw output preview: {raw_output[:500]!r}"
    )


def log_skipped_row(table_name, trial_number, reason):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with SKIP_LOG_PATH.open("a") as log_file:
        log_file.write(f"{timestamp}\t{table_name}\t{trial_number}\t{reason}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        choices=sorted(TARGETS.keys()),
        default="discretionary_denials_granular",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--keyword-candidates-only", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    session = requests.Session()

    with get_connection() as conn:
        ensure_columns(conn)
        candidates = get_candidates(
            conn,
            table_name=args.table,
            limit=args.limit,
            keyword_only=args.keyword_candidates_only,
        )

    print(f"Found {len(candidates)} candidate rows in {args.table}")

    for index, (trial_number, processed_text) in enumerate(candidates, start=1):
        print(f"[{index}/{len(candidates)}] Processing {trial_number}")
        try:
            raw_output = call_openai(session, processed_text)
            analysis = parse_analysis(session, raw_output)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            response_text = exc.response.text[:500] if exc.response is not None else ""
            print(
                f"[{index}/{len(candidates)}] Skipping {trial_number}: HTTP {status_code}. "
                f"{response_text}"
            )
            log_skipped_row(
                args.table,
                trial_number,
                f"HTTP {status_code}: {response_text}",
            )
            continue
        except Exception as exc:
            print(f"[{index}/{len(candidates)}] Skipping {trial_number}: {exc}")
            log_skipped_row(args.table, trial_number, str(exc))
            continue
        with get_connection() as conn:
            update_flag(conn, args.table, trial_number, analysis["applies"])
        print(
            json.dumps(
                {
                    "table": args.table,
                    "trial_number": trial_number,
                    "conflicting_positions_314a": analysis["applies"],
                    "reason": analysis["reason"],
                    "evidence_snippet": analysis["evidence_snippet"],
                    "cited_authorities": analysis["cited_authorities"],
                    "prompt_version": PROMPT_VERSION,
                }
            )
        )
        time.sleep(SECONDS_BETWEEN_ROWS)


if __name__ == "__main__":
    main()
