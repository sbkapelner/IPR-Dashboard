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
PROMPT_VERSION = "2026-04-30-fintiv-v1"
SECONDS_BETWEEN_ROWS = 1.5
SKIP_LOG_PATH = Path("fintiv_factors_skipped_rows.log")
TARGETS = {
    "discretionary_denials_granular": {
        "where": "processed_text IS NOT NULL AND parallel_litigation_314a IS TRUE",
    },
    "issue_analysis_non_discretionary": {
        "where": (
            "processed_text IS NOT NULL AND analysis_status = 'analyzed' "
            "AND parallel_litigation_314a IS TRUE"
        ),
    },
}


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
            for factor in range(1, 7):
                cur.execute(
                    f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS fintiv_factor_{factor} BOOLEAN
                    """
                )
    conn.commit()


def get_candidates(conn, table_name, limit):
    where = TARGETS[table_name]["where"]
    query = f"""
        SELECT trial_number, processed_text
        FROM {table_name}
        WHERE {where}
          AND (
                fintiv_factor_1 IS NULL
                OR fintiv_factor_2 IS NULL
                OR fintiv_factor_3 IS NULL
                OR fintiv_factor_4 IS NULL
                OR fintiv_factor_5 IS NULL
                OR fintiv_factor_6 IS NULL
              )
        ORDER BY random()
    """
    params = []
    if limit is not None:
        query += "\nLIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def update_factors(conn, table_name, trial_number, analysis):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET
                fintiv_factor_1 = %s,
                fintiv_factor_2 = %s,
                fintiv_factor_3 = %s,
                fintiv_factor_4 = %s,
                fintiv_factor_5 = %s,
                fintiv_factor_6 = %s
            WHERE trial_number = %s
            """,
            [
                analysis["fintiv_factor_1"]["applies"],
                analysis["fintiv_factor_2"]["applies"],
                analysis["fintiv_factor_3"]["applies"],
                analysis["fintiv_factor_4"]["applies"],
                analysis["fintiv_factor_5"]["applies"],
                analysis["fintiv_factor_6"]["applies"],
                trial_number,
            ],
        )
    conn.commit()


def system_prompt():
    return """You are a legal issue-classification assistant. A patent owner discretionary-denial brief has already been identified as raising a parallel-litigation argument. Determine which Fintiv factors the brief substantively raises.

Return valid JSON only.

Classify these six factors:
- fintiv_factor_1: whether there is or is not a stay, or evidence one may be granted
- fintiv_factor_2: proximity of the court's trial date to the Board's projected final written decision deadline
- fintiv_factor_3: investment in the parallel proceeding by the court and parties
- fintiv_factor_4: overlap between issues raised in the petition and in the parallel proceeding
- fintiv_factor_5: whether the petitioner and the defendant in the parallel proceeding are the same party
- fintiv_factor_6: other circumstances that impact the Board's exercise of discretion, including merits or broader fairness considerations

Mark applies = true only if the brief substantively develops the factor as part of its discretionary-denial argument. Do not mark a factor true merely because the brief briefly mentions a related fact without using it as an argument.

Return this JSON shape exactly:
{
  "fintiv_factor_1": {"applies": true, "reason": "...", "evidence_snippet": "..."},
  "fintiv_factor_2": {"applies": true, "reason": "...", "evidence_snippet": "..."},
  "fintiv_factor_3": {"applies": true, "reason": "...", "evidence_snippet": "..."},
  "fintiv_factor_4": {"applies": true, "reason": "...", "evidence_snippet": "..."},
  "fintiv_factor_5": {"applies": true, "reason": "...", "evidence_snippet": "..."},
  "fintiv_factor_6": {"applies": true, "reason": "...", "evidence_snippet": "..."}
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
            {"role": "user", "content": f"Classify these Fintiv factors:\n\n{text}"},
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
                print(f"OpenAI 429 during Fintiv classification: {response.text[:800]}")
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
                print(f"OpenAI 429 during Fintiv repair: {response.text[:800]}")
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
    payload = extract_json_payload(raw_output)
    if payload:
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
    parser.add_argument("--table", choices=sorted(TARGETS.keys()), default="discretionary_denials_granular")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    load_dotenv()
    session = requests.Session()

    with get_connection() as conn:
        ensure_columns(conn)
        candidates = get_candidates(conn, args.table, args.limit)

    print(f"Found {len(candidates)} candidate rows in {args.table}")

    for index, (trial_number, processed_text) in enumerate(candidates, start=1):
        print(f"[{index}/{len(candidates)}] Processing {trial_number}")
        try:
            raw_output = call_openai(session, processed_text)
            analysis = parse_analysis(session, raw_output)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            response_text = exc.response.text[:500] if exc.response is not None else ""
            print(f"[{index}/{len(candidates)}] Skipping {trial_number}: HTTP {status_code}. {response_text}")
            log_skipped_row(args.table, trial_number, f"HTTP {status_code}: {response_text}")
            continue
        except Exception as exc:
            print(f"[{index}/{len(candidates)}] Skipping {trial_number}: {exc}")
            log_skipped_row(args.table, trial_number, str(exc))
            continue

        with get_connection() as conn:
            update_factors(conn, args.table, trial_number, analysis)

        print(
            json.dumps(
                {
                    "table": args.table,
                    "trial_number": trial_number,
                    **{f"fintiv_factor_{i}": analysis[f"fintiv_factor_{i}"]["applies"] for i in range(1, 7)},
                    "prompt_version": PROMPT_VERSION,
                }
            )
        )
        time.sleep(SECONDS_BETWEEN_ROWS)


if __name__ == "__main__":
    main()
