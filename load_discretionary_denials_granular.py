import argparse
import json
import os
import re
import subprocess
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import requests

try:
    import psycopg
except ImportError as exc:
    raise ImportError(
        "psycopg is required. Install it with: pip install psycopg[binary]"
    ) from exc


MODEL_NAME = "gpt-4.1-mini"
PROMPT_VERSION = "2026-04-12-v1"
OUTPUT_PATH = "discretionary_denials_granular_output.json"
TABLE_NAME = "discretionary_denials_granular"


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
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                trial_number TEXT PRIMARY KEY,
                document_identifier TEXT,
                source_filename TEXT,
                proceeding_zip_uri TEXT,
                document_type_description_text TEXT,
                document_title_text TEXT,
                extracted_text TEXT,
                processed_text TEXT,
                model_name TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                parallel_litigation_314a BOOLEAN,
                serial_petitions_314a BOOLEAN,
                settled_expectations_314a BOOLEAN,
                previous_art_or_arguments_325d BOOLEAN,
                prior_office_presentation_325d BOOLEAN,
                material_error_325d BOOLEAN,
                estoppel_315e BOOLEAN,
                analysis_json JSONB NOT NULL,
                analyzed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                proceedings_last_modified_datetime TIMESTAMP
            )
            """
        )
    conn.commit()


def get_candidate_rows(conn, full_refresh=False, limit=None):
    query = f"""
        SELECT
            p.trial_number,
            p.trial_meta_file_download_uri,
            p.last_modified_datetime
        FROM proceedings p
        LEFT JOIN {TABLE_NAME} g
            ON p.trial_number = g.trial_number
        WHERE p.trial_meta_trial_status_category = 'Discretionary Denial'
          AND (
                %s
                OR g.trial_number IS NULL
                OR g.proceedings_last_modified_datetime IS DISTINCT FROM p.last_modified_datetime
                OR g.prompt_version <> %s
              )
        ORDER BY p.trial_number
    """
    params = [full_refresh, PROMPT_VERSION]
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def upsert_analysis(conn, record):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                trial_number,
                document_identifier,
                source_filename,
                proceeding_zip_uri,
                document_type_description_text,
                document_title_text,
                extracted_text,
                processed_text,
                model_name,
                prompt_version,
                parallel_litigation_314a,
                serial_petitions_314a,
                settled_expectations_314a,
                previous_art_or_arguments_325d,
                prior_office_presentation_325d,
                material_error_325d,
                estoppel_315e,
                analysis_json,
                analyzed_at,
                proceedings_last_modified_datetime
            ) VALUES (
                %(trial_number)s,
                %(document_identifier)s,
                %(source_filename)s,
                %(proceeding_zip_uri)s,
                %(document_type_description_text)s,
                %(document_title_text)s,
                %(extracted_text)s,
                %(processed_text)s,
                %(model_name)s,
                %(prompt_version)s,
                %(parallel_litigation_314a)s,
                %(serial_petitions_314a)s,
                %(settled_expectations_314a)s,
                %(previous_art_or_arguments_325d)s,
                %(prior_office_presentation_325d)s,
                %(material_error_325d)s,
                %(estoppel_315e)s,
                %(analysis_json)s::jsonb,
                %(analyzed_at)s,
                %(proceedings_last_modified_datetime)s
            )
            ON CONFLICT (trial_number) DO UPDATE SET
                document_identifier = EXCLUDED.document_identifier,
                source_filename = EXCLUDED.source_filename,
                proceeding_zip_uri = EXCLUDED.proceeding_zip_uri,
                document_type_description_text = EXCLUDED.document_type_description_text,
                document_title_text = EXCLUDED.document_title_text,
                extracted_text = EXCLUDED.extracted_text,
                processed_text = EXCLUDED.processed_text,
                model_name = EXCLUDED.model_name,
                prompt_version = EXCLUDED.prompt_version,
                parallel_litigation_314a = EXCLUDED.parallel_litigation_314a,
                serial_petitions_314a = EXCLUDED.serial_petitions_314a,
                settled_expectations_314a = EXCLUDED.settled_expectations_314a,
                previous_art_or_arguments_325d = EXCLUDED.previous_art_or_arguments_325d,
                prior_office_presentation_325d = EXCLUDED.prior_office_presentation_325d,
                material_error_325d = EXCLUDED.material_error_325d,
                estoppel_315e = EXCLUDED.estoppel_315e,
                analysis_json = EXCLUDED.analysis_json,
                analyzed_at = EXCLUDED.analyzed_at,
                proceedings_last_modified_datetime = EXCLUDED.proceedings_last_modified_datetime
            """,
            record,
        )


def download_zip(session, zip_uri):
    response = session.get(
        zip_uri,
        headers={"x-api-key": os.environ["USPTO_API_KEY"]},
        timeout=60,
    )
    response.raise_for_status()
    return response.content


def pdf_to_text(pdf_path, txt_path, pages=None):
    cmd = ["pdftotext"]
    if pages is not None:
        cmd.extend(["-f", "1", "-l", str(pages)])
    cmd.extend([str(pdf_path), str(txt_path)])
    subprocess.run(cmd, check=True, capture_output=True)
    return txt_path.read_text(errors="ignore")


def score_candidate(text):
    lowered = text.lower()
    score = 0

    if "patent owner" in lowered:
        score += 5
    if "discretionary denial" in lowered:
        score += 8
    if "request for discretionary denial" in lowered:
        score += 5
    if "memorandum in support of discretionary denial" in lowered:
        score += 5
    if "brief on discretionary denial" in lowered:
        score += 5
    if "fintiv" in lowered:
        score += 2
    if "settled expectations" in lowered:
        score += 2

    for phrase in (
        "petitioner opposition",
        "opposition to patent owner's request",
        "reply brief",
        "sur-reply",
        "director discretionary decision",
        "institution decision",
        "board",
    ):
        if phrase in lowered:
            score -= 8

    if "reply" in lowered and "patent owner" in lowered:
        score -= 4

    return score


def find_opening_brief(zip_bytes, trial_number):
    with tempfile.TemporaryDirectory(prefix=f"{trial_number.lower()}_") as temp_dir:
        temp_path = Path(temp_dir)
        with zipfile.ZipFile(Path(temp_dir) / "trial.zip", "w"):
            pass
        zip_path = temp_path / "trial.zip"
        zip_path.write_bytes(zip_bytes)

        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(temp_path / "unzipped")

        unzipped = temp_path / "unzipped"
        pdf_paths = sorted(unzipped.glob("*.pdf"))
        if not pdf_paths:
            return None

        best = None
        for pdf_path in pdf_paths:
            preview_path = temp_path / f"{pdf_path.stem}_preview.txt"
            try:
                preview_text = pdf_to_text(pdf_path, preview_path, pages=3)
            except Exception:
                continue

            score = score_candidate(preview_text)
            if score <= 0:
                continue

            if best is None or score > best["score"]:
                best = {
                    "score": score,
                    "pdf_path": pdf_path,
                    "preview_text": preview_text,
                }

        if best is None:
            return None

        full_text_path = temp_path / f"{best['pdf_path'].stem}_full.txt"
        full_text = pdf_to_text(best["pdf_path"], full_text_path)
        return {
            "document_identifier": best["pdf_path"].stem,
            "source_filename": best["pdf_path"].name,
            "extracted_text": full_text,
        }


def normalize_whitespace(value):
    return re.sub(r"\s+", " ", value).strip()


def section_hits(text, needle):
    hits = []
    start = 0
    while True:
        index = text.find(needle, start)
        if index == -1:
            break
        hits.append(index)
        start = index + 1
    return hits


def choose_start_heading(text):
    toc_start = text.find("TABLE OF CONTENTS")
    if toc_start == -1:
        toc_start = 0
    toc_region = text[toc_start : toc_start + 8000]

    lines = [normalize_whitespace(line) for line in toc_region.splitlines()]
    combined = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line:
            index += 1
            continue

        if re.fullmatch(r"[IVXLC]+\.", line):
            next_index = index + 1
            while next_index < len(lines) and not lines[next_index]:
                next_index += 1
            if next_index < len(lines):
                combined.append(f"{line} {lines[next_index]}")
                index = next_index + 1
                continue

        combined.append(line)
        index += 1

    for line in combined:
        lowered = line.lower()
        if any(
            token in lowered
            for token in (
                "table of",
                "introduction",
                "background",
                "factual background",
                "procedural",
                "conclusion",
                "certificate",
            )
        ):
            continue
        if any(
            token in lowered
            for token in (
                "argument",
                "fintiv",
                "settled expectations",
                "325(d)",
                "315(e)",
                "general plastic",
                "discretionary denial",
                "institution under",
                "weak and overly reliant",
                "compelling national security",
            )
        ):
            return line
    return None


def choose_end_heading(text):
    for heading in ("V. CONCLUSION", "V. Conclusion", "Conclusion", "CONCLUSION"):
        hits = section_hits(text, heading)
        if len(hits) >= 2:
            return heading, hits[1]
        if hits:
            return heading, hits[0]
    return None, -1


def preprocess_text(text):
    start_heading = choose_start_heading(text)
    if not start_heading:
        return text

    start_hits = section_hits(text, start_heading)
    if not start_hits:
        return text
    if len(start_hits) >= 2:
        start = start_hits[1]
    else:
        start = start_hits[0]

    _, end = choose_end_heading(text)
    if end != -1 and end > start:
        return text[start:end]
    return text[start:]


def system_prompt():
    return """You are a legal issue-classification assistant. Classify whether a patent owner's discretionary denial brief substantively raises each category below. Use reasoning based on the context and argument substance, not mere keyword matching.

You must classify based only on the substantive argument sections provided. Do not infer a category from table of contents material, factual background, procedural background, or a list of cited authorities unless the provided argument text itself uses those facts as a reason supporting discretionary denial.

Return valid JSON only.

Definitions:
- 314a_parallel_litigation: true only if the brief substantively argues denial because parallel district court or ITC litigation will resolve overlapping issues sooner or more efficiently. Strong indicators include Fintiv, trial timing, stay, overlap of issues/claims/grounds, investment in parallel litigation, or same parties.
- 314a_serial_petitions: true only if the brief substantively argues repeat, follow-on, coordinated, joined, or multiple PTAB challenges by the same petitioner, real party in interest, privy, or related party against the same patent. Strong indicators include General Plastic, follow-on petitioning, multiple petitions, joinder tactics, or coordinated petitioning. Do not mark this true merely because the brief mentions related petitions or related patents as background facts.
- 314a_settled_expectations: true only if the brief argues the patent's age, long period in force, reliance interests, investment-backed expectations, or delayed challenge supports denial. Patent age alone is not enough unless tied to a settled-expectations or delayed-challenge argument.
- 325d_previous_art_or_arguments: true only if the brief substantively argues that the same or substantially the same prior art or arguments were previously presented to the USPTO during examination, reexamination, reissue, or other Office proceedings, and/or discusses whether the Office made a material error in evaluating them. Strong indicators include Advanced Bionics, Becton Dickinson, material error, same/substantially same art, examiner considered, prosecution history, IDS, or prior Office evaluation. Do NOT count petitioner knowledge, delay, awareness of the patent family, parallel litigation, licensing discussions, or infringement notice as 325(d).
- 315e_estoppel: true only if the brief substantively relies on statutory estoppel under 35 U.S.C. 315(e) as a reason supporting denial, including that estoppel applies, will apply after a final written decision, should preclude further challenges, or that petitioner is trying to evade estoppel. Do NOT count it if the brief merely says estoppel is narrow, limited, or insufficient.

For each category, provide:
- applies: boolean
- reason: short explanation
- evidence_snippet: short direct quotation or excerpt if applies is true, otherwise null
- cited_authorities: short list of statutes/cases if clearly tied to that category

For 325(d), also provide:
- prior_office_presentation_discussed: boolean
- material_error_discussed: boolean

Return this JSON shape exactly:
{
  "314a_parallel_litigation": {...},
  "314a_serial_petitions": {...},
  "314a_settled_expectations": {...},
  "325d_previous_art_or_arguments": {...},
  "315e_estoppel": {...},
  "summary": "..."
}
"""


def call_openai(session, text):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Set OPENAI_API_KEY in .env before running this script.")

    response = session.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL_NAME,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt()},
                {
                    "role": "user",
                    "content": f"Classify this patent owner discretionary denial argument text:\n\n{text}",
                },
            ],
        },
        timeout=180,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def repair_json(session, raw_output):
    api_key = os.environ.get("OPENAI_API_KEY")
    response = session.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL_NAME,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": "Repair the following into strictly valid JSON only. Do not add commentary.",
                },
                {"role": "user", "content": raw_output},
            ],
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def extract_json_payload(text):
    if not text:
        return ""

    cleaned = text.strip()

    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()

    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        return cleaned[start : end + 1]

    return cleaned


def parse_analysis(session, raw_output):
    candidates = [raw_output]
    last_error = None

    for candidate in candidates:
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


def build_record(trial_number, zip_uri, proceedings_last_modified_datetime, brief, processed_text, analysis):
    category_325d = analysis["325d_previous_art_or_arguments"]
    return {
        "trial_number": trial_number,
        "document_identifier": brief["document_identifier"],
        "source_filename": brief["source_filename"],
        "proceeding_zip_uri": zip_uri,
        "document_type_description_text": brief.get("document_type_description_text"),
        "document_title_text": brief.get("document_title_text"),
        "extracted_text": brief["extracted_text"],
        "processed_text": processed_text,
        "model_name": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
        "parallel_litigation_314a": analysis["314a_parallel_litigation"]["applies"],
        "serial_petitions_314a": analysis["314a_serial_petitions"]["applies"],
        "settled_expectations_314a": analysis["314a_settled_expectations"]["applies"],
        "previous_art_or_arguments_325d": category_325d["applies"],
        "prior_office_presentation_325d": category_325d["prior_office_presentation_discussed"],
        "material_error_325d": category_325d["material_error_discussed"],
        "estoppel_315e": analysis["315e_estoppel"]["applies"],
        "analysis_json": json.dumps(analysis),
        "analyzed_at": datetime.utcnow(),
        "proceedings_last_modified_datetime": proceedings_last_modified_datetime,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    load_dotenv()
    session = requests.Session()

    analyzed = []
    with get_connection() as conn:
        ensure_schema(conn)
        rows = get_candidate_rows(conn, full_refresh=args.full_refresh, limit=args.limit)

    total_candidates = len(rows)
    print(f"Found {total_candidates} candidate proceedings to process")

    for index, (trial_number, zip_uri, proceedings_last_modified_datetime) in enumerate(
        rows, start=1
    ):
        print(f"[{index}/{total_candidates}] Processing {trial_number}")
        if not zip_uri:
            print(f"[{index}/{total_candidates}] Skipping {trial_number}: missing ZIP URI")
            continue

        zip_bytes = download_zip(session, zip_uri)
        brief = find_opening_brief(zip_bytes, trial_number)
        if not brief:
            print(f"[{index}/{total_candidates}] Skipping {trial_number}: no opening brief found")
            continue

        processed_text = preprocess_text(brief["extracted_text"])
        raw_output = call_openai(session, processed_text)
        analysis = parse_analysis(session, raw_output)

        record = build_record(
            trial_number=trial_number,
            zip_uri=zip_uri,
            proceedings_last_modified_datetime=proceedings_last_modified_datetime,
            brief=brief,
            processed_text=processed_text,
            analysis=analysis,
        )
        with get_connection() as write_conn:
            upsert_analysis(write_conn, record)
            write_conn.commit()

        analyzed.append(
            {
                "trial_number": trial_number,
                "document_identifier": brief["document_identifier"],
                "analysis": analysis,
            }
        )
        print(f"[{index}/{total_candidates}] Analyzed {trial_number}")

    Path(OUTPUT_PATH).write_text(json.dumps(analyzed, indent=2))
    print(f"Analyzed {len(analyzed)} records into {TABLE_NAME}")


if __name__ == "__main__":
    main()
