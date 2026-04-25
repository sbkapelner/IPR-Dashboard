import argparse
import gc
import json
import os
import re
import shutil
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
PROMPT_VERSION = "2026-04-17-v1"
TABLE_NAME = "issue_analysis_non_discretionary"
DATE_FLOOR = "2025-03-01"


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
                trial_status_category TEXT,
                document_identifier TEXT,
                source_filename TEXT,
                proceeding_zip_uri TEXT,
                document_type_description_text TEXT,
                document_title_text TEXT,
                extracted_text TEXT,
                processed_text TEXT,
                model_name TEXT,
                prompt_version TEXT NOT NULL,
                analysis_status TEXT NOT NULL,
                analysis_status_detail TEXT,
                parallel_litigation_314a BOOLEAN,
                serial_petitions_314a BOOLEAN,
                settled_expectations_314a BOOLEAN,
                previous_art_or_arguments_325d BOOLEAN,
                prior_office_presentation_325d BOOLEAN,
                material_error_325d BOOLEAN,
                estoppel_315e BOOLEAN,
                analysis_json JSONB,
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
            p.trial_meta_trial_status_category,
            p.trial_meta_file_download_uri,
            p.last_modified_datetime
        FROM proceedings p
        LEFT JOIN {TABLE_NAME} a
            ON p.trial_number = a.trial_number
        WHERE p.trial_meta_trial_type_code = 'IPR'
          AND p.trial_meta_trial_status_category IS DISTINCT FROM 'Discretionary Denial'
          AND p.trial_meta_institution_decision_date >= %s
          AND (
                %s
                OR a.trial_number IS NULL
                OR a.proceedings_last_modified_datetime IS DISTINCT FROM p.last_modified_datetime
                OR a.prompt_version <> %s
              )
        ORDER BY p.trial_number
    """
    params = [DATE_FLOOR, full_refresh, PROMPT_VERSION]
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def prune_out_of_range_rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {TABLE_NAME} a
            USING proceedings p
            WHERE a.trial_number = p.trial_number
              AND p.trial_meta_institution_decision_date < %s
            """,
            [DATE_FLOOR],
        )
    conn.commit()


def upsert_record(conn, record):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                trial_number,
                trial_status_category,
                document_identifier,
                source_filename,
                proceeding_zip_uri,
                document_type_description_text,
                document_title_text,
                extracted_text,
                processed_text,
                model_name,
                prompt_version,
                analysis_status,
                analysis_status_detail,
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
                %(trial_status_category)s,
                %(document_identifier)s,
                %(source_filename)s,
                %(proceeding_zip_uri)s,
                %(document_type_description_text)s,
                %(document_title_text)s,
                %(extracted_text)s,
                %(processed_text)s,
                %(model_name)s,
                %(prompt_version)s,
                %(analysis_status)s,
                %(analysis_status_detail)s,
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
                trial_status_category = EXCLUDED.trial_status_category,
                document_identifier = EXCLUDED.document_identifier,
                source_filename = EXCLUDED.source_filename,
                proceeding_zip_uri = EXCLUDED.proceeding_zip_uri,
                document_type_description_text = EXCLUDED.document_type_description_text,
                document_title_text = EXCLUDED.document_title_text,
                extracted_text = EXCLUDED.extracted_text,
                processed_text = EXCLUDED.processed_text,
                model_name = EXCLUDED.model_name,
                prompt_version = EXCLUDED.prompt_version,
                analysis_status = EXCLUDED.analysis_status,
                analysis_status_detail = EXCLUDED.analysis_status_detail,
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


def download_zip_to_path(session, zip_uri, output_path):
    response = session.get(
        zip_uri,
        headers={"x-api-key": os.environ["USPTO_API_KEY"]},
        stream=True,
        timeout=60,
    )
    response.raise_for_status()
    with open(output_path, "wb") as output_file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output_file.write(chunk)
    response.close()


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

    title_region = lowered[:2000]
    title_signal = re.search(
        r"(patent owner['’]s?\s+)?[a-z0-9 ,./()'-]{0,120}discretionary denial",
        title_region,
        flags=re.IGNORECASE,
    )
    if not title_signal:
        return -100

    positive_markers = (
        "patent owner request for discretionary denial",
        "patent owner's request for discretionary denial",
        "patent owner memorandum in support of discretionary denial",
        "patent owner's memorandum in support of discretionary denial",
        "patent owner brief on discretionary denial",
        "patent owner's brief on discretionary denial",
        "request for discretionary denial",
        "memorandum in support of discretionary denial",
        "brief on discretionary denial",
        "discretionary denial",
    )
    for phrase in positive_markers:
        if phrase in title_region:
            score += 10

    if "patent owner" in title_region:
        score += 4
    if "deny institution" in lowered:
        score += 3
    if "fintiv" in lowered:
        score += 2
    if "general plastic" in lowered:
        score += 2
    if "settled expectations" in lowered:
        score += 2
    if "advanced bionics" in lowered:
        score += 2
    if "becton" in lowered:
        score += 1

    hard_negative_markers = (
        "declaration of",
        "third party requester",
        "requester's comments",
        "requester comments",
        "office action",
        "reexamination",
        "control no.:",
        "control no.",
        "examiner:",
        "notice of filing date accorded to petition",
        "petitioner opposition",
        "opposition to patent owner's request",
        "reply brief",
        "sur-reply",
        "director discretionary decision",
        "institution decision",
        "show cause why the petition should not be denied",
    )
    for phrase in hard_negative_markers:
        if phrase in lowered:
            return -100

    if "board" in lowered and "patent owner" not in lowered:
        score -= 8

    if "reply" in lowered and "patent owner" in lowered:
        score -= 4

    return score


def find_opening_brief(zip_path, trial_number):
    with tempfile.TemporaryDirectory(prefix=f"{trial_number.lower()}_") as temp_dir:
        temp_path = Path(temp_dir)
        with zipfile.ZipFile(zip_path) as archive:
            pdf_members = sorted(
                (info for info in archive.infolist() if info.filename.lower().endswith(".pdf")),
                key=lambda info: info.filename.lower(),
            )
            if not pdf_members:
                return None

            best = None
            for info in pdf_members:
                pdf_name = Path(info.filename).name
                pdf_path = temp_path / pdf_name
                pdf_path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as src, open(pdf_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)

                preview_path = temp_path / f"{pdf_path.stem}_preview.txt"
                try:
                    preview_text = pdf_to_text(pdf_path, preview_path, pages=3)
                except Exception:
                    if pdf_path.exists():
                        pdf_path.unlink()
                    continue

                score = score_candidate(preview_text)
                if score <= 0:
                    if pdf_path.exists():
                        pdf_path.unlink()
                    if preview_path.exists():
                        preview_path.unlink()
                    continue

                if best is None or score > best["score"]:
                    if best and best["pdf_path"].exists():
                        best["pdf_path"].unlink()
                    best = {
                        "score": score,
                        "pdf_path": pdf_path,
                    }
                else:
                    pdf_path.unlink()

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


def build_status_record(
    trial_number,
    trial_status_category,
    zip_uri,
    proceedings_last_modified_datetime,
    analysis_status,
    analysis_status_detail,
):
    return {
        "trial_number": trial_number,
        "trial_status_category": trial_status_category,
        "document_identifier": None,
        "source_filename": None,
        "proceeding_zip_uri": zip_uri,
        "document_type_description_text": None,
        "document_title_text": None,
        "extracted_text": None,
        "processed_text": None,
        "model_name": None,
        "prompt_version": PROMPT_VERSION,
        "analysis_status": analysis_status,
        "analysis_status_detail": analysis_status_detail,
        "parallel_litigation_314a": None,
        "serial_petitions_314a": None,
        "settled_expectations_314a": None,
        "previous_art_or_arguments_325d": None,
        "prior_office_presentation_325d": None,
        "material_error_325d": None,
        "estoppel_315e": None,
        "analysis_json": None,
        "analyzed_at": datetime.utcnow(),
        "proceedings_last_modified_datetime": proceedings_last_modified_datetime,
    }


def build_analysis_record(
    trial_number,
    trial_status_category,
    zip_uri,
    proceedings_last_modified_datetime,
    brief,
    processed_text,
    analysis,
):
    category_325d = analysis["325d_previous_art_or_arguments"]
    return {
        "trial_number": trial_number,
        "trial_status_category": trial_status_category,
        "document_identifier": brief["document_identifier"],
        "source_filename": brief["source_filename"],
        "proceeding_zip_uri": zip_uri,
        "document_type_description_text": brief.get("document_type_description_text"),
        "document_title_text": brief.get("document_title_text"),
        "extracted_text": brief["extracted_text"],
        "processed_text": processed_text,
        "model_name": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
        "analysis_status": "analyzed",
        "analysis_status_detail": None,
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

    with get_connection() as conn:
        ensure_schema(conn)
        prune_out_of_range_rows(conn)
        rows = get_candidate_rows(conn, full_refresh=args.full_refresh, limit=args.limit)

    total_candidates = len(rows)
    print(f"Found {total_candidates} non-discretionary candidate proceedings to process")

    for index, (
        trial_number,
        trial_status_category,
        zip_uri,
        proceedings_last_modified_datetime,
    ) in enumerate(rows, start=1):
        print(f"[{index}/{total_candidates}] Processing {trial_number}")
        if not zip_uri:
            print(f"[{index}/{total_candidates}] Skipping {trial_number}: missing ZIP URI")
            record = build_status_record(
                trial_number,
                trial_status_category,
                zip_uri,
                proceedings_last_modified_datetime,
                "missing_zip_uri",
                "Proceeding row has no ZIP URI",
            )
            with get_connection() as write_conn:
                upsert_record(write_conn, record)
                write_conn.commit()
            continue

        with tempfile.TemporaryDirectory(prefix=f"{trial_number.lower()}_zip_") as zip_temp_dir:
            zip_path = Path(zip_temp_dir) / "trial.zip"
            download_zip_to_path(session, zip_uri, zip_path)
            brief = find_opening_brief(zip_path, trial_number)
        if not brief:
            print(f"[{index}/{total_candidates}] Skipping {trial_number}: no relevant brief found")
            record = build_status_record(
                trial_number,
                trial_status_category,
                zip_uri,
                proceedings_last_modified_datetime,
                "no_relevant_brief",
                "No patent owner discretionary denial brief found in proceeding ZIP",
            )
            with get_connection() as write_conn:
                upsert_record(write_conn, record)
                write_conn.commit()
            continue

        processed_text = preprocess_text(brief["extracted_text"])
        raw_output = call_openai(session, processed_text)
        analysis = parse_analysis(session, raw_output)

        record = build_analysis_record(
            trial_number=trial_number,
            trial_status_category=trial_status_category,
            zip_uri=zip_uri,
            proceedings_last_modified_datetime=proceedings_last_modified_datetime,
            brief=brief,
            processed_text=processed_text,
            analysis=analysis,
        )
        with get_connection() as write_conn:
            upsert_record(write_conn, record)
            write_conn.commit()

        print(f"[{index}/{total_candidates}] Analyzed {trial_number}")
        del brief, processed_text, raw_output, analysis, record
        gc.collect()


if __name__ == "__main__":
    main()
