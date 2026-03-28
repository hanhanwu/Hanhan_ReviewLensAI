import io
import uuid
from io import BytesIO
import json
import math
import os
from contextlib import redirect_stdout
from typing import Any, Callable, Dict, Iterable, Iterator, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.db import (
    create_upload_placeholder,
    ensure_schema,
    finalize_upload,
    get_backend_stats,
    get_chat_context,
    get_conn,
    get_db_aggregates,
    get_upload_rows,
    insert_upload_rows,
    reset_all_upload_data,
)

app = FastAPI(
    title="ReviewLens CSV Counter",
    description="Upload a CSV and report how many rows and columns it contains.",
)

GROQ_MODEL = "openai/gpt-oss-safeguard-20b"
UNKNOWN_ANSWER = "I don't know"
MAX_SQL_RESULT_ROWS = 200
DATA_ANALYSIS_TERMS = {
    "average",
    "column",
    "columns",
    "count",
    "counts",
    "csv",
    "data",
    "dataset",
    "distribution",
    "file",
    "highest",
    "how many",
    "lowest",
    "mean",
    "median",
    "missing",
    "most",
    "null",
    "rating",
    "ratings",
    "review",
    "reviews",
    "row",
    "rows",
    "sample",
    "stats",
    "summarize",
    "summary",
    "top",
    "trend",
    "trends",
    "upload",
    "value",
    "values",
}
SAFE_PYTHON_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
}


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    question: str
    history: List[ChatMessage] = []


def _require_groq_token() -> str:
    token = os.getenv("GROQ_TOKEN")
    if not token:
        raise RuntimeError('Missing environment variable "GROQ_TOKEN".')
    return token


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _question_appears_data_related(question: str, column_names: List[str]) -> bool:
    normalized_question = _normalize_text(question)
    if not normalized_question:
        return False

    if any(term in normalized_question for term in DATA_ANALYSIS_TERMS):
        return True

    normalized_columns = {
        _normalize_text(name).replace("_", " ").replace("-", " ") for name in column_names
    }
    return any(column and column in normalized_question for column in normalized_columns)


def _trim_history(history: List[ChatMessage], *, max_messages: int = 8) -> List[dict[str, str]]:
    trimmed = history[-max_messages:]
    messages: List[dict[str, str]] = []
    for message in trimmed:
        role = "assistant" if message.role == "assistant" else "user"
        content = message.content.strip()
        if content:
            messages.append({"role": role, "content": content})
    return messages


def _build_chat_context(upload_id: uuid.UUID) -> Dict[str, Any]:
    context = get_chat_context(upload_id, max_rows=150)
    context["db_aggregates"] = get_db_aggregates(upload_id)
    return context


def _call_groq_messages(messages: List[dict[str, str]], *, temperature: float = 0.1) -> str:
    token = _require_groq_token()
    payload = {
        "model": GROQ_MODEL,
        "temperature": temperature,
        "messages": messages,
    }

    request = Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "ReviewLensAI/1.0",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=45) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Groq API request failed with status {exc.code}: {detail}")
    except URLError as exc:
        raise RuntimeError(f"Unable to reach Groq API: {exc.reason}")

    try:
        payload = json.loads(raw)
        answer = payload["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        raise RuntimeError(f"Unexpected response from Groq API: {exc}")

    return answer or UNKNOWN_ANSWER


def _extract_json_object(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        cleaned = ""
        for part in parts:
            part = part.strip()
            if part and not part.startswith("json"):
                cleaned = part
                break

    try:
        return json.loads(cleaned)
    except Exception:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start : end + 1])
        raise RuntimeError("Model did not return valid JSON.")


def _normalize_unknown_answer(answer: str) -> str:
    if _normalize_text(answer) in {"i don't know", "i do not know"}:
        return UNKNOWN_ANSWER
    return answer.strip() or UNKNOWN_ANSWER


def _get_commander_decision(
    *, question: str, history: List[ChatMessage], context: Dict[str, Any]
) -> Dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "You are the commander agent for a dataset QA system. "
                "You receive a user question, the dataset schema, aggregates, and example rows. "
                "Rewrite the question with the correct assumptions and relevant details from the dataset. "
                "Choose exactly one worker: sql, python, or unknown. "
                "Choose sql for straightforward counting, filtering, grouping, sorting, and aggregation that can be answered directly in SQL. "
                "Choose python for questions that need more flexible dataframe logic, derived calculations, or operations that are easier in pandas. "
                f"Choose unknown if the question is unrelated to the dataset or cannot be answered from the provided data. "
                "Return strict JSON with keys: rewritten_question, agent, assumptions."
            ),
        },
        {
            "role": "system",
            "content": f"Dataset context:\n{json.dumps(context, ensure_ascii=True)}",
        },
        *_trim_history(history),
        {"role": "user", "content": question.strip()},
    ]
    raw = _call_groq_messages(messages, temperature=0.0)
    decision = _extract_json_object(raw)
    agent = str(decision.get("agent", "unknown")).strip().lower()
    if agent not in {"sql", "python", "unknown"}:
        agent = "unknown"
    rewritten_question = str(decision.get("rewritten_question", question)).strip() or question
    assumptions = decision.get("assumptions", [])
    if not isinstance(assumptions, list):
        assumptions = []
    return {
        "agent": agent,
        "rewritten_question": rewritten_question,
        "assumptions": [str(item) for item in assumptions[:8]],
    }


def _is_safe_sql(sql_text: str) -> bool:
    normalized = _normalize_text(sql_text)
    if not normalized:
        return False
    if ";" in sql_text.strip().rstrip(";"):
        return False
    if not (normalized.startswith("select") or normalized.startswith("with")):
        return False
    blocked = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "create "]
    if any(token in normalized for token in blocked):
        return False
    if " upload_rows" in normalized or " uploads" in normalized:
        return False
    return True


def _run_sql_worker(*, upload_id: uuid.UUID, rewritten_question: str, context: Dict[str, Any]) -> str:
    sql_generation_messages = [
        {
            "role": "system",
            "content": (
                "You are the SQL worker agent for dataset QA. "
                "Write one read-only PostgreSQL query that answers the user's rewritten question. "
                "You may only query these two CTEs, which will already exist when your query runs: "
                "current_upload_rows(row_number, data) and current_upload_meta(upload_id, filename, created_at, rows_count, columns_count, column_names, backend_stats). "
                "Each row in current_upload_rows has a JSONB column named data. "
                "Return strict JSON with keys: sql and notes. "
                "The sql must be a single SELECT or WITH query only."
            ),
        },
        {
            "role": "system",
            "content": f"Dataset context:\n{json.dumps(context, ensure_ascii=True)}",
        },
        {"role": "user", "content": rewritten_question},
    ]
    sql_raw = _call_groq_messages(sql_generation_messages, temperature=0.0)
    sql_plan = _extract_json_object(sql_raw)
    sql_text = str(sql_plan.get("sql", "")).strip()
    if not _is_safe_sql(sql_text):
        return UNKNOWN_ANSWER

    wrapped_sql = f"""
        WITH current_upload_rows AS (
          SELECT row_number, data
          FROM upload_rows
          WHERE upload_id = %s
        ),
        current_upload_meta AS (
          SELECT upload_id, filename, created_at, rows_count, columns_count, column_names, backend_stats
          FROM uploads
          WHERE upload_id = %s
        )
        {sql_text}
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(wrapped_sql, (upload_id, upload_id))
            rows = cur.fetchmany(MAX_SQL_RESULT_ROWS)
            columns = [desc[0] for desc in cur.description] if cur.description else []

    result_rows = [
        {columns[i]: _json_compatible(value) for i, value in enumerate(row)}
        for row in rows
    ]

    answer_messages = [
        {
            "role": "system",
            "content": (
                "You are the SQL worker agent finishing a dataset answer. "
                "Answer only from the SQL result and the dataset context. "
                f"If the result does not support a grounded answer, reply with exactly: {UNKNOWN_ANSWER}. "
                "Keep the answer concise."
            ),
        },
        {
            "role": "system",
            "content": json.dumps(
                {
                    "rewritten_question": rewritten_question,
                    "sql": sql_text,
                    "result_columns": columns,
                    "result_rows": result_rows,
                    "row_count": len(result_rows),
                },
                ensure_ascii=True,
            ),
        },
        {"role": "user", "content": rewritten_question},
    ]
    return _normalize_unknown_answer(_call_groq_messages(answer_messages, temperature=0.0))


def _extract_python_code(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        for part in parts:
            stripped = part.strip()
            if stripped.startswith("python"):
                return stripped[len("python") :].strip()
            if stripped and not stripped.startswith("json"):
                return stripped
    return cleaned


def _run_python_code(code: str, df: pd.DataFrame) -> Dict[str, Any]:
    stdout_buffer = io.StringIO()
    globals_dict = {
        "__builtins__": SAFE_PYTHON_BUILTINS,
        "df": df,
        "pd": pd,
        "math": math,
    }
    locals_dict: Dict[str, Any] = {}
    try:
        with redirect_stdout(stdout_buffer):
            exec(code, globals_dict, locals_dict)
    except Exception:
        return {"ok": False, "error": "Python worker execution failed."}

    result = locals_dict.get("result")
    if result is None and "result" not in locals_dict:
        return {"ok": False, "error": "Python worker did not set result."}

    return {
        "ok": True,
        "result": _json_compatible(result),
        "stdout": stdout_buffer.getvalue().strip(),
    }


def _run_python_worker(
    *, upload_id: uuid.UUID, rewritten_question: str, context: Dict[str, Any]
) -> str:
    rows = get_upload_rows(upload_id)
    dataframe_rows = []
    for item in rows:
        row = dict(item["data"])
        row["row_number"] = item["row_number"]
        dataframe_rows.append(row)
    df = pd.DataFrame(dataframe_rows)

    code_generation_messages = [
        {
            "role": "system",
            "content": (
                "You are the Python worker agent for dataset QA. "
                "Write Python code using the pandas DataFrame variable df to answer the user's rewritten question. "
                "Set a variable named result to the final answer object. "
                "Do not import anything. Do not access files, network, subprocesses, or system resources. "
                "Return only Python code."
            ),
        },
        {
            "role": "system",
            "content": f"Dataset context:\n{json.dumps(context, ensure_ascii=True)}",
        },
        {"role": "user", "content": rewritten_question},
    ]
    code_raw = _call_groq_messages(code_generation_messages, temperature=0.0)
    code = _extract_python_code(code_raw)
    execution_result = _run_python_code(code, df)
    if not execution_result.get("ok"):
        return UNKNOWN_ANSWER

    answer_messages = [
        {
            "role": "system",
            "content": (
                "You are the Python worker agent finishing a dataset answer. "
                "Answer only from the python execution result and dataset context. "
                f"If the result does not support a grounded answer, reply with exactly: {UNKNOWN_ANSWER}. "
                "Keep the answer concise."
            ),
        },
        {
            "role": "system",
            "content": json.dumps(
                {
                    "rewritten_question": rewritten_question,
                    "python_code": code,
                    "execution_result": execution_result,
                },
                ensure_ascii=True,
            ),
        },
        {"role": "user", "content": rewritten_question},
    ]
    return _normalize_unknown_answer(_call_groq_messages(answer_messages, temperature=0.0))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    ensure_schema()


def _json_compatible(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (uuid.UUID,)):
        return str(value)
    if hasattr(value, "isoformat"):
        # datetime/date-like
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        # numpy/pandas scalar
        try:
            scalar = value.item()
            if isinstance(scalar, float) and (math.isnan(scalar) or math.isinf(scalar)):
                return None
            return scalar
        except Exception:
            pass
    return str(value)


def _iter_csv_rows_and_stats(
    raw_bytes: bytes,
) -> Tuple[
    Iterable[Tuple[int, Dict[str, Any]]],
    Callable[[], Dict[str, Any]],
    int,
    list[str],
]:
    if not raw_bytes:
        empty_stats = {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "missing_by_column": {},
            "rating_counts": [],
            "rating_category_counts": [],
        }
        return iter(()), (lambda: empty_stats), 0, []

    reader = pd.read_csv(BytesIO(raw_bytes), chunksize=2000)

    try:
        first_chunk = next(reader)
    except StopIteration:
        empty_stats = {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "missing_by_column": {},
            "rating_counts": [],
            "rating_category_counts": [],
        }
        return iter(()), (lambda: empty_stats), 0, []

    column_names = [str(c) for c in list(first_chunk.columns)]
    columns_count = len(column_names)
    rows_count = 0

    missing_by_column: Dict[str, int] = {name: 0 for name in column_names[:50]}
    rating_counts: Dict[str, int] = {}
    rating_category_counts: Dict[str, int] = {}

    def _update_aggregates(chunk: pd.DataFrame) -> None:
        nonlocal rows_count
        rows_count += int(len(chunk))

        if missing_by_column:
            missing = chunk.isna().sum().to_dict()
            for key in list(missing_by_column.keys()):
                missing_by_column[key] += int(missing.get(key, 0) or 0)

        if "rating" in chunk.columns:
            vc = chunk["rating"].astype("string").fillna("(missing)").value_counts()
            for rating, count in vc.items():
                rating_counts[str(rating)] = rating_counts.get(str(rating), 0) + int(
                    count
                )

        if "rating_category" in chunk.columns:
            vc = (
                chunk["rating_category"]
                .astype("string")
                .fillna("(missing)")
                .value_counts()
            )
            for category, count in vc.items():
                rating_category_counts[str(category)] = rating_category_counts.get(
                    str(category), 0
                ) + int(count)

    def _chunk_to_row_dicts(chunk: pd.DataFrame) -> Iterator[Dict[str, Any]]:
        normalized = chunk.where(pd.notnull(chunk), None)
        for record in normalized.to_dict(orient="records"):
            yield {k: _json_compatible(v) for k, v in record.items()}

    def row_iter() -> Iterator[Tuple[int, Dict[str, Any]]]:
        row_number = 1
        chunk = first_chunk
        while True:
            _update_aggregates(chunk)
            for record in _chunk_to_row_dicts(chunk):
                yield row_number, record
                row_number += 1
            try:
                chunk = next(reader)
            except StopIteration:
                break

    backend_stats = {
        "rows": None,
        "columns": columns_count,
        "column_names": column_names,
        "missing_by_column": missing_by_column,
        "rating_counts": [],
        "rating_category_counts": [],
    }

    def finalize_stats() -> Dict[str, Any]:
        backend_stats["rows"] = rows_count
        backend_stats["rating_counts"] = [
            {"rating": k, "count": rating_counts[k]}
            for k in sorted(rating_counts.keys())
        ][:20]
        backend_stats["rating_category_counts"] = [
            {"category": k, "count": rating_category_counts[k]}
            for k in sorted(rating_category_counts.keys())
        ][:20]
        return backend_stats

    # rows_count is updated lazily while iterating row_iter()
    return row_iter(), finalize_stats, columns_count, column_names


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """Accept a CSV upload, persist to Postgres, and return an upload id plus stats."""
    filename = file.filename or "uploaded CSV"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    try:
        raw_bytes = await file.read()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to read the CSV file: {exc}")
    finally:
        await file.close()

    try:
        rows_iter, finalize_stats, columns_count, column_names = _iter_csv_rows_and_stats(
            raw_bytes
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to parse the CSV file: {exc}")

    try:
        reset_all_upload_data()
        upload_id = create_upload_placeholder(filename=filename)
        inserted = insert_upload_rows(upload_id=upload_id, rows=rows_iter)
        backend_stats = finalize_stats()
        backend_stats["rows"] = int(inserted)
        finalize_upload(
            upload_id=upload_id,
            rows_count=int(inserted),
            columns_count=int(columns_count),
            column_names=column_names,
            backend_stats=backend_stats,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save data to the database: {exc}")

    return {
        "upload_id": str(upload_id),
        "filename": filename,
        "rows": int(inserted),
        "columns": int(columns_count),
    }


@app.get("/uploads/{upload_id}/backend-stats")
def upload_backend_stats(upload_id: uuid.UUID):
    try:
        return get_backend_stats(upload_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Upload not found.")


@app.get("/uploads/{upload_id}/db-aggregates")
def upload_db_aggregates(upload_id: uuid.UUID):
    try:
        return get_db_aggregates(upload_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Upload not found.")


@app.post("/uploads/{upload_id}/chat")
def upload_chat(upload_id: uuid.UUID, request: ChatRequest):
    try:
        context = _build_chat_context(upload_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Upload not found.")

    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required.")

    if not _question_appears_data_related(question, context.get("column_names", [])):
        return {"answer": UNKNOWN_ANSWER, "model": None, "agent": "unknown"}

    try:
        decision = _get_commander_decision(
            question=question,
            history=request.history,
            context=context,
        )
        agent = decision["agent"]
        rewritten_question = decision["rewritten_question"]
        if agent == "sql":
            answer = _run_sql_worker(
                upload_id=upload_id,
                rewritten_question=rewritten_question,
                context=context,
            )
        elif agent == "python":
            answer = _run_python_worker(
                upload_id=upload_id,
                rewritten_question=rewritten_question,
                context=context,
            )
        else:
            answer = UNKNOWN_ANSWER
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "answer": _normalize_unknown_answer(answer),
        "model": GROQ_MODEL,
        "agent": decision.get("agent", "unknown"),
        "rewritten_question": decision.get("rewritten_question", question),
        "assumptions": decision.get("assumptions", []),
    }
