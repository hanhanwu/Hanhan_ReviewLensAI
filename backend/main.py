import uuid
from io import BytesIO
import math
from typing import Any, Callable, Dict, Iterable, Iterator, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from backend.db import (
    create_upload_placeholder,
    ensure_schema,
    finalize_upload,
    get_backend_stats,
    get_db_aggregates,
    insert_upload_rows,
    reset_all_upload_data,
)

app = FastAPI(
    title="ReviewLens CSV Counter",
    description="Upload a CSV and report how many rows and columns it contains.",
)

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
