from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb


def _require_db_url() -> str:
    db_url = os.getenv("NERO_DB_URL")
    if not db_url:
        raise RuntimeError('Missing environment variable "NERO_DB_URL".')
    return db_url


@contextmanager
def get_conn() -> Iterable[Connection]:
    conn = psycopg.connect(_require_db_url())
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                  upload_id UUID PRIMARY KEY,
                  filename TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  rows_count INTEGER NOT NULL,
                  columns_count INTEGER NOT NULL,
                  column_names TEXT[] NOT NULL,
                  backend_stats JSONB NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_rows (
                  id BIGSERIAL PRIMARY KEY,
                  upload_id UUID NOT NULL REFERENCES uploads(upload_id) ON DELETE CASCADE,
                  row_number INTEGER NOT NULL,
                  data JSONB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS upload_rows_upload_id_idx
                  ON upload_rows(upload_id);
                """
            )
        conn.commit()


def reset_all_upload_data() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE uploads RESTART IDENTITY CASCADE;")
        conn.commit()


def create_upload_placeholder(*, filename: str, upload_id: Optional[uuid.UUID] = None) -> uuid.UUID:
    upload_id = upload_id or uuid.uuid4()
    created_at = datetime.now(timezone.utc)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO uploads (
                  upload_id,
                  filename,
                  created_at,
                  rows_count,
                  columns_count,
                  column_names,
                  backend_stats
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (upload_id, filename, created_at, 0, 0, [], Jsonb({})),
            )
        conn.commit()

    return upload_id


def insert_upload_rows(
    *, upload_id: uuid.UUID, rows: Iterable[Tuple[int, Dict[str, Any]]]
) -> int:
    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            batch: List[Tuple[uuid.UUID, int, Jsonb]] = []
            for row_number, row_data in rows:
                inserted += 1
                batch.append((upload_id, row_number, Jsonb(row_data)))
                if len(batch) >= 1000:
                    cur.executemany(
                        "INSERT INTO upload_rows (upload_id, row_number, data) VALUES (%s, %s, %s);",
                        batch,
                    )
                    batch.clear()

            if batch:
                cur.executemany(
                    "INSERT INTO upload_rows (upload_id, row_number, data) VALUES (%s, %s, %s);",
                    batch,
                )

        conn.commit()

    return inserted


def finalize_upload(
    *,
    upload_id: uuid.UUID,
    rows_count: int,
    columns_count: int,
    column_names: List[str],
    backend_stats: Dict[str, Any],
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE uploads
                SET rows_count = %s,
                    columns_count = %s,
                    column_names = %s,
                    backend_stats = %s
                WHERE upload_id = %s;
                """,
                (
                    rows_count,
                    columns_count,
                    column_names,
                    Jsonb(backend_stats),
                    upload_id,
                ),
            )
        conn.commit()


def insert_upload(
    *,
    filename: str,
    rows_count: int,
    columns_count: int,
    column_names: List[str],
    backend_stats: Dict[str, Any],
    rows: Iterable[Tuple[int, Dict[str, Any]]],
    upload_id: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    upload_id = upload_id or uuid.uuid4()
    created_at = datetime.now(timezone.utc)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO uploads (
                  upload_id,
                  filename,
                  created_at,
                  rows_count,
                  columns_count,
                  column_names,
                  backend_stats
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    upload_id,
                    filename,
                    created_at,
                    rows_count,
                    columns_count,
                    column_names,
                    Jsonb(backend_stats),
                ),
            )

            batch: List[Tuple[uuid.UUID, int, Jsonb]] = []
            for row_number, row_data in rows:
                batch.append((upload_id, row_number, Jsonb(row_data)))
                if len(batch) >= 1000:
                    cur.executemany(
                        "INSERT INTO upload_rows (upload_id, row_number, data) VALUES (%s, %s, %s);",
                        batch,
                    )
                    batch.clear()

            if batch:
                cur.executemany(
                    "INSERT INTO upload_rows (upload_id, row_number, data) VALUES (%s, %s, %s);",
                    batch,
                )

        conn.commit()

    return upload_id


def get_backend_stats(upload_id: uuid.UUID) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT backend_stats FROM uploads WHERE upload_id = %s;",
                (upload_id,),
            )
            row = cur.fetchone()
            if not row:
                raise KeyError("upload not found")
            return row[0]


def get_db_aggregates(upload_id: uuid.UUID) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rows_count, columns_count, column_names, filename, created_at FROM uploads WHERE upload_id = %s;",
                (upload_id,),
            )
            meta = cur.fetchone()
            if not meta:
                raise KeyError("upload not found")

            rows_count, columns_count, column_names, filename, created_at = meta

            cur.execute(
                """
                SELECT
                  key,
                  COUNT(*) AS present_count,
                  COUNT(*) FILTER (WHERE value <> 'null'::jsonb) AS non_null_count
                FROM upload_rows
                CROSS JOIN LATERAL jsonb_each(upload_rows.data) AS kv(key, value)
                WHERE upload_rows.upload_id = %s
                GROUP BY key
                ORDER BY non_null_count DESC, present_count DESC, key ASC
                LIMIT 50;
                """,
                (upload_id,),
            )
            column_counts = [
                {"column": r[0], "present_count": int(r[1]), "non_null_count": int(r[2])}
                for r in cur.fetchall()
            ]

            rating_category_counts: List[Dict[str, Any]] = []
            cur.execute(
                """
                SELECT COUNT(*)
                FROM upload_rows
                WHERE upload_id = %s AND (data ? 'rating_category');
                """,
                (upload_id,),
            )
            has_rating_category = (cur.fetchone() or [0])[0] > 0
            if has_rating_category:
                cur.execute(
                    """
                    SELECT COALESCE(data->>'rating_category', '(missing)') AS category, COUNT(*) AS count
                    FROM upload_rows
                    WHERE upload_id = %s
                    GROUP BY category
                    ORDER BY count DESC, category ASC
                    LIMIT 20;
                    """,
                    (upload_id,),
                )
                rating_category_counts = [
                    {"category": r[0], "count": int(r[1])} for r in cur.fetchall()
                ]

            rating_counts: List[Dict[str, Any]] = []
            cur.execute(
                """
                SELECT COUNT(*)
                FROM upload_rows
                WHERE upload_id = %s AND (data ? 'rating');
                """,
                (upload_id,),
            )
            has_rating = (cur.fetchone() or [0])[0] > 0
            if has_rating:
                cur.execute(
                    """
                    SELECT data->>'rating' AS rating, COUNT(*) AS count
                    FROM upload_rows
                    WHERE upload_id = %s
                    GROUP BY rating
                    ORDER BY count DESC, rating ASC
                    LIMIT 20;
                    """,
                    (upload_id,),
                )
                rating_counts = [
                    {"rating": r[0] or "(missing)", "count": int(r[1])}
                    for r in cur.fetchall()
                ]

            return {
                "upload_id": str(upload_id),
                "filename": filename,
                "created_at": created_at.isoformat() if created_at else None,
                "rows_count": int(rows_count),
                "columns_count": int(columns_count),
                "column_names": list(column_names or []),
                "column_value_counts": column_counts,
                "rating_category_counts": rating_category_counts,
                "rating_counts": rating_counts,
            }
