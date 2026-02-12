from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


def init_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "create table if not exists meta_updates (dataset text primary key, last_date text)"
    )
    conn.commit()
    return conn


def set_last_date(conn: sqlite3.Connection, dataset: str, last_date: str) -> None:
    conn.execute(
        "insert into meta_updates(dataset, last_date) values(?, ?) "
        "on conflict(dataset) do update set last_date=excluded.last_date",
        (dataset, last_date),
    )
    conn.commit()


def get_last_date(conn: sqlite3.Connection, dataset: str) -> str | None:
    row = conn.execute(
        "select last_date from meta_updates where dataset = ?", (dataset,)
    ).fetchone()
    return row[0] if row else None


def replace_table(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    df.to_sql(table, conn, if_exists="replace", index=False)


def read_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    return pd.read_sql_query(f"select * from {table}", conn)


def upsert_parquet_by_year(
    df: pd.DataFrame,
    base_dir: Path,
    date_col: str,
    key_cols: Iterable[str],
) -> None:
    if df.empty:
        return
    data = df.copy()
    data[date_col] = data[date_col].astype(str)
    data["year"] = data[date_col].str.slice(0, 4)
    for year, part in data.groupby("year"):
        path = base_dir / f"{year}.parquet"
        part = part.drop(columns=["year"])
        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, part], ignore_index=True)
            combined = combined.drop_duplicates(subset=list(key_cols), keep="last")
        else:
            combined = part
        combined = combined.sort_values(list(key_cols))
        combined.to_parquet(path, index=False)
