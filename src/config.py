from __future__ import annotations

import os
from pathlib import Path


class AppConfig:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parents[1]
        self.data_dir = self.base_dir / "data"
        self.parquet_dir = self.data_dir / "parquet"
        self.price_dir = self.parquet_dir / "price_daily"
        self.balance_dir = self.parquet_dir / "balance_sheet"
        self.income_dir = self.parquet_dir / "income_statement"
        self.cashflow_dir = self.parquet_dir / "cashflow_statement"
        self.indicator_dir = self.parquet_dir / "fina_indicator"
        self.sqlite_path = self.data_dir / "meta.db"
        self.default_start_date = "20210210"
        self.default_end_date = "20260210"
        self.data_source = os.getenv("DATA_SOURCE", "akshare").lower()
        self.price_source = os.getenv("PRICE_SOURCE", self.data_source).lower()
        self.request_sleep = float(os.getenv("REQUEST_SLEEP", "0.3"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.retry_backoff = float(os.getenv("RETRY_BACKOFF", "1.5"))

    def ensure_dirs(self) -> None:
        self.price_dir.mkdir(parents=True, exist_ok=True)
        self.balance_dir.mkdir(parents=True, exist_ok=True)
        self.income_dir.mkdir(parents=True, exist_ok=True)
        self.cashflow_dir.mkdir(parents=True, exist_ok=True)
        self.indicator_dir.mkdir(parents=True, exist_ok=True)
