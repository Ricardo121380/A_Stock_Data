from __future__ import annotations

from .config import AppConfig
from .data_source import (
    fetch_main_board_stocks,
    fetch_price_data_for_code,
    fetch_trade_calendar,
    fetch_balance_sheet_for_code,
    fetch_income_statement_for_code,
    fetch_cashflow_statement_for_code,
    fetch_financial_indicator_for_code,
    next_day,
)
from .storage import get_last_date, replace_table, set_last_date, upsert_parquet_by_year


def init_storage(cfg: AppConfig) -> None:
    cfg.ensure_dirs()


def _progress_key(dataset: str, mode: str) -> str:
    return f"{dataset}_progress_{mode}"


def _sorted_codes(stocks):
    sorted_stocks = stocks.sort_values("ts_code").reset_index(drop=True)
    codes = sorted_stocks["symbol"].astype(str).str.zfill(6).tolist()
    return codes


def full_download(cfg: AppConfig, start_date: str, end_date: str) -> None:
    stocks = fetch_main_board_stocks(cfg)
    trade_cal = fetch_trade_calendar(cfg, start_date, end_date)

    from .storage import init_sqlite

    conn = init_sqlite(cfg.sqlite_path)
    replace_table(conn, "stock_basic", stocks)
    replace_table(conn, "trade_calendar", trade_cal)

    codes = _sorted_codes(stocks)

    price_progress = get_last_date(conn, _progress_key("price_daily", "full"))
    price_start_index = int(price_progress) if price_progress else 0
    for idx, code in enumerate(codes[price_start_index:], start=price_start_index):
        try:
            price = fetch_price_data_for_code(cfg, code, start_date, end_date)
        except Exception:
            continue
        upsert_parquet_by_year(
            price, cfg.price_dir, "trade_date", ["ts_code", "trade_date", "adjust"]
        )
        if not price.empty:
            set_last_date(conn, "price_daily", price["trade_date"].max())
        set_last_date(conn, _progress_key("price_daily", "full"), str(idx + 1))

    for dataset, fetcher, target_dir in [
        ("balance_sheet", fetch_balance_sheet_for_code, cfg.balance_dir),
        ("income_statement", fetch_income_statement_for_code, cfg.income_dir),
        ("cashflow_statement", fetch_cashflow_statement_for_code, cfg.cashflow_dir),
        ("fina_indicator", fetch_financial_indicator_for_code, cfg.indicator_dir),
    ]:
        progress = get_last_date(conn, _progress_key(dataset, "full"))
        start_index = int(progress) if progress else 0
        for idx, code in enumerate(codes[start_index:], start=start_index):
            try:
                df = fetcher(cfg, code, start_date, end_date)
            except Exception:
                continue
            upsert_parquet_by_year(df, target_dir, "end_date", ["ts_code", "end_date"])
            if not df.empty:
                set_last_date(conn, dataset, df["end_date"].max())
            set_last_date(conn, _progress_key(dataset, "full"), str(idx + 1))

    conn.close()


def incremental_update(cfg: AppConfig, end_date: str) -> None:
    from .storage import init_sqlite

    conn = init_sqlite(cfg.sqlite_path)

    stocks = fetch_main_board_stocks(cfg)
    replace_table(conn, "stock_basic", stocks)

    trade_cal = fetch_trade_calendar(cfg, cfg.default_start_date, end_date)
    replace_table(conn, "trade_calendar", trade_cal)

    last_price_date = get_last_date(conn, "price_daily")
    if last_price_date:
        price_start = next_day(last_price_date)
    else:
        price_start = cfg.default_start_date

    codes = _sorted_codes(stocks)

    price_progress = get_last_date(conn, _progress_key("price_daily", "update"))
    price_start_index = int(price_progress) if price_progress else 0
    for idx, code in enumerate(codes[price_start_index:], start=price_start_index):
        try:
            price = fetch_price_data_for_code(cfg, code, price_start, end_date)
        except Exception:
            continue
        upsert_parquet_by_year(
            price, cfg.price_dir, "trade_date", ["ts_code", "trade_date", "adjust"]
        )
        if not price.empty:
            set_last_date(conn, "price_daily", price["trade_date"].max())
        set_last_date(conn, _progress_key("price_daily", "update"), str(idx + 1))

    for dataset, fetcher, target_dir in [
        ("balance_sheet", fetch_balance_sheet_for_code, cfg.balance_dir),
        ("income_statement", fetch_income_statement_for_code, cfg.income_dir),
        ("cashflow_statement", fetch_cashflow_statement_for_code, cfg.cashflow_dir),
        ("fina_indicator", fetch_financial_indicator_for_code, cfg.indicator_dir),
    ]:
        last_date = get_last_date(conn, dataset)
        start_date = next_day(last_date) if last_date else cfg.default_start_date
        progress = get_last_date(conn, _progress_key(dataset, "update"))
        start_index = int(progress) if progress else 0
        for idx, code in enumerate(codes[start_index:], start=start_index):
            try:
                df = fetcher(cfg, code, start_date, end_date)
            except Exception:
                continue
            upsert_parquet_by_year(df, target_dir, "end_date", ["ts_code", "end_date"])
            if not df.empty:
                set_last_date(conn, dataset, df["end_date"].max())
            set_last_date(conn, _progress_key(dataset, "update"), str(idx + 1))

    conn.close()
