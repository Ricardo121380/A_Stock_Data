from __future__ import annotations

from datetime import datetime, timedelta
import time

import pandas as pd

from .config import AppConfig


def _normalize_date(value: str) -> str:
    text = str(value).strip()
    for sep in ["-", "/", "."]:
        text = text.replace(sep, "")
    return text[:8]


def _to_ymd(value: str) -> str:
    text = _normalize_date(value)
    if len(text) == 8:
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return value


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _akshare():
    import akshare as ak

    return ak


def _adata():
    import adata

    return adata


def _retry_call(func, cfg: AppConfig, *args, **kwargs):
    attempt = 0
    delay = cfg.retry_backoff
    while True:
        try:
            return func(*args, **kwargs)
        except Exception:
            attempt += 1
            if attempt >= cfg.max_retries:
                raise
            time.sleep(delay)
            delay *= 2


def _sleep(cfg: AppConfig) -> None:
    if cfg.request_sleep > 0:
        time.sleep(cfg.request_sleep)


def _ak_stock_list_main_board(cfg: AppConfig) -> pd.DataFrame:
    ak = _akshare()
    try:
        sh = _retry_call(ak.stock_info_sh_name_code, cfg, indicator="主板A股")
    except TypeError:
        sh = _retry_call(ak.stock_info_sh_name_code, cfg)
    try:
        sz = _retry_call(ak.stock_info_sz_name_code, cfg, indicator="A股列表")
    except TypeError:
        sz = _retry_call(ak.stock_info_sz_name_code, cfg)

    if "板块" in sz.columns:
        sz = sz[sz["板块"] == "主板"]

    sh_code_col = _pick_col(sh, ["证券代码", "A股代码", "股票代码", "code"])
    sh_name_col = _pick_col(sh, ["证券简称", "A股简称", "股票简称", "name"])
    sz_code_col = _pick_col(sz, ["A股代码", "证券代码", "股票代码", "code"])
    sz_name_col = _pick_col(sz, ["A股简称", "证券简称", "股票简称", "name"])

    sh = sh[[sh_code_col, sh_name_col]].copy()
    sz = sz[[sz_code_col, sz_name_col]].copy()
    sh.columns = ["symbol", "name"]
    sz.columns = ["symbol", "name"]

    sh["symbol"] = sh["symbol"].astype(str).str.zfill(6)
    sz["symbol"] = sz["symbol"].astype(str).str.zfill(6)

    sh = sh[sh["symbol"].str.startswith(("600", "601", "603", "605"))]
    sz = sz[sz["symbol"].str.startswith(("000", "001", "002"))]

    sh["ts_code"] = sh["symbol"] + ".SH"
    sz["ts_code"] = sz["symbol"] + ".SZ"
    sh["exchange"] = "SSE"
    sz["exchange"] = "SZSE"
    sh["market"] = "主板"
    sz["market"] = "主板"

    combined = pd.concat([sh, sz], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ts_code"], keep="last")
    return combined[["ts_code", "symbol", "name", "exchange", "market"]]


def _adata_stock_list_main_board(cfg: AppConfig) -> pd.DataFrame:
    adata = _adata()
    df = _retry_call(adata.stock.info.all_code, cfg)
    df = df.rename(
        columns={"stock_code": "symbol", "short_name": "name", "exchange": "exchange"}
    )
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    sh = df[(df["exchange"] == "SH") & df["symbol"].str.startswith(("600", "601", "603", "605"))]
    sz = df[(df["exchange"] == "SZ") & df["symbol"].str.startswith(("000", "001", "002"))]

    sh = sh.copy()
    sz = sz.copy()
    sh["ts_code"] = sh["symbol"] + ".SH"
    sz["ts_code"] = sz["symbol"] + ".SZ"
    sh["exchange"] = "SSE"
    sz["exchange"] = "SZSE"
    sh["market"] = "主板"
    sz["market"] = "主板"

    combined = pd.concat([sh, sz], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ts_code"], keep="last")
    return combined[["ts_code", "symbol", "name", "exchange", "market"]]


def fetch_main_board_stocks(cfg: AppConfig) -> pd.DataFrame:
    if cfg.data_source == "adata":
        return _adata_stock_list_main_board(cfg)
    return _ak_stock_list_main_board(cfg)


def fetch_trade_calendar(cfg: AppConfig, start_date: str, end_date: str) -> pd.DataFrame:
    start = _normalize_date(start_date)
    end = _normalize_date(end_date)
    if cfg.data_source == "adata":
        adata = _adata()
        df = _retry_call(adata.stock.info.trade_calendar, cfg)
        date_col = _pick_col(df, ["trade_date", "交易日期", "日期"])
        df = df[[date_col]].copy()
        df.columns = ["cal_date"]
        df["cal_date"] = df["cal_date"].apply(_normalize_date)
        df = df[(df["cal_date"] >= start) & (df["cal_date"] <= end)]
        df["is_open"] = 1
        return df

    ak = _akshare()
    try:
        df = _retry_call(
            ak.tool_trade_date_hist_sina, cfg, start_date=start, end_date=end
        )
    except TypeError:
        df = _retry_call(ak.tool_trade_date_hist_sina, cfg)
    date_col = _pick_col(df, ["trade_date", "交易日期", "日期"])
    df = df[[date_col]].copy()
    df.columns = ["cal_date"]
    df["cal_date"] = df["cal_date"].apply(_normalize_date)
    df = df[(df["cal_date"] >= start) & (df["cal_date"] <= end)]
    df["is_open"] = 1
    return df


def _normalize_price_df(df: pd.DataFrame, ts_code: str, adjust: str) -> pd.DataFrame:
    mapping = {
        "日期": "trade_date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "date": "trade_date",
    }
    renamed = {c: mapping[c] for c in df.columns if c in mapping}
    data = df.rename(columns=renamed).copy()
    if "trade_date" in data.columns:
        data["trade_date"] = data["trade_date"].apply(_normalize_date)
    data["ts_code"] = ts_code
    data["adjust"] = adjust
    return data


def _ak_price_data(
    cfg: AppConfig, codes: list[str], start_date: str, end_date: str
) -> pd.DataFrame:
    ak = _akshare()
    frames: list[pd.DataFrame] = []
    for code in codes:
        raw = _retry_call(
            ak.stock_zh_a_hist,
            cfg,
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        _sleep(cfg)
        if not raw.empty:
            frames.append(_normalize_price_df(raw, _code_to_ts(code), "none"))
        qfq = _retry_call(
            ak.stock_zh_a_hist,
            cfg,
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        _sleep(cfg)
        if not qfq.empty:
            frames.append(_normalize_price_df(qfq, _code_to_ts(code), "qfq"))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _adata_price_data(
    cfg: AppConfig, codes: list[str], start_date: str, end_date: str
) -> pd.DataFrame:
    adata = _adata()
    start = _to_ymd(start_date)
    end = _normalize_date(end_date)
    frames: list[pd.DataFrame] = []
    for code in codes:
        df = _retry_call(
            adata.stock.market.get_market,
            cfg,
            stock_code=code,
            k_type=1,
            start_date=start,
        )
        _sleep(cfg)
        if df is None or df.empty:
            continue
        df = df.copy()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].apply(_normalize_date)
            df = df[df["trade_date"] <= end]
        df["ts_code"] = _code_to_ts(code)
        df["adjust"] = "none"
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _code_to_ts(code: str) -> str:
    code = str(code).zfill(6)
    if code.startswith(("600", "601", "603", "605")):
        return f"{code}.SH"
    return f"{code}.SZ"


def fetch_price_data(
    cfg: AppConfig, stocks: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    codes = stocks["symbol"].astype(str).str.zfill(6).tolist()
    if cfg.price_source == "adata":
        return _adata_price_data(cfg, codes, start_date, end_date)
    return _ak_price_data(cfg, codes, start_date, end_date)


def fetch_price_data_for_code(
    cfg: AppConfig, code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    if cfg.price_source == "adata":
        return _adata_price_data(cfg, [code], start_date, end_date)
    return _ak_price_data(cfg, [code], start_date, end_date)


def _ak_financial_report(cfg: AppConfig, code: str, report_type: str) -> pd.DataFrame:
    ak = _akshare()
    df = _retry_call(
        ak.stock_financial_report_sina, cfg, stock=code, symbol=report_type
    )
    _sleep(cfg)
    date_col = _pick_col(df, ["报表日期", "截止日期", "报告期"])
    if date_col:
        df = df.rename(columns={date_col: "end_date"})
        df["end_date"] = df["end_date"].apply(_normalize_date)
    df["ts_code"] = _code_to_ts(code)
    return df


def _ak_financial_indicator(cfg: AppConfig, code: str) -> pd.DataFrame:
    ak = _akshare()
    df = _retry_call(ak.stock_financial_analysis_indicator, cfg, stock=code)
    _sleep(cfg)
    date_col = _pick_col(df, ["报表日期", "截止日期", "报告期"])
    if date_col:
        df = df.rename(columns={date_col: "end_date"})
        df["end_date"] = df["end_date"].apply(_normalize_date)
    df["ts_code"] = _code_to_ts(code)
    return df


def _filter_by_end_date(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if "end_date" not in df.columns or df.empty:
        return df
    start = _normalize_date(start_date)
    end = _normalize_date(end_date)
    data = df.copy()
    data["end_date"] = data["end_date"].apply(_normalize_date)
    return data[(data["end_date"] >= start) & (data["end_date"] <= end)]


def fetch_balance_sheet(
    cfg: AppConfig, stocks: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in stocks["symbol"].astype(str).str.zfill(6).tolist():
        df = _ak_financial_report(cfg, code, "资产负债表")
        frames.append(_filter_by_end_date(df, start_date, end_date))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_balance_sheet_for_code(
    cfg: AppConfig, code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    df = _ak_financial_report(cfg, code, "资产负债表")
    return _filter_by_end_date(df, start_date, end_date)


def fetch_income_statement(
    cfg: AppConfig, stocks: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in stocks["symbol"].astype(str).str.zfill(6).tolist():
        df = _ak_financial_report(cfg, code, "利润表")
        frames.append(_filter_by_end_date(df, start_date, end_date))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_income_statement_for_code(
    cfg: AppConfig, code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    df = _ak_financial_report(cfg, code, "利润表")
    return _filter_by_end_date(df, start_date, end_date)


def fetch_cashflow_statement(
    cfg: AppConfig, stocks: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in stocks["symbol"].astype(str).str.zfill(6).tolist():
        df = _ak_financial_report(cfg, code, "现金流量表")
        frames.append(_filter_by_end_date(df, start_date, end_date))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_cashflow_statement_for_code(
    cfg: AppConfig, code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    df = _ak_financial_report(cfg, code, "现金流量表")
    return _filter_by_end_date(df, start_date, end_date)


def fetch_financial_indicator(
    cfg: AppConfig, stocks: pd.DataFrame, start_date: str, end_date: str
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code in stocks["symbol"].astype(str).str.zfill(6).tolist():
        df = _ak_financial_indicator(cfg, code)
        frames.append(_filter_by_end_date(df, start_date, end_date))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_financial_indicator_for_code(
    cfg: AppConfig, code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    df = _ak_financial_indicator(cfg, code)
    return _filter_by_end_date(df, start_date, end_date)


def next_day(date_str: str) -> str:
    base = _normalize_date(date_str)
    if len(base) != 8:
        return date_str
    dt = datetime.strptime(base, "%Y%m%d")
    return (dt + timedelta(days=1)).strftime("%Y%m%d")
