import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:

    import adata  # type: ignore
except Exception:  # noqa: BLE001
    adata = None



logger = logging.getLogger(__name__)

CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)


def _cache_path(code: str, period: str) -> Path:
    safe_code = code.replace("/", "_")
    return CACHE_DIR / f"{safe_code}_{period}.csv"


def _load_cache(code: str, period: str) -> Optional[pd.DataFrame]:
    path = _cache_path(code, period)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        logger.info("命中缓存: %s", path)
        return df
    except Exception as exc:  # noqa: BLE001
        logger.warning("读取缓存失败: %s", exc)
        return None


def _save_cache(code: str, period: str, df: pd.DataFrame) -> None:
    path = _cache_path(code, period)
    df.to_csv(path, index=False)
    logger.info("已写入缓存: %s", path)


def _mock_data() -> pd.DataFrame:

    logger.warning("使用模拟数据 (未安装 adata 或取数失败)")

    dates = pd.date_range(end=dt.date.today(), periods=200, freq="B")
    close = pd.Series(100 + pd.Series(range(len(dates))).rolling(5).mean().fillna(0))
    df = pd.DataFrame(
        {
            "date": dates,
            "open": close + 1,
            "high": close + 3,
            "low": close - 2,
            "close": close,
            "volume": 1000000,
        }
    )
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_map = {
        "日期": "date",
        "时间": "date",
        "date": "date",
        "Date": "date",
        "开盘": "open",
        "open": "open",
        "Open": "open",
        "收盘": "close",
        "close": "close",
        "Close": "close",
        "最高": "high",
        "high": "high",
        "High": "high",
        "最低": "low",
        "low": "low",
        "Low": "low",
        "成交量": "volume",
        "volume": "volume",
        "Volume": "volume",
    }
    df = df.rename(columns={col: column_map.get(col, col) for col in df.columns})
    return df


def _resolve_callable(path: str):
    current = adata
    for chunk in path.split("."):
        if current is None:
            return None
        current = getattr(current, chunk, None)
    return current


def _call_with_available_args(func, params: dict):
    try:
        import inspect

        signature = inspect.signature(func)
        accepted = {
            name: value
            for name, value in params.items()
            if name in signature.parameters and value is not None
        }
        return func(**accepted)
    except Exception as exc:  # noqa: BLE001
        logger.warning("调用 adata 接口失败: %s", exc)
        return None


def _fetch_from_adata(code: str, period: str, start: Optional[str], end: Optional[str]) -> Optional[pd.DataFrame]:
    if adata is None:
        logger.warning("adata 未安装，使用模拟数据")
        return None
    candidates = [
        "stock.market.get_market",
        "stock.hist.get_hist",
        "stock.hist",
        "stock.daily",
    ]
    params = {
        "code": code,
        "symbol": code,
        "period": period,
        "k_type": period,
        "start_date": start,
        "end_date": end,
        "start": start,
        "end": end,
    }
    for path in candidates:
        func = _resolve_callable(path)
        if not callable(func):
            continue
        logger.info("请求 adata 数据: %s (%s)", code, path)
        df = _call_with_available_args(func, params)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    logger.warning("adata 未返回有效数据，回退到模拟数据")
    return None



def fetch_stock_data(code: str, period: str = "daily", start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    cached = _load_cache(code, period)
    if cached is not None:
        df = cached
    else:

        df = _fetch_from_adata(code=code, period=period, start=start, end=end)
        if df is None:
            df = _mock_data()

        _save_cache(code, period, df)

    df = _normalize_columns(df)
    if "date" not in df.columns:
        logger.warning("未找到日期列，回退到模拟数据")
        df = _mock_data()
        df = _normalize_columns(df)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df["timestamp"] = df["date"].dt.strftime("%Y-%m-%d")
    return df
