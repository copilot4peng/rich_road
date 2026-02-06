import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import akshare as ak  # type: ignore
except Exception:  # noqa: BLE001
    ak = None


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
    logger.warning("使用模拟数据 (未安装 AkShare 或取数失败)")
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


def fetch_stock_data(code: str, period: str = "daily", start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    cached = _load_cache(code, period)
    if cached is not None:
        df = cached
    else:
        if ak is None:
            df = _mock_data()
        else:
            logger.info("请求 AkShare 数据: %s", code)
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily" if period == "daily" else period,
                start_date=start,
                end_date=end,
                adjust="qfq",
            )
            df = df.rename(
                columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                }
            )
        _save_cache(code, period, df)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df["timestamp"] = df["date"].dt.strftime("%Y-%m-%d")
    return df
