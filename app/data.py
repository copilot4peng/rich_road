"""
股票数据获取模块
基于 adata 库获取A股行情数据
参考文档: https://adata.30006124.xyz/
"""
import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import adata
except ImportError:
    adata = None

logger = logging.getLogger(__name__)

CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)


def _cache_path(code: str, period: str) -> Path:
    """生成缓存文件路径"""
    safe_code = code.replace("/", "_")
    return CACHE_DIR / f"{safe_code}_{period}.csv"


def _load_cache(code: str, period: str) -> Optional[pd.DataFrame]:
    """从缓存文件加载数据"""
    path = _cache_path(code, period)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"])
        logger.info("命中缓存: %s", path)
        return df
    except Exception as exc:
        logger.warning("读取缓存失败: %s", exc)
        return None


def _save_cache(code: str, period: str, df: pd.DataFrame) -> None:
    """保存数据到缓存文件"""
    path = _cache_path(code, period)
    try:
        df.to_csv(path, index=False)
        logger.info("已写入缓存: %s", path)
    except Exception as exc:
        logger.warning("写入缓存失败: %s", exc)


def _mock_data() -> pd.DataFrame:
    """生成模拟数据，用于测试或 adata 不可用时"""
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
    """
    标准化列名
    adata返回的列名可能是中文或英文，统一转换为英文小写
    """
    # 定义日期列的优先级（从高到低）
    date_columns = ["trade_time", "trade_date", "日期", "时间", "date", "Date"]
    
    # 找到第一个存在的日期列
    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break
    
    # 标准列名映射
    column_map = {
        # 价格相关
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
        # 成交量
        "成交量": "volume",
        "volume": "volume",
        "Volume": "volume",
    }
    
    # 只映射第一个找到的日期列
    if date_col and date_col != "date":
        column_map[date_col] = "date"
    
    # 重命名列
    df = df.rename(columns={col: column_map.get(col, col) for col in df.columns})
    
    # 如果有重复的 date 列，只保留第一个
    if "date" in df.columns and df.columns.tolist().count("date") > 1:
        # 找到所有 date 列的位置
        date_indices = [i for i, col in enumerate(df.columns) if col == "date"]
        # 删除除第一个之外的所有 date 列
        cols_to_keep = [i for i in range(len(df.columns)) if i not in date_indices[1:]]
        df = df.iloc[:, cols_to_keep]
    
    return df


def _period_to_k_type(period: str) -> int:
    """
    将周期字符串转换为adata的k_type参数
    k_type: 1=日K, 2=周K, 3=月K
    """
    period_map = {
        "daily": 1,
        "day": 1,
        "d": 1,
        "1d": 1,
        "weekly": 2,
        "week": 2,
        "w": 2,
        "1w": 2,
        "monthly": 3,
        "month": 3,
        "m": 3,
        "1m": 3,
    }
    return period_map.get(period.lower(), 1)


def _fetch_from_adata(
    code: str, 
    period: str = "daily", 
    start: Optional[str] = None, 
    end: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    从 adata 获取股票行情数据
    
    Args:
        code: 股票代码，如 '000001' (平安银行)
        period: 周期类型，如 'daily', 'weekly', 'monthly'
        start: 开始日期，格式 'YYYY-MM-DD'
        end: 结束日期，格式 'YYYY-MM-DD'
    
    Returns:
        DataFrame 或 None
    """
    if adata is None:
        logger.warning("adata 未安装，请运行: pip install adata")
        return None
    
    try:
        # 根据 adata 官方文档，使用正确的 API
        k_type = _period_to_k_type(period)
        
        logger.info("请求 adata 数据: stock_code=%s, k_type=%d, start=%s, end=%s", 
                   code, k_type, start, end)
        
        # 调用 adata.stock.market.get_market
        # 参数: stock_code, k_type, start_date, end_date
        df = adata.stock.market.get_market(
            stock_code=code,
            k_type=k_type,
            start_date=start,
            end_date=end
        )
        
        if df is not None and not df.empty:
            logger.info("成功获取 %d 条数据", len(df))
            return df
        else:
            logger.warning("adata 返回空数据")
            return None
            
    except AttributeError as exc:
        logger.error("adata API调用失败，请检查adata版本: %s", exc)
        return None
    except Exception as exc:
        logger.error("获取数据失败: %s", exc)
        return None


def fetch_stock_data(
    code: str, 
    period: str = "daily", 
    start: Optional[str] = None, 
    end: Optional[str] = None,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    获取股票行情数据（带缓存）
    
    Args:
        code: 股票代码，如 '000001' (平安银行)
        period: 周期类型，支持 'daily', 'weekly', 'monthly'
        start: 开始日期，格式 'YYYY-MM-DD'
        end: 结束日期，格式 'YYYY-MM-DD'
        use_cache: 是否使用缓存
    
    Returns:
        DataFrame，包含 date, open, high, low, close, volume 等列
    
    Examples:
        >>> df = fetch_stock_data('000001', period='daily', start='2021-01-01')
        >>> print(df.head())
    """
    # 尝试从缓存加载
    if use_cache:
        cached = _load_cache(code, period)
        if cached is not None:
            return cached
    
    # 从 adata 获取数据
    df = _fetch_from_adata(code=code, period=period, start=start, end=end)
    
    # 如果获取失败，使用模拟数据
    if df is None or df.empty:
        logger.warning("无法获取真实数据，使用模拟数据")
        df = _mock_data()
    
    # 标准化列名
    df = _normalize_columns(df)
    
    # 确保有 date 列
    if "date" not in df.columns:
        logger.error("数据缺少日期列，使用模拟数据")
        df = _mock_data()
        df = _normalize_columns(df)
    
    # 转换日期格式
    try:
        df["date"] = pd.to_datetime(df["date"], errors='coerce')
    except Exception as exc:
        logger.error("日期转换失败: %s", exc)
        # 尝试其他常见的日期列
        possible_date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if possible_date_cols:
            logger.info("尝试使用列: %s", possible_date_cols[0])
            df["date"] = pd.to_datetime(df[possible_date_cols[0]], errors='coerce')
        else:
            logger.error("无法找到合适的日期列，使用模拟数据")
            df = _mock_data()
    
    df = df.sort_values("date").reset_index(drop=True)
    df["timestamp"] = df["date"].dt.strftime("%Y-%m-%d")
    
    # 保存到缓存
    if use_cache:
        _save_cache(code, period, df)
    
    return df


def get_stock_list() -> pd.DataFrame:
    """
    获取所有A股股票代码列表
    
    Returns:
        DataFrame，包含 stock_code, short_name, exchange 列
    
    Examples:
        >>> df = get_stock_list()
        >>> print(df.head())
    """
    if adata is None:
        logger.error("adata 未安装")
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange"])
    
    try:
        df = adata.stock.info.all_code()
        logger.info("获取到 %d 只股票", len(df))
        return df
    except Exception as exc:
        logger.error("获取股票列表失败: %s", exc)
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange"])


def get_realtime_quote(codes: list[str]) -> pd.DataFrame:
    """
    获取多只股票的实时行情
    
    Args:
        codes: 股票代码列表，如 ['000001', '000002']
    
    Returns:
        DataFrame，包含实时行情数据
    
    Examples:
        >>> df = get_realtime_quote(['000001', '000002'])
        >>> print(df)
    """
    if adata is None:
        logger.error("adata 未安装")
        return pd.DataFrame()
    
    try:
        df = adata.stock.market.list_market_current(stock_codes=codes)
        logger.info("获取到 %d 只股票的实时行情", len(df))
        return df
    except Exception as exc:
        logger.error("获取实时行情失败: %s", exc)
        return pd.DataFrame()
