import logging
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

import pandas as pd

try:
    import pandas_ta as ta  # type: ignore
except Exception:  # noqa: BLE001
    ta = None


logger = logging.getLogger(__name__)

PlotType = Literal["overlay", "oscillator"]


@dataclass
class IndicatorResult:
    name: str
    plot_type: PlotType
    series: Dict[str, List[dict]]


@dataclass
class BaseIndicator:
    name: str
    params: Dict[str, object] = field(default_factory=dict)
    plot_type: PlotType = "overlay"

    def calculate(self, df: pd.DataFrame) -> Optional[IndicatorResult]:
        raise NotImplementedError


class MAIndicator(BaseIndicator):
    def __init__(self, periods: Optional[List[int]] = None) -> None:
        super().__init__(name="MA", params={"periods": periods or [5, 10, 20, 30, 60]})
        self.plot_type = "overlay"

    def calculate(self, df: pd.DataFrame) -> Optional[IndicatorResult]:
        periods = self.params.get("periods", [])
        series: Dict[str, List[dict]] = {}
        for period in periods:
            col = f"ma_{period}"
            df[col] = df["close"].rolling(window=period).mean()
            series[col.upper()] = [
                {"time": row["timestamp"], "value": None if pd.isna(row[col]) else float(row[col])}
                for _, row in df.iterrows()
            ]
        return IndicatorResult(name=self.name, plot_type=self.plot_type, series=series)


class MACDIndicator(BaseIndicator):
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        super().__init__(name="MACD", params={"fast": fast, "slow": slow, "signal": signal})
        self.plot_type = "oscillator"

    def calculate(self, df: pd.DataFrame) -> Optional[IndicatorResult]:
        if ta is None:
            logger.warning("pandas_ta 未安装，MACD 将跳过计算")
            return None
        macd = ta.macd(df["close"], fast=self.params["fast"], slow=self.params["slow"], signal=self.params["signal"])
        if macd is None:
            return None
        df = df.join(macd)
        series = {
            "MACD": [
                {"time": row["timestamp"], "value": None if pd.isna(row["MACD_12_26_9"]) else float(row["MACD_12_26_9"])}
                for _, row in df.iterrows()
            ],
            "SIGNAL": [
                {"time": row["timestamp"], "value": None if pd.isna(row["MACDs_12_26_9"]) else float(row["MACDs_12_26_9"])}
                for _, row in df.iterrows()
            ],
            "HIST": [
                {"time": row["timestamp"], "value": None if pd.isna(row["MACDh_12_26_9"]) else float(row["MACDh_12_26_9"])}
                for _, row in df.iterrows()
            ],
        }
        return IndicatorResult(name=self.name, plot_type=self.plot_type, series=series)


class KDJIndicator(BaseIndicator):
    def __init__(self, length: int = 9, smooth_k: int = 3, smooth_d: int = 3) -> None:
        super().__init__(name="KDJ", params={"length": length, "smooth_k": smooth_k, "smooth_d": smooth_d})
        self.plot_type = "oscillator"

    def calculate(self, df: pd.DataFrame) -> Optional[IndicatorResult]:
        if ta is None:
            logger.warning("pandas_ta 未安装，KDJ 将跳过计算")
            return None
        kdj = ta.stoch(df["high"], df["low"], df["close"], k=self.params["smooth_k"], d=self.params["smooth_d"], length=self.params["length"])
        if kdj is None:
            return None
        df = df.join(kdj)
        k_col = [col for col in df.columns if col.startswith("STOCHk")][-1]
        d_col = [col for col in df.columns if col.startswith("STOCHd")][-1]
        j_col = "J"
        df[j_col] = 3 * df[k_col] - 2 * df[d_col]
        series = {
            "K": [
                {"time": row["timestamp"], "value": None if pd.isna(row[k_col]) else float(row[k_col])}
                for _, row in df.iterrows()
            ],
            "D": [
                {"time": row["timestamp"], "value": None if pd.isna(row[d_col]) else float(row[d_col])}
                for _, row in df.iterrows()
            ],
            "J": [
                {"time": row["timestamp"], "value": None if pd.isna(row[j_col]) else float(row[j_col])}
                for _, row in df.iterrows()
            ],
        }
        return IndicatorResult(name=self.name, plot_type=self.plot_type, series=series)


class RSIIndicator(BaseIndicator):
    def __init__(self, length: int = 14) -> None:
        super().__init__(name="RSI", params={"length": length})
        self.plot_type = "oscillator"

    def calculate(self, df: pd.DataFrame) -> Optional[IndicatorResult]:
        if ta is None:
            logger.warning("pandas_ta 未安装，RSI 将跳过计算")
            return None
        rsi = ta.rsi(df["close"], length=self.params["length"])
        if rsi is None:
            return None
        df = df.assign(RSI=rsi)
        series = {
            "RSI": [
                {"time": row["timestamp"], "value": None if pd.isna(row["RSI"]) else float(row["RSI"])}
                for _, row in df.iterrows()
            ]
        }
        return IndicatorResult(name=self.name, plot_type=self.plot_type, series=series)


class IndicatorRegistry:
    def __init__(self) -> None:
        self.indicators: Dict[str, BaseIndicator] = {}

    def register(self, indicator: BaseIndicator) -> None:
        self.indicators[indicator.name] = indicator
        logger.info("指标已注册: %s", indicator.name)

    def get_all_configs(self) -> List[dict]:
        return [
            {"name": name, "params": indicator.params, "type": indicator.plot_type}
            for name, indicator in self.indicators.items()
        ]

    def calculate(self, names: List[str], df: pd.DataFrame) -> List[IndicatorResult]:
        results: List[IndicatorResult] = []
        for name in names:
            indicator = self.indicators.get(name)
            if not indicator:
                logger.warning("未找到指标: %s", name)
                continue
            result = indicator.calculate(df.copy())
            if result:
                results.append(result)
        return results


def build_registry() -> IndicatorRegistry:
    registry = IndicatorRegistry()
    registry.register(MAIndicator())
    registry.register(MACDIndicator())
    registry.register(KDJIndicator())
    registry.register(RSIIndicator())
    return registry
