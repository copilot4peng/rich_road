import datetime as dt
import logging
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse

from app.data import fetch_stock_data
from app.indicators import IndicatorResult, build_registry
from app.report import render_html_report, render_markdown_report


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="智能股票量化分析平台")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

registry = build_registry()


def _build_candles(df) -> List[dict]:
    return [
        {
            "time": row["timestamp"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
        }
        for _, row in df.iterrows()
    ]


def _detect_cross(series_short: List[float], series_long: List[float], name: str) -> List[str]:
    signals: List[str] = []
    if len(series_short) < 2 or len(series_long) < 2:
        return signals
    if series_short[-2] < series_long[-2] and series_short[-1] > series_long[-1]:
        signals.append(f"{name} 金叉")
    if series_short[-2] > series_long[-2] and series_short[-1] < series_long[-1]:
        signals.append(f"{name} 死叉")
    return signals


def _extract_series(indicators: List[IndicatorResult], name: str) -> Optional[IndicatorResult]:
    for indicator in indicators:
        if indicator.name == name:
            return indicator
    return None


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/")
def index() -> dict:
    return {"message": "智能股票量化分析平台 API 已启动"}


@app.get("/api/indicators/config")
def indicator_config() -> dict:
    return {"indicators": registry.get_all_configs()}


@app.get("/api/data")
def stock_data(
    code: str,
    period: str = Query("daily", description="daily/weekly/monthly"),
    indicators: str = Query("", description="逗号分隔的指标名"),
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    df = fetch_stock_data(code=code, period=period, start=start, end=end)
    indicator_list = [name.strip() for name in indicators.split(",") if name.strip()]
    indicator_results = registry.calculate(indicator_list, df)

    return {
        "code": code,
        "period": period,
        "candles": _build_candles(df),
        "indicators": [
            {
                "name": result.name,
                "type": result.plot_type,
                "series": result.series,
            }
            for result in indicator_results
        ],
    }


@app.get("/api/signals")
def signals(
    code: str,
    period: str = Query("daily", description="daily/weekly/monthly"),
    ma_short: int = 10,
    ma_long: int = 30,
) -> dict:
    df = fetch_stock_data(code=code, period=period)
    df["ma_short"] = df["close"].rolling(window=ma_short).mean()
    df["ma_long"] = df["close"].rolling(window=ma_long).mean()
    signals_list: List[str] = []

    signals_list.extend(
        _detect_cross(df["ma_short"].fillna(0).tolist(), df["ma_long"].fillna(0).tolist(), f"MA{ma_short}/MA{ma_long}")
    )

    indicator_results = registry.calculate(["MACD", "RSI"], df)
    macd = _extract_series(indicator_results, "MACD")
    if macd:
        macd_line = [point["value"] or 0 for point in macd.series["MACD"]]
        signal_line = [point["value"] or 0 for point in macd.series["SIGNAL"]]
        signals_list.extend(_detect_cross(macd_line, signal_line, "MACD"))
    rsi = _extract_series(indicator_results, "RSI")
    if rsi:
        latest_rsi = rsi.series["RSI"][-1]["value"]
        if latest_rsi is not None:
            if latest_rsi > 80:
                signals_list.append("RSI 超买 (>80)")
            if latest_rsi < 20:
                signals_list.append("RSI 超卖 (<20)")

    return {"code": code, "period": period, "signals": signals_list}


@app.get("/api/report/markdown", response_class=PlainTextResponse)
def report_markdown(
    code: str,
    period: str = Query("daily", description="daily/weekly/monthly"),
    indicators: str = Query("MA,MACD,RSI", description="逗号分隔的指标名"),
) -> str:
    payload = _build_report_payload(code, period, indicators)
    return render_markdown_report(payload)


@app.get("/api/report/html", response_class=HTMLResponse)
def report_html(
    code: str,
    period: str = Query("daily", description="daily/weekly/monthly"),
    indicators: str = Query("MA,MACD,RSI", description="逗号分隔的指标名"),
) -> str:
    payload = _build_report_payload(code, period, indicators)
    return render_html_report(payload)


def _build_report_payload(code: str, period: str, indicators: str) -> dict:
    df = fetch_stock_data(code=code, period=period)
    indicator_list = [name.strip() for name in indicators.split(",") if name.strip()]
    indicator_results = registry.calculate(indicator_list, df)
    signals_list = signals(code=code, period=period)["signals"]

    return {
        "code": code,
        "period": period,
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "candles": _build_candles(df),
        "indicators": [
            {
                "name": result.name,
                "type": result.plot_type,
                "series": result.series,
            }
            for result in indicator_results
        ],
        "signals": signals_list,
    }
