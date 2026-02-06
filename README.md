# 智能股票量化分析平台 (Web版)

基于 FastAPI 的股票量化分析后端，支持指标注册、K 线数据、信号扫描与报告导出。

## 快速开始

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn app.main:app --reload
```

访问 `http://127.0.0.1:8000` 查看前端演示页面，`http://127.0.0.1:8000/docs` 查看交互式 API 文档。

## 主要接口

- `GET /api/indicators/config` 指标配置列表
- `GET /api/data?code=000001&indicators=MA,MACD` 获取 K 线与指标数据
- `GET /api/signals?code=000001` 获取金叉/死叉与 RSI 预警
- `GET /api/report/markdown?code=000001` 导出 Markdown 报告
- `GET /api/report/html?code=000001` 导出 HTML 报告 (离线可交互)

## 设计要点

- 指标插件化注册，前端通过配置自动渲染勾选框。
- 使用 AkShare 获取 A 股复权数据，失败时自动回退到模拟数据。
- 报告模板采用 Jinja2 + Lightweight Charts，内嵌 JSON 数据离线渲染。
