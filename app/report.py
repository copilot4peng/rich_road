import json
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape


template_dir = Path(__file__).resolve().parent.parent / "templates"


env = Environment(
    loader=FileSystemLoader(template_dir),
    autoescape=select_autoescape(["html"]),
)


def render_html_report(payload: Dict[str, Any]) -> str:
    template = env.get_template("report_template.html")
    return template.render(payload_json=json.dumps(payload, ensure_ascii=False))


def render_markdown_report(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# {payload['code']} 分析报告")
    lines.append("")
    lines.append(f"- 周期: {payload['period']}")
    lines.append(f"- 生成时间: {payload['generated_at']}")
    lines.append("")
    lines.append("## 信号摘要")
    for signal in payload.get("signals", []):
        lines.append(f"- {signal}")
    lines.append("")
    lines.append("## 指标概览")
    for indicator in payload.get("indicators", []):
        lines.append(f"- {indicator['name']} ({indicator['type']})")
    return "\n".join(lines)
