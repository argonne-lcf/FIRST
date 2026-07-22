from collections import defaultdict
from typing import Any, get_args

from first_common.schema.resources.runtime import (
    AlertGroup,
    Severity,
    StagedTransition,
)

_SEVERITY_ICON: dict[Severity, str] = {"crit": "🔴", "warn": "🟡", "info": "ℹ️"}
_SEVERITY_RANK: list[Severity] = ["crit", "warn", "info"]
_GROUP_ORDER: list[AlertGroup] = list(get_args(AlertGroup))


def _render_grouped(lines_by_group: dict[AlertGroup, list[str]]) -> str:
    """Render `{group: [line, ...]}` under group headers in canonical order."""
    out: list[str] = []

    for group in sorted(lines_by_group, key=_GROUP_ORDER.index):
        out.append(f"*{group}*")
        out.extend([f"  • {line}" for line in lines_by_group[group]])

    text = "\n".join(out)
    if len(text) > 2900:
        text = text[:2900] + "\n…(truncated)"
    return text


def build_alert_blocks(
    degradations: list[StagedTransition],
    recoveries: list[StagedTransition],
    failed_checks: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    if degradations:
        has_crit = any(s.severity == "crit" for s in degradations)
        header = "🚨 Health degradation" if has_crit else "⚠️ Health update"
    elif recoveries:
        header = "✅ Recovery"
    else:
        header = "⚠️ Health update"

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
    ]

    grouped: dict[AlertGroup, list[str]]

    grouped = defaultdict(list)
    for staged in sorted(degradations, key=lambda s: _SEVERITY_RANK.index(s.severity)):
        icon = _SEVERITY_ICON.get(staged.severity, "")
        grouped[staged.group].append(f"{icon} {staged.key} — {staged.summary}")
    if text := _render_grouped(grouped):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    grouped = defaultdict(list)
    for staged in recoveries:
        grouped[staged.group].append(f"✅ {staged.key} — recovered")
    if text := _render_grouped(grouped):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if failed_checks:
        lines = ["*Check execution failures:*"]
        lines.extend([f"  • {fn}: {msg[:200]}" for fn, msg in failed_checks])
        text = "\n".join(lines)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    return blocks


def build_digest_blocks(
    resource_counts: dict[str, tuple[int, int]],
    current_degradations: dict[str, str],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 Daily Health Digest"},
        },
    ]
    lines: list[str] = []
    for category, (total, issues) in resource_counts.items():
        if issues > 0:
            lines.append(f"*{category}*: {total} total, {issues} open issue(s)")
        else:
            lines.append(f"*{category}*: {total} healthy")

    if current_degradations:
        lines.append("")
        lines.append("*Current degradations:*")
        for key, status in sorted(current_degradations.items()):
            lines.append(f"  • {key}: {status}")

    text = "\n".join(lines) or "All systems healthy."
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
    return blocks
