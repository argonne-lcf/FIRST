from collections import defaultdict
from typing import Any, get_args

from first_common.schema.resources.runtime import (
    AlertGroup,
    Severity,
    StagedTransition,
)
from first_gateway.controllers.workers.health_alerter.types import DigestRow

_SEVERITY_ICON: dict[Severity, str] = {"crit": "🔴", "warn": "🟡", "info": "ℹ️"}
_SEVERITY_RANK: list[Severity] = ["crit", "warn", "info"]
_GROUP_ORDER: list[AlertGroup] = list(get_args(AlertGroup))
_THREAD_BODY_LIMIT = 2800


def _recovery_line(staged: StagedTransition) -> str:
    resource = staged.display_name or staged.key
    if staged.owner == "check_quarantined_nodes":
        return f"{resource}: Quarantine ended; node is eligible for startup screening"
    if staged.recovery_hint:
        return f"✅ {resource} recovered after {staged.recovery_hint}"
    return f"✅ {resource} recovered"


def _render_grouped(lines_by_group: dict[AlertGroup, list[str]]) -> str:
    """Render `{group: [line, ...]}` under group headers in canonical order."""
    out: list[str] = []

    for group in sorted(lines_by_group, key=_GROUP_ORDER.index):
        out.append(f"*{group}*")
        out.extend([f"  • {line}" for line in lines_by_group[group]])

    return _truncate("\n".join(out))


def _truncate(text: str) -> str:
    if len(text) > 2900:
        text = text[:2900] + "\n…(truncated)"
    return text


def _mrkdwn(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def build_alert_blocks(
    degradations: list[StagedTransition],
    recoveries: list[StagedTransition],
    failed_checks: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    if degradations:
        has_crit = any(s.severity == "crit" for s in degradations)
        header = "🚨 Health degradation" if has_crit else "⚠️ Health update"
    elif recoveries:
        header = (
            "ℹ️ Health update"
            if all(r.owner == "check_quarantined_nodes" for r in recoveries)
            else "✅ Recovery"
        )
    else:
        header = "⚠️ Health update"

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
    ]

    grouped: dict[AlertGroup, list[str]]

    grouped = defaultdict(list)
    for staged in sorted(degradations, key=lambda s: _SEVERITY_RANK.index(s.severity)):
        icon = _SEVERITY_ICON.get(staged.severity, "")
        grouped[staged.group].append(f"{icon} {staged.summary}")
    if text := _render_grouped(grouped):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    grouped = defaultdict(list)
    for staged in recoveries:
        grouped[staged.group].append(_recovery_line(staged))
    if text := _render_grouped(grouped):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if failed_checks:
        lines = ["*Check execution failures:*"]
        lines.extend([f"  • {fn}: {msg[:600]}" for fn, msg in failed_checks])
        text = "\n".join(lines)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    return blocks


def _digest_section(
    title: str, rows: list[DigestRow], child: str, issues: int
) -> dict[str, Any]:
    """`*Title* (N total, ...)` then one `• name: status (Child: counts)` line each."""
    health = f"{issues} open issue(s)" if issues else "all healthy"
    lines = [f"*{title}* ({len(rows)} total, {health})"]
    for row in sorted(rows, key=lambda r: r.name):
        line = f"• {row.name}: {row.status}"
        if row.counts is not None:
            counts = ", ".join(f"{n} {s}" for s, n in sorted(row.counts.items()))
            line += f" ({child}: {counts or 'none'})"
        lines.append(line)
    return _mrkdwn(_truncate("\n".join(lines)))


def _deployment_sections(rows: list[DigestRow]) -> list[dict[str, Any]]:
    """One `*N State Deployments*` section per state (healthy first, offline
    last), each listing `• name (detail)`; empty states are skipped."""
    by_state: dict[str, list[DigestRow]] = defaultdict(list)
    for row in rows:
        by_state[row.status].append(row)

    def order(state: str) -> tuple[int, str]:
        return ({"healthy": 0, "offline": 2}.get(state, 1), state)

    sections = []
    for state in sorted(by_state, key=order):
        group = by_state[state]
        noun = "Deployment" if len(group) == 1 else "Deployments"
        label = state.replace("_", " ").title()
        lines = [f"*{len(group)} {label} {noun}*"]
        for row in sorted(group, key=lambda r: r.name):
            if row.counts is None:
                lines.append(f"• {row.name} (StaticDeployment)")
            elif state == "offline":
                lines.append(f"• {row.name}")
            else:
                counts = ", ".join(f"{n} {s}" for s, n in sorted(row.counts.items()))
                lines.append(f"• {row.name} (Replicas: {counts or 'none'})")
        sections.append(_mrkdwn(_truncate("\n".join(lines))))
    return sections


def build_digest_blocks(
    clusters: list[DigestRow],
    deployments: list[DigestRow],
    issues_by_group: dict[str, int],
    current_degradations: dict[str, str],
) -> list[dict[str, Any]]:
    # Pilot Job issues roll up into their parent cluster's open-issue count.
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 Daily Health Digest"},
        },
        _digest_section(
            "Clusters",
            clusters,
            "Pilot Jobs",
            issues_by_group.get("Clusters", 0) + issues_by_group.get("Pilot Jobs", 0),
        ),
        *_deployment_sections(deployments),
    ]
    if current_degradations:
        lines = ["*Current degradations*"]
        lines.extend(f"• {line}" for line in sorted(current_degradations.values()))
        blocks.append(_mrkdwn(_truncate("\n".join(lines))))
    return blocks


def _fenced(label: str, body: str) -> str:
    body = body.strip()
    if len(body) > _THREAD_BODY_LIMIT:
        # Keep the tail: a traceback's innermost frames and exception come last.
        body = "…(truncated)\n" + body[-_THREAD_BODY_LIMIT:]
    return f"*{label}*\n```\n{body}\n```"


def build_thread_blocks(details: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Render `(label, full-text)` pairs as code-fenced sections for a thread.

    Posted as a reply under a parent alert so the channel stays terse while the
    full context (tracebacks, multi-line errors) lives one click away.
    """
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": _fenced(label, body)}}
        for label, body in details
        if body.strip()
    ]
