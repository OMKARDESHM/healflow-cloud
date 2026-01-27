from __future__ import annotations
from typing import Dict, Any, List, Tuple
import yaml

from .validator import Issue


def auto_heal(
    yaml_text: str,
    issues: List[Issue],
) -> Tuple[str, List[str]]:
    """
    Simple remediation engine:
    - For too_many_nulls, increase max_null_fraction slightly.
    - For other issues, log suggestions but don't auto-change.

    Returns:
      (new_yaml_text, actions_applied)
    """
    config = yaml.safe_load(yaml_text) or {}
    dq = config.get("data_quality", {}) or {}
    actions: List[str] = []

    # handle too_many_nulls issues
    null_issues = [i for i in issues if i.type == "too_many_nulls"]
    if null_issues:
        # bump max_null_fraction by a small amount (up to 0.95)
        raw_max = dq.get("max_null_fraction", 0.2)
        try:
            current_max = float(raw_max)
        except Exception:
            current_max = 0.2
        new_max = min(current_max + 0.15, 0.95)
        dq["max_null_fraction"] = new_max
        config["data_quality"] = dq
        actions.append(
            f"Increased max_null_fraction from {current_max:.2f} to {new_max:.2f} "
            f"to tolerate observed null patterns."
        )

    # for other issues, we only record suggestions now
    for issue in issues:
        if issue.type in ("missing_column", "duplicate_key", "type_mismatch"):
            actions.append(
                f"Suggestion: Review issue '{issue.type}' on column "
                f"'{issue.column}' - {issue.details}"
            )

    new_yaml = yaml.safe_dump(config, sort_keys=False)
    return new_yaml, actions
