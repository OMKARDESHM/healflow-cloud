from typing import Dict, List

from .causal_log import log_causal_event
from .validator import DataQualityError


def diagnose_schema_drift(missing_columns: List[str], config: Dict) -> Dict:
    pipeline_name = config.get("pipeline_name", "unknown")

    diagnosis = {
        "pipeline_name": pipeline_name,
        "root_cause": "schema_drift",
        "missing_columns": missing_columns,
        "suggested_actions": [],
    }

    if "date_of_sale" in missing_columns:
        diagnosis["suggested_actions"].append(
            {
                "type": "schema_update",
                "description": "Rename required column 'date_of_sale' to 'txn_date' in YAML schema.",
                "from": "date_of_sale",
                "to": "txn_date",
            }
        )
    else:
        diagnosis["suggested_actions"].append(
            {
                "type": "no_safe_fix",
                "description": "No known automatic remediation. Escalate to engineer.",
            }
        )

    log_causal_event("diagnosis", diagnosis)
    return diagnosis


def apply_schema_healing(config: Dict, diagnosis: Dict) -> Dict:
    actions = diagnosis.get("suggested_actions", [])
    if not actions:
        return config

    action = actions[0]
    if action.get("type") != "schema_update":
        return config

    from_col = action["from"]
    to_col = action["to"]

    old_cols = config.get("schema", {}).get("required_columns", [])
    new_cols = [to_col if c == from_col else c for c in old_cols]
    config.setdefault("schema", {})["required_columns"] = new_cols

    log_causal_event(
        "healing_applied",
        {
            "action_type": "schema_update",
            "from": from_col,
            "to": to_col,
            "old_required_columns": old_cols,
            "new_required_columns": new_cols,
        },
    )

    return config


def diagnose_dq_issue(err: DataQualityError, config: Dict) -> Dict:
    pipeline_name = config.get("pipeline_name", "unknown")

    diag = {
        "pipeline_name": pipeline_name,
        "root_cause": err.kind or "dq_failure",
        "column": err.column,
        "observed": err.observed,
        "threshold": err.threshold,
        "suggested_actions": [],
    }

    if err.kind == "null_fraction" and err.column and err.observed is not None:
        new_threshold = min(max(err.observed + 0.05, err.threshold or 0.0), 1.0)
        diag["suggested_actions"].append(
            {
                "type": "dq_update_max_null",
                "description": (
                    f"Increase max_null_fraction for '{err.column}' "
                    f"to {new_threshold:.3f} based on observed data."
                ),
                "column": err.column,
                "new_threshold": new_threshold,
            }
        )
    else:
        diag["suggested_actions"].append(
            {
                "type": "no_safe_fix",
                "description": "No automatic remediation defined. Please inspect data and rules.",
            }
        )

    log_causal_event("dq_diagnosis", diag)
    return diag


def apply_dq_healing(config: Dict, diagnosis: Dict) -> Dict:
    actions = diagnosis.get("suggested_actions", [])
    if not actions:
        return config
    action = actions[0]

    if action.get("type") == "dq_update_max_null":
        new_thr = action["new_threshold"]
        config.setdefault("data_quality", {})["max_null_fraction"] = float(new_thr)
        log_causal_event(
            "healing_applied",
            {
                "action_type": "dq_update_max_null",
                "column": action.get("column"),
                "new_threshold": new_thr,
            },
        )
        return config

    return config
