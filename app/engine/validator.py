from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml


class ConfigError(Exception):
    """Raised when the YAML / config is invalid or incompatible."""
    pass


@dataclass
class Issue:
    type: str
    severity: str
    column: Optional[str]
    details: str


class DataQualityError(Exception):
    """Raised when data quality checks fail."""

    def __init__(self, issues: List[Issue]):
        self.issues = issues
        super().__init__("; ".join(i.details for i in issues))


def load_config_yaml(yaml_text: str) -> Dict[str, Any]:
    try:
        cfg = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML: {e}")
    if not isinstance(cfg, dict):
        raise ConfigError("Root of YAML config must be a mapping.")
    return cfg


def build_dq_config(config: Dict[str, Any]) -> Dict[str, Any]:
    dq = config.get("data_quality", {}) or {}

    # min_row_count
    raw_min_rows = dq.get("min_row_count", 0)
    try:
        min_row_count = int(raw_min_rows)
    except (TypeError, ValueError):
        raise ConfigError(
            f"data_quality.min_row_count must be an integer, got: {raw_min_rows!r}"
        )

    # max_null_fraction: scalar OR mapping (we collapse mapping to strictest)
    raw_max_null = dq.get("max_null_fraction", 1.0)

    if isinstance(raw_max_null, dict):
        try:
            values = [float(v) for v in raw_max_null.values()]
        except (TypeError, ValueError):
            raise ConfigError(
                "data_quality.max_null_fraction mapping must contain only numbers, "
                f"got: {raw_max_null!r}"
            )
        max_null_fraction = min(values) if values else 1.0
    else:
        try:
            max_null_fraction = float(raw_max_null)
        except (TypeError, ValueError):
            raise ConfigError(
                "data_quality.max_null_fraction must be a number or mapping of column→number, "
                f"got: {raw_max_null!r}"
            )

    column_types = dq.get("column_types", {}) or {}
    unique_keys = dq.get("unique_keys", []) or []
    allowed_values = dq.get("allowed_values", {}) or {}

    freshness_cfg = dq.get("freshness", {}) or {}
    freshness = {
        "date_column": freshness_cfg.get("date_column"),
        "max_days_delay": freshness_cfg.get("max_days_delay", None),
    }

    return {
        "min_row_count": min_row_count,
        "max_null_fraction": max_null_fraction,
        "column_types": column_types,
        "unique_keys": unique_keys,
        "allowed_values": allowed_values,
        "freshness": freshness,
    }


def ensure_required_columns(df: pd.DataFrame, config: Dict[str, Any]) -> List[Issue]:
    schema = config.get("schema", {}) or {}
    required = schema.get("required_columns", []) or []
    issues: List[Issue] = []
    for col in required:
        if col not in df.columns:
            issues.append(
                Issue(
                    type="missing_column",
                    severity="high",
                    column=col,
                    details=f"Required column '{col}' is missing from data.",
                )
            )
    return issues


def check_min_row_count(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    min_rows = dq_cfg["min_row_count"]
    if len(df) < min_rows:
        issues.append(
            Issue(
                type="too_few_rows",
                severity="high",
                column=None,
                details=f"Row count {len(df)} < min_row_count {min_rows}.",
            )
        )
    return issues


def check_nulls(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    max_null = dq_cfg["max_null_fraction"]
    for col in df.columns:
        frac_null = df[col].isna().mean()
        if frac_null > max_null:
            issues.append(
                Issue(
                    type="too_many_nulls",
                    severity="medium",
                    column=col,
                    details=f"Null fraction in '{col}' {frac_null:.3f} > max_null_fraction {max_null}.",
                )
            )
    return issues


def check_unique_keys(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    unique_keys = dq_cfg["unique_keys"] or []
    for key_col in unique_keys:
        if key_col in df.columns:
            dup_count = df[key_col].duplicated().sum()
            if dup_count > 0:
                issues.append(
                    Issue(
                        type="duplicate_key",
                        severity="high",
                        column=key_col,
                        details=f"Uniqueness violation in '{key_col}': {dup_count} duplicates.",
                    )
                )
    return issues


def check_allowed_values(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    allowed_values = dq_cfg["allowed_values"] or {}
    for col, allowed in allowed_values.items():
        if col not in df.columns:
            continue
        non_null = df[col].dropna()
        invalid = non_null[~non_null.isin(allowed)]
        if not invalid.empty:
            examples = invalid.unique()[:5]
            issues.append(
                Issue(
                    type="invalid_value",
                    severity="medium",
                    column=col,
                    details=f"Column '{col}' contains values outside allowed set: {list(examples)}",
                )
            )
    return issues


def check_column_types(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    types_map = dq_cfg["column_types"] or {}

    for col, expected_type in types_map.items():
        if col not in df.columns:
            continue
        series = df[col]
        if expected_type == "int":
            # allow NaNs but non-integers are an issue
            bad = series.dropna().apply(lambda x: not float(x).is_integer())
            if bad.any():
                issues.append(
                    Issue(
                        type="type_mismatch",
                        severity="medium",
                        column=col,
                        details=f"Type check failed for '{col}' (expected int).",
                    )
                )
        elif expected_type == "float":
            try:
                series.astype(float)
            except Exception:
                issues.append(
                    Issue(
                        type="type_mismatch",
                        severity="medium",
                        column=col,
                        details=f"Type check failed for '{col}' (expected float).",
                    )
                )
        elif expected_type == "datetime":
            try:
                pd.to_datetime(series, errors="raise")
            except Exception:
                issues.append(
                    Issue(
                        type="type_mismatch",
                        severity="medium",
                        column=col,
                        details=f"Type check failed for '{col}' (expected datetime).",
                    )
                )
        # You can add more types (string, bool, etc.)

    return issues


def check_freshness(df: pd.DataFrame, dq_cfg: Dict[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    freshness = dq_cfg["freshness"]
    date_col = freshness.get("date_column")
    max_delay = freshness.get("max_days_delay")
    if not date_col or max_delay is None:
        return issues
    if date_col not in df.columns:
        issues.append(
            Issue(
                type="freshness_missing_column",
                severity="low",
                column=date_col,
                details=f"Freshness date column '{date_col}' not present in data.",
            )
        )
        return issues

    try:
        dates = pd.to_datetime(df[date_col], errors="coerce")
    except Exception:
        issues.append(
            Issue(
                type="freshness_parse_error",
                severity="medium",
                column=date_col,
                details=f"Could not parse '{date_col}' as datetime for freshness check.",
            )
        )
        return issues

    max_date = dates.max()
    if pd.isna(max_date):
        issues.append(
            Issue(
                type="freshness_all_null",
                severity="medium",
                column=date_col,
                details=f"All values in '{date_col}' are null or invalid; cannot check freshness.",
            )
        )
        return issues

    now = datetime.utcnow()
    age_days = (now - max_date.to_pydatetime()).days
    if age_days > max_delay:
        issues.append(
            Issue(
                type="stale_data",
                severity="medium",
                column=date_col,
                details=f"Latest '{date_col}' is {age_days} days old (> max_days_delay {max_delay}).",
            )
        )
    return issues


def run_validation(
    df: pd.DataFrame,
    yaml_text: str,
    pipeline_name: str = "pipeline",
) -> Dict[str, Any]:
    """
    Core engine: validates data against YAML config.
    Returns summary dict. Raises DataQualityError if issues found.
    """
    config = load_config_yaml(yaml_text)
    dq_cfg = build_dq_config(config)

    issues: List[Issue] = []

    issues += ensure_required_columns(df, config)
    issues += check_min_row_count(df, dq_cfg)
    issues += check_nulls(df, dq_cfg)
    issues += check_unique_keys(df, dq_cfg)
    issues += check_allowed_values(df, dq_cfg)
    issues += check_column_types(df, dq_cfg)
    issues += check_freshness(df, dq_cfg)

    if issues:
        raise DataQualityError(issues)

    return {
        "pipeline_name": pipeline_name,
        "rows": len(df),
        "message": "Data quality checks passed.",
    }
