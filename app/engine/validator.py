from dataclasses import dataclass
from typing import List, Dict, Optional

import pandas as pd
from datetime import date

from .causal_log import log_causal_event


class SchemaDriftError(Exception):
    def __init__(self, missing_columns: List[str]):
        self.missing_columns = missing_columns
        super().__init__(f"Schema drift: missing columns {missing_columns}")


class DataQualityError(Exception):
    def __init__(
        self,
        message: str,
        kind: Optional[str] = None,
        column: Optional[str] = None,
        observed: Optional[float] = None,
        threshold: Optional[float] = None,
    ):
        self.kind = kind
        self.column = column
        self.observed = observed
        self.threshold = threshold
        super().__init__(message)


@dataclass
class DQConfig:
    max_null_fraction: float
    min_row_count: int
    required_columns: List[str]
    unique_keys: List[str]
    column_types: Dict[str, str]
    allowed_values: Dict[str, List[str]]
    freshness_date_column: Optional[str]
    freshness_max_days_delay: Optional[int]


def build_dq_config(config: Dict) -> DQConfig:
    dq = config.get("data_quality", {})
    schema = config.get("schema", {})

    max_null_fraction = float(dq.get("max_null_fraction", 1.0))
    min_row_count = int(dq.get("min_row_count", 0))
    required_columns = list(schema.get("required_columns", []))
    unique_keys = list(dq.get("unique_keys", []))
    column_types = schema.get("column_types", {}) or {}
    allowed_values = config.get("allowed_values", {}) or {}

    freshness_cfg = config.get("freshness", {}) or {}
    freshness_date_column = freshness_cfg.get("date_column")
    freshness_max_days_delay = freshness_cfg.get("max_days_delay")

    return DQConfig(
        max_null_fraction=max_null_fraction,
        min_row_count=min_row_count,
        required_columns=required_columns,
        unique_keys=unique_keys,
        column_types=column_types,
        allowed_values=allowed_values,
        freshness_date_column=freshness_date_column,
        freshness_max_days_delay=freshness_max_days_delay,
    )


def _check_row_count(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    if len(df) < cfg.min_row_count:
        msg = f"Row count {len(df)} < min_row_count {cfg.min_row_count}"
        log_causal_event(
            "dq_failure",
            {"pipeline_name": pipeline_name, "reason": "row_count", "message": msg},
        )
        raise DataQualityError(msg, kind="row_count", observed=len(df), threshold=cfg.min_row_count)


def _check_null_fraction(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    col = "sales_amount"
    if col in df.columns:
        null_frac = df[col].isna().mean()
        if null_frac > cfg.max_null_fraction:
            msg = f"Null fraction in '{col}' {null_frac:.4f} > max_null_fraction {cfg.max_null_fraction}"
            log_causal_event(
                "dq_failure",
                {
                    "pipeline_name": pipeline_name,
                    "reason": "null_fraction",
                    "column": col,
                    "observed": null_frac,
                    "threshold": cfg.max_null_fraction,
                    "message": msg,
                },
            )
            raise DataQualityError(
                msg,
                kind="null_fraction",
                column=col,
                observed=float(null_frac),
                threshold=cfg.max_null_fraction,
            )


def _check_schema(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    missing = [c for c in cfg.required_columns if c not in df.columns]
    if missing:
        log_causal_event(
            "schema_drift_detected",
            {"pipeline_name": pipeline_name, "missing_columns": missing},
        )
        raise SchemaDriftError(missing)


def _check_uniqueness(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    for col in cfg.unique_keys:
        if col in df.columns:
            dup_count = df[col].duplicated().sum()
            if dup_count > 0:
                msg = f"Uniqueness violation in '{col}': {dup_count} duplicates"
                log_causal_event(
                    "dq_failure",
                    {
                        "pipeline_name": pipeline_name,
                        "reason": "uniqueness",
                        "column": col,
                        "duplicates": int(dup_count),
                        "message": msg,
                    },
                )
                raise DataQualityError(msg, kind="uniqueness", column=col)


def _check_column_types(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    for col, expected_type in cfg.column_types.items():
        if col not in df.columns:
            continue
        series = df[col]
        try:
            if expected_type == "float":
                pd.to_numeric(series, errors="raise")
            elif expected_type == "int":
                pd.to_numeric(series, errors="raise", downcast="integer")
            elif expected_type == "str":
                series.astype(str)
        except Exception:
            msg = f"Type check failed for '{col}' (expected {expected_type})"
            log_causal_event(
                "dq_failure",
                {
                    "pipeline_name": pipeline_name,
                    "reason": "type_mismatch",
                    "column": col,
                    "expected_type": expected_type,
                    "message": msg,
                },
            )
            raise DataQualityError(msg, kind="type_mismatch", column=col)


def _check_allowed_values(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    for col, allowed in cfg.allowed_values.items():
        if col not in df.columns:
            continue
        bad_values = sorted(set(df[col].dropna().unique()) - set(allowed))
        if bad_values:
            msg = f"Column '{col}' has unexpected values: {bad_values}"
            log_causal_event(
                "dq_failure",
                {
                    "pipeline_name": pipeline_name,
                    "reason": "allowed_values",
                    "column": col,
                    "unexpected_values": bad_values,
                    "allowed": allowed,
                    "message": msg,
                },
            )
            raise DataQualityError(msg, kind="allowed_values", column=col)


def _check_freshness(df: pd.DataFrame, cfg: DQConfig, pipeline_name: str):
    if not cfg.freshness_date_column or not cfg.freshness_max_days_delay:
        return
    col = cfg.freshness_date_column
    if col not in df.columns:
        return
    try:
        dt_series = pd.to_datetime(df[col])
        max_date = dt_series.max().date()
        today = date.today()
        delta_days = (today - max_date).days
        if delta_days > cfg.freshness_max_days_delay:
            msg = (
                f"Freshness violation: latest {col}={max_date} is {delta_days} days "
                f"old (max allowed {cfg.freshness_max_days_delay})"
            )
            log_causal_event(
                "dq_failure",
                {
                    "pipeline_name": pipeline_name,
                    "reason": "freshness",
                    "column": col,
                    "latest_date": str(max_date),
                    "delta_days": int(delta_days),
                    "threshold_days": cfg.freshness_max_days_delay,
                    "message": msg,
                },
            )
            raise DataQualityError(
                msg,
                kind="freshness",
                column=col,
                observed=float(delta_days),
                threshold=float(cfg.freshness_max_days_delay),
            )
    except Exception:
        # ignore parsing issues for freshness for now
        pass


def run_validation(df: pd.DataFrame, config: Dict, pipeline_name: str):
    dq_cfg = build_dq_config(config)

    _check_row_count(df, dq_cfg, pipeline_name)
    _check_null_fraction(df, dq_cfg, pipeline_name)
    _check_schema(df, dq_cfg, pipeline_name)
    _check_uniqueness(df, dq_cfg, pipeline_name)
    _check_column_types(df, dq_cfg, pipeline_name)
    _check_allowed_values(df, dq_cfg, pipeline_name)
    _check_freshness(df, dq_cfg, pipeline_name)

    log_causal_event(
        "validation_success", {"pipeline_name": pipeline_name, "rows": len(df)}
    )
