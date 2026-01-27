from typing import Optional, List
from sqlalchemy.orm import Session

from .. import models
from .validator import Issue


def log_incident(
    db: Session,
    pipeline: models.Pipeline,
    run: Optional[models.PipelineRun],
    stage: str,
    status: str,
    message: str,
    details: Optional[str] = None,
):
    incident = models.Incident(
        pipeline_id=pipeline.id,
        run_id=run.id if run else None,
        stage=stage,
        status=status,
        message=message,
        details=details,
    )
    db.add(incident)
    db.commit()


def issues_to_text(issues: List[Issue]) -> str:
    lines = []
    for i in issues:
        lines.append(f"[{i.severity.upper()}] {i.type} ({i.column}): {i.details}")
    return "\n".join(lines)
