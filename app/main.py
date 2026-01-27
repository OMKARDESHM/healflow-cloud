import os
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import (
    FastAPI,
    Request,
    Depends,
    Form,
    UploadFile,
    File,
    status,
)
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from .db import Base, engine, get_db
from . import models, auth
from .engine import run_validation, DataQualityError, ConfigError, auto_heal, log_incident
from .engine.causal_log import issues_to_text

# --- DB setup ---
Base.metadata.create_all(bind=engine)

# --- App setup ---
app = FastAPI(title="HealFlow Cloud V3")

SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-prod")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Static files (custom CSS/JS)
static_dir = os.path.join(BASE_DIR, "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


# --- helpers ---

def get_current_user(request: Request, db: Session) -> Optional[models.User]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return db.get(models.User, user_id)


def require_user(request: Request, db: Session) -> models.User:
    user = get_current_user(request, db)
    if not user:
        raise RedirectResponse("/login", status_code=status.HTTP_302_FOUND)
    return user


# --- routes ---

@app.get("/", response_class=HTMLResponse)
async def landing(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user:
        return RedirectResponse("/dashboard", status_code=status.HTTP_302_FOUND)
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = auth.get_user_by_username(db, username)
    if not user:
        # auto-create first user for simplicity
        user = auth.create_user(db, username, password)
    else:
        if not auth.verify_password(password, user.password_hash):
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "error": "Invalid credentials"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

    request.session["user_id"] = user.id
    return RedirectResponse("/dashboard", status_code=status.HTTP_302_FOUND)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=status.HTTP_302_FOUND)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    # stats
    pipelines = (
        db.query(models.Pipeline)
        .filter(models.Pipeline.owner_id == user.id)
        .order_by(models.Pipeline.created_at.desc())
        .all()
    )

    total_pipelines = len(pipelines)
    total_runs = (
        db.query(models.PipelineRun)
        .join(models.Pipeline)
        .filter(models.Pipeline.owner_id == user.id)
        .count()
    )
    failed_runs = (
        db.query(models.PipelineRun)
        .join(models.Pipeline)
        .filter(
            models.Pipeline.owner_id == user.id,
            models.PipelineRun.status == "failed",
        )
        .count()
    )
    healed_runs = (
        db.query(models.PipelineRun)
        .join(models.Pipeline)
        .filter(
            models.Pipeline.owner_id == user.id,
            models.PipelineRun.status == "healed",
        )
        .count()
    )

    recent_incidents = (
        db.query(models.Incident)
        .join(models.Pipeline)
        .filter(models.Pipeline.owner_id == user.id)
        .order_by(models.Incident.created_at.desc())
        .limit(5)
        .all()
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "pipelines": pipelines,
            "total_pipelines": total_pipelines,
            "total_runs": total_runs,
            "failed_runs": failed_runs,
            "healed_runs": healed_runs,
            "recent_incidents": recent_incidents,
        },
    )


@app.get("/pipelines/new", response_class=HTMLResponse)
async def new_pipeline(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    return templates.TemplateResponse(
        "pipeline_detail.html",
        {
            "request": request,
            "user": user,
            "pipeline": None,
            "config_yaml": "",
            "message": None,
        },
    )


@app.post("/pipelines/new", response_class=HTMLResponse)
async def create_pipeline(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    sample_file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    # read sample data
    content = await sample_file.read()
    df = pd.read_csv(pd.compat.StringIO(content.decode("utf-8")))

    # simple auto-schema & dq suggestion
    cols = list(df.columns)
    yaml_suggestion = f"""pipeline_name: "{name}"
file_type: "csv"
delimiter: ","

schema:
  required_columns:
{''.join(f'    - {c}\n' for c in cols)}

data_quality:
  min_row_count: 10
  max_null_fraction: 0.2
  column_types:
"""

    for c in cols:
        dt = df[c].dtype
        if "int" in str(dt):
            t = "int"
        elif "float" in str(dt):
            t = "float"
        elif "datetime" in str(dt):
            t = "datetime"
        else:
            t = "string"
        yaml_suggestion += f"    {c}: {t}\n"

    # create pipeline
    pipeline = models.Pipeline(
        owner_id=user.id,
        name=name,
        description=description,
        is_active=True,
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)

    cfg = models.ConfigVersion(
        pipeline_id=pipeline.id,
        version=1,
        yaml_text=yaml_suggestion,
        comment="Initial auto-generated config",
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)

    return RedirectResponse(
        f"/pipelines/{pipeline.id}",
        status_code=status.HTTP_302_FOUND,
    )


@app.get("/pipelines/{pipeline_id}", response_class=HTMLResponse)
async def pipeline_detail(
    pipeline_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    pipeline = db.get(models.Pipeline, pipeline_id)
    if not pipeline or pipeline.owner_id != user.id:
        return RedirectResponse("/dashboard", status_code=status.HTTP_302_FOUND)

    # latest config
    config = (
        db.query(models.ConfigVersion)
        .filter(models.ConfigVersion.pipeline_id == pipeline.id)
        .order_by(models.ConfigVersion.version.desc())
        .first()
    )

    runs = (
        db.query(models.PipelineRun)
        .filter(models.PipelineRun.pipeline_id == pipeline.id)
        .order_by(models.PipelineRun.run_at.desc())
        .limit(10)
        .all()
    )

    incidents = (
        db.query(models.Incident)
        .filter(models.Incident.pipeline_id == pipeline.id)
        .order_by(models.Incident.created_at.desc())
        .limit(10)
        .all()
    )

    return templates.TemplateResponse(
        "pipeline_detail.html",
        {
            "request": request,
            "user": user,
            "pipeline": pipeline,
            "config_yaml": config.yaml_text if config else "",
            "runs": runs,
            "incidents": incidents,
            "message": None,
        },
    )


@app.post("/pipelines/{pipeline_id}/update-config", response_class=HTMLResponse)
async def update_config(
    pipeline_id: int,
    request: Request,
    config_yaml: str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    pipeline = db.get(models.Pipeline, pipeline_id)
    if not pipeline or pipeline.owner_id != user.id:
        return RedirectResponse("/dashboard", status_code=status.HTTP_302_FOUND)

    # validate YAML quickly
    try:
        _ = pd.Series([1])  # just to avoid unused import warning
        _ = run_validation(pd.DataFrame({"dummy": [1]}), config_yaml, "validation_dummy")
    except DataQualityError:
        # we don't care, just checking YAML structure
        pass
    except ConfigError as e:
        # show error to user
        runs = []
        incidents = []
        return templates.TemplateResponse(
            "pipeline_detail.html",
            {
                "request": request,
                "user": user,
                "pipeline": pipeline,
                "config_yaml": config_yaml,
                "runs": runs,
                "incidents": incidents,
                "message": f"Invalid config: {e}",
            },
        )
    except Exception as e:
        runs = []
        incidents = []
        return templates.TemplateResponse(
            "pipeline_detail.html",
            {
                "request": request,
                "user": user,
                "pipeline": pipeline,
                "config_yaml": config_yaml,
                "runs": runs,
                "incidents": incidents,
                "message": f"Error while validating config: {e}",
            },
        )

    latest = (
        db.query(models.ConfigVersion)
        .filter(models.ConfigVersion.pipeline_id == pipeline.id)
        .order_by(models.ConfigVersion.version.desc())
        .first()
    )
    new_version = (latest.version + 1) if latest else 1
    cfg = models.ConfigVersion(
        pipeline_id=pipeline.id,
        version=new_version,
        yaml_text=config_yaml,
        comment="Manual update via UI",
    )
    db.add(cfg)
    db.commit()

    return RedirectResponse(
        f"/pipelines/{pipeline.id}",
        status_code=status.HTTP_302_FOUND,
    )


@app.post("/pipelines/{pipeline_id}/run", response_class=HTMLResponse)
async def run_pipeline(
    pipeline_id: int,
    request: Request,
    data_file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    pipeline = db.get(models.Pipeline, pipeline_id)
    if not pipeline or pipeline.owner_id != user.id:
        return RedirectResponse("/dashboard", status_code=status.HTTP_302_FOUND)

    config = (
        db.query(models.ConfigVersion)
        .filter(models.ConfigVersion.pipeline_id == pipeline.id)
        .order_by(models.ConfigVersion.version.desc())
        .first()
    )
    if not config:
        return templates.TemplateResponse(
            "pipeline_detail.html",
            {
                "request": request,
                "user": user,
                "pipeline": pipeline,
                "config_yaml": "",
                "runs": [],
                "incidents": [],
                "message": "No config found. Please define a YAML config first.",
            },
        )

    content = await data_file.read()
    df = pd.read_csv(pd.compat.StringIO(content.decode("utf-8")))

    run = models.PipelineRun(
        pipeline_id=pipeline.id,
        run_at=datetime.utcnow(),
        status="running",
        rows=len(df),
        message="Running...",
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    message = ""
    healing_actions_text = None

    try:
        summary = run_validation(df, config.yaml_text, pipeline.name)
        run.status = "success"
        run.message = summary["message"]
        pipeline.last_run_status = "success"
        pipeline.last_run_at = run.run_at
        db.commit()

        log_incident(
            db,
            pipeline,
            run,
            stage="data_quality",
            status="success",
            message="Data quality checks passed.",
            details=None,
        )
        message = "Run completed successfully. No issues detected."

    except DataQualityError as dq_err:
        # Log the failure
        issues = dq_err.issues
        issues_text = issues_to_text(issues)

        log_incident(
            db,
            pipeline,
            run,
            stage="data_quality",
            status="failed",
            message="Data quality checks failed.",
            details=issues_text,
        )

        # auto-heal config
        new_yaml, actions = auto_heal(config.yaml_text, issues)

        if actions:
            # save new config version
            latest = (
                db.query(models.ConfigVersion)
                .filter(models.ConfigVersion.pipeline_id == pipeline.id)
                .order_by(models.ConfigVersion.version.desc())
                .first()
            )
            new_version = (latest.version + 1) if latest else 1
            healed_cfg = models.ConfigVersion(
                pipeline_id=pipeline.id,
                version=new_version,
                yaml_text=new_yaml,
                comment="Auto-healed by HealFlow based on last run.",
            )
            db.add(healed_cfg)
            db.commit()

            healing_actions_text = "\n".join(actions)
            run.status = "healed"
            run.message = "Data quality failed, but HealFlow applied auto-healing."
            run.healing_actions = healing_actions_text
            pipeline.last_run_status = "healed"
            pipeline.last_run_at = run.run_at
            db.commit()

            log_incident(
                db,
                pipeline,
                run,
                stage="healing",
                status="healed",
                message="HealFlow auto-healed the pipeline configuration.",
                details=healing_actions_text,
            )
            message = "Data quality failed, but HealFlow auto-healed the configuration."
        else:
            run.status = "failed"
            run.message = "Data quality failed; no auto-healing applied."
            pipeline.last_run_status = "failed"
            pipeline.last_run_at = run.run_at
            db.commit()
            message = "Data quality failed; no safe auto-healing action was available."

    except ConfigError as cfg_err:
        run.status = "failed"
        run.message = f"Invalid configuration: {cfg_err}"
        pipeline.last_run_status = "failed"
        pipeline.last_run_at = run.run_at
        db.commit()
        log_incident(
            db,
            pipeline,
            run,
            stage="config",
            status="failed",
            message="Invalid pipeline configuration.",
            details=str(cfg_err),
        )
        message = f"Configuration error: {cfg_err}"

    except Exception as e:
        run.status = "failed"
        run.message = f"Unexpected error: {e}"
        pipeline.last_run_status = "failed"
        pipeline.last_run_at = run.run_at
        db.commit()
        log_incident(
            db,
            pipeline,
            run,
            stage="engine",
            status="failed",
            message="Unexpected engine error.",
            details=str(e),
        )
        message = f"Unexpected error: {e}"

    runs = (
        db.query(models.PipelineRun)
        .filter(models.PipelineRun.pipeline_id == pipeline.id)
        .order_by(models.PipelineRun.run_at.desc())
        .limit(10)
        .all()
    )
    incidents = (
        db.query(models.Incident)
        .filter(models.Incident.pipeline_id == pipeline.id)
        .order_by(models.Incident.created_at.desc())
        .limit(10)
        .all()
    )

    # latest config after potential healing
    latest_cfg = (
        db.query(models.ConfigVersion)
        .filter(models.ConfigVersion.pipeline_id == pipeline.id)
        .order_by(models.ConfigVersion.version.desc())
        .first()
    )

    return templates.TemplateResponse(
        "pipeline_detail.html",
        {
            "request": request,
            "user": user,
            "pipeline": pipeline,
            "config_yaml": latest_cfg.yaml_text if latest_cfg else config.yaml_text,
            "runs": runs,
            "incidents": incidents,
            "message": message,
        },
    )


@app.get("/incidents", response_class=HTMLResponse)
async def incidents_view(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse("/login", status_code=status.HTTP_302_FOUND)

    incidents = (
        db.query(models.Incident)
        .join(models.Pipeline)
        .filter(models.Pipeline.owner_id == user.id)
        .order_by(models.Incident.created_at.desc())
        .limit(100)
        .all()
    )

    return templates.TemplateResponse(
        "incidents.html",
        {"request": request, "user": user, "incidents": incidents},
    )
