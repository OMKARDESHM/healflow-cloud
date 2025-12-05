from pathlib import Path
import uuid
import io

import pandas as pd
import yaml
from fastapi import FastAPI, Depends, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .db import Base, engine, get_db
from . import models
from .auth import authenticate_user, create_user, get_user_by_email
from .engine.validator import run_validation, SchemaDriftError, DataQualityError
from .engine.healer import (
    diagnose_schema_drift,
    apply_schema_healing,
    diagnose_dq_issue,
    apply_dq_healing,
)

app = FastAPI()
Base.metadata.create_all(bind=engine)

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# mount static files
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# simple in-memory session store – fine for MVP
SESSIONS = {}  # session_id -> user_email


DEFAULT_CONFIG_YAML = """pipeline_name: "daily_sales_processing"

data_quality:
  max_null_fraction: 0.05
  min_row_count: 5
  unique_keys:
    - transaction_id

schema:
  required_columns:
    - transaction_id
    - customer_id
    - sales_amount
    - date_of_sale
  column_types:
    sales_amount: float

allowed_values:
  region: ["APAC", "EMEA", "US"]

freshness:
  date_column: date_of_sale
  max_days_delay: 3
"""


def get_current_user_email(request: Request):
    sid = request.cookies.get("session_id")
    return SESSIONS.get(sid)


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    user_email = get_current_user_email(request)
    if not user_email:
        return templates.TemplateResponse("login.html", {"request": request, "error": None})
    return RedirectResponse(url="/dashboard")


@app.post("/login", response_class=HTMLResponse)
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = authenticate_user(db, username, password)
    if not user:
        existing = get_user_by_email(db, username)
        if existing:
            return templates.TemplateResponse(
                "login.html",
                {"request": request, "error": "Invalid credentials"},
            )
        user = create_user(db, username, password)

    if not user.metrics:
        metrics = models.Metrics(user_id=user.id, attempts=0, successes=0)
        db.add(metrics)
        db.commit()
        db.refresh(metrics)

    if not user.configs:
        cfg = models.Config(user_id=user.id, yaml_text=DEFAULT_CONFIG_YAML)
        db.add(cfg)
        db.commit()

    sid = str(uuid.uuid4())
    SESSIONS[sid] = user.email
    resp = RedirectResponse(url="/dashboard", status_code=302)
    resp.set_cookie("session_id", sid, httponly=True)
    return resp


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    email = get_current_user_email(request)
    if not email:
        return RedirectResponse(url="/")

    user = get_user_by_email(db, email)
    cfg = user.configs[0] if user.configs else None
    yaml_text = cfg.yaml_text if cfg else DEFAULT_CONFIG_YAML
    metrics = user.metrics or models.Metrics(user_id=user.id, attempts=0, successes=0)

    recent_incidents = (
        db.query(models.Incident)
        .filter(models.Incident.user_id == user.id)
        .order_by(models.Incident.created_at.desc())
        .limit(5)
        .all()
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user_email": email,
            "yaml_text": yaml_text,
            "message": "",
            "healed_yaml": None,
            "metrics": metrics,
            "recent_incidents": recent_incidents,
        },
    )


@app.post("/run-upload", response_class=HTMLResponse)
async def run_upload(
    request: Request,
    file_type: str = Form("csv"),
    data_file: UploadFile = File(...),
    yaml_text: str = Form(...),
    db: Session = Depends(get_db),
):
    email = get_current_user_email(request)
    if not email:
        return RedirectResponse(url="/")

    user = get_user_by_email(db, email)

    # parse YAML
    try:
        config = yaml.safe_load(yaml_text)
    except Exception as e:
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user_email": email,
                "yaml_text": yaml_text,
                "message": f"❌ Invalid YAML: {e}",
                "healed_yaml": None,
                "metrics": user.metrics,
                "recent_incidents": [],
            },
        )

    pipeline_name = config.get("pipeline_name", "user_pipeline")

    # read file into DataFrame
    try:
        contents = await data_file.read()
        if file_type == "json":
            df = pd.read_json(io.BytesIO(contents))
        else:
            df = pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user_email": email,
                "yaml_text": yaml_text,
                "message": f"❌ Failed to read data file ({file_type}): {e}",
                "healed_yaml": None,
                "metrics": user.metrics,
                "recent_incidents": [],
            },
        )

    metrics = user.metrics
    if not metrics:
        metrics = models.Metrics(user_id=user.id, attempts=0, successes=0)
        db.add(metrics)
        db.commit()
        db.refresh(metrics)

    message = ""
    healed_yaml = None

    try:
        run_validation(df, config, pipeline_name)
        inc = models.Incident(
            user_id=user.id,
            incident_type="run",
            status="success",
            message="Pipeline validation passed without healing.",
        )
        db.add(inc)
        db.commit()
        message = "✅ Pipeline validation passed without healing."

    except SchemaDriftError as e:
        metrics.attempts += 1
        db.add(metrics)
        db.commit()

        inc = models.Incident(
            user_id=user.id,
            incident_type="schema_drift",
            status="failed",
            message=str(e),
        )
        db.add(inc)
        db.commit()

        diagnosis = diagnose_schema_drift(e.missing_columns, config)
        healed_config = apply_schema_healing(config, diagnosis)
        healed_yaml = yaml.safe_dump(healed_config, sort_keys=False)

        try:
            run_validation(df, healed_config, pipeline_name)
            inc2 = models.Incident(
                user_id=user.id,
                incident_type="post_healing",
                status="success",
                message="Pipeline healed and validation passed on second run.",
            )
            db.add(inc2)
            metrics.successes += 1
            db.add(metrics)
            db.commit()
            message = "✅ Schema drift healed. Validation passed with suggested YAML."
        except Exception as e2:
            inc2 = models.Incident(
                user_id=user.id,
                incident_type="post_healing",
                status="failed",
                message=f"Post-healing failure: {e2}",
            )
            db.add(inc2)
            db.commit()
            message = "❌ Even after healing suggestion, validation failed. Please inspect YAML & data."

    except DataQualityError as e:
        metrics.attempts += 1
        db.add(metrics)
        db.commit()

        inc = models.Incident(
            user_id=user.id,
            incident_type="data_quality",
            status="failed",
            message=str(e),
        )
        db.add(inc)
        db.commit()

        diagnosis = diagnose_dq_issue(e, config)
        healed_config = apply_dq_healing(config, diagnosis)
        healed_yaml = yaml.safe_dump(healed_config, sort_keys=False)

        try:
            run_validation(df, healed_config, pipeline_name)
            inc2 = models.Incident(
                user_id=user.id,
                incident_type="post_healing_dq",
                status="success",
                message="DQ rule auto-tuned and validation passed.",
            )
            db.add(inc2)
            metrics.successes += 1
            db.add(metrics)
            db.commit()
            message = "✅ Data quality rule healed. Validation passed with suggested YAML."
        except Exception as e2:
            inc2 = models.Incident(
                user_id=user.id,
                incident_type="post_healing_dq",
                status="failed",
                message=f"Post-healing DQ failure: {e2}",
            )
            db.add(inc2)
            db.commit()
            message = f"❌ Data quality still failing after auto-healing suggestion: {e2}"

    db.refresh(metrics)
    recent_incidents = (
        db.query(models.Incident)
        .filter(models.Incident.user_id == user.id)
        .order_by(models.Incident.created_at.desc())
        .limit(5)
        .all()
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user_email": email,
            "yaml_text": healed_yaml or yaml_text,
            "message": message,
            "healed_yaml": healed_yaml,
            "metrics": metrics,
            "recent_incidents": recent_incidents,
        },
    )


@app.get("/incidents", response_class=HTMLResponse)
def view_incidents(request: Request, db: Session = Depends(get_db)):
    email = get_current_user_email(request)
    if not email:
        return RedirectResponse(url="/")

    user = get_user_by_email(db, email)
    incidents = (
        db.query(models.Incident)
        .filter(models.Incident.user_id == user.id)
        .order_by(models.Incident.created_at.desc())
        .all()
    )
    return templates.TemplateResponse(
        "incidents.html",
        {"request": request, "incidents": incidents},
    )
