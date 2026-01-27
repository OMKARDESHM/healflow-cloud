from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Text,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from .db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    pipelines = relationship("Pipeline", back_populates="owner")


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)

    # simple scheduling: number of minutes between automatic runs (future use)
    schedule_interval_minutes = Column(Integer, nullable=True)

    last_run_status = Column(String(50), nullable=True)
    last_run_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    owner = relationship("User", back_populates="pipelines")
    configs = relationship("ConfigVersion", back_populates="pipeline")
    runs = relationship("PipelineRun", back_populates="pipeline")
    incidents = relationship("Incident", back_populates="pipeline")


class ConfigVersion(Base):
    __tablename__ = "config_versions"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)

    version = Column(Integer, nullable=False)
    yaml_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    comment = Column(String(255), nullable=True)

    pipeline = relationship("Pipeline", back_populates="configs")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)

    run_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50), nullable=False)  # success, failed, healed
    rows = Column(Integer, nullable=True)
    message = Column(Text, nullable=True)
    healing_actions = Column(Text, nullable=True)

    pipeline = relationship("Pipeline", back_populates="runs")
    incidents = relationship("Incident", back_populates="run")


class Incident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True, index=True)

    pipeline_id = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    run_id = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=True)

    stage = Column(String(100), nullable=False)   # e.g. schema, dq, healing
    status = Column(String(50), nullable=False)   # failed, healed, warning, info
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="incidents")
    run = relationship("PipelineRun", back_populates="incidents")
