import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, List

import mlflow
import numpy as np
from fastapi import FastAPI
from kafka import KafkaConsumer
from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.sql import func



KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC","transactions")
POSTGRES_URI=os.getenv(
    "POSTGRES_URI",
    "postgresql+psycopg2://admin:admin@postgres:5432/frauddb",
)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "fraud_detector")
MLFLOW_MODEL_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "Production")

app = FastAPI(title="Fraud Detection Inference Service")


# Database setup
engine=create_engine(POSTGRES_URI,pool_pre_ping=True)
metadata=MetaData()
predictions_table=Table(
    "predictions",
    metadata,
    Column("id",Integer,primary_key=True,utoincrement=True),
    Column("event_time", DateTime(timezone=True), server_default=func.now()),
    Column("transaction_id", String(64), nullable=False),
    Column("amount", DOUBLE_PRECISION, nullable=False),
    Column("features", JSON, nullable=False),
    Column("prediction", Integer, nullable=False),
    Column("proba", DOUBLE_PRECISION),
    Column("model_version", String(255)),
    Column("raw_payload", JSON),
    schema="public",
    extend_existing=True,
    )


def ensure_table():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        metadata.create_all(conn)


class Transaction(BaseModel):
    transaction_id: str
    amount: float
    features: Dict[str, Any] = Field(default_factory=dict)

FEATURE_ORDER=List[str]= ["amount", "num_items", "merchant_risk", "hour"]


class ModelWrapper:
    def __init__(self)->None:
        self._model: Optional[Any]=None
        self._model_version: Optional[Any]=None
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    def load(self)->None:
        try:
            client=mlflow.tracking.MlflowClient()
            if MLFLOW_MODEL_STAGE:
                mv=client.get_latest_versions(MLFLOW_MODEL_NAME,stages=[MLFLOW_MODEL_STAGE])
                if not mv:
                    self._model=None
                    self._model_version=None
                    return
                run_id=mv[0].version
                model_uri=f"runs:/{run_id}/model"
            else:
                versions=client.get_latest_versions(MLFLOW_MODEL_NAME)
                if not versions:
                    self._model=None
                    self._model_version=None
                    return
                run_id=versions[0].run_id
                self._model_version=versions[0].version
                model_uri=f"runs:/{run_id}/model"
            self._model=mlflow.pyfunc.load_model(model_uri)
        except Exception:
            # Model may not exist yet
            self._model=None
            self._model_version=None

    def predict_proba(self,features:Dict[str,Any])->float:
        if self._model is None:
            print("No Model Yet")
            # Fallback: simple heuristic based on amount
            amount=float(features.get('amount',0.0))
            return 1.0 if amount>1000 else 0.05

        # Project Features in the training order
        x=np.array([[float(features.get(k,0.0)) for k in FEATURE_ORDER]],dtype=float)
        try:
            # pyfunc models don't always expose predict_proba; call predict
            y=self._model.predict(x)

            # If shape is (n,2), treat as proba for [0,1]
            if isinstance(y,(list,tuple,np.ndarray)):
                arr=np.array(y)
                if arr.ndim==2 and arr.shape[1]>=2:
                    return float(arr[0][1])
                return float(arr[0])
            return float(y)
        except Exception:
            amount=float(features.get('amount',0.0))
            return 1.0 if amount>1000 else 0.05
    @property
    def version(self)-> Optional[str]:
        return self._model_version

