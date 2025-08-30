import os
import random
from typing import Tuple
import mlflow
import numpy as np
import pandas as pd
from mlflow.models.signature import infer_signature
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import auc, roc_auc_score, roc_curve
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def generate_synthetic_data(n: int = 5000, seed: int = 42) -> pd.DataFrame:
    random.seed(seed)
    np.random.seed(seed)
    amounts = np.random.lognormal(mean=3.0, sigma=1.0, size=n)
    num_items = np.maximum(1, np.random.normal(loc=2.0, scale=1.0, size=n)).astype(int)
    merchant_risk = np.random.rand(n)
    hour = np.random.randint(0, 24, size=n)

    # Latent fraud probability: high amount, high risk, odd hour
    logits = 0.002 * amounts + 1.5 * merchant_risk + 0.05 * (hour.isin([0, 1, 2, 3, 23]).astype(float))
    probs = 1 / (1 + np.exp(-(logits - 2.5)))
    y = (np.random.rand(n) < probs).astype(int)

    df = pd.DataFrame(
        {
            "amount": amounts,
            "num_items": num_items,
            "merchant_risk": merchant_risk,
            "hour": hour.astype(float),
            "label": y,
        }
    )
    return df

def train_model(df: pd.DataFrame) -> Tuple[LogisticRegression, StandardScaler, float]:
    X = df[["amount", "num_items", "merchant_risk", "hour"]].values.astype(float)
    y = df["label"].values
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    clf = LogisticRegression(max_iter=1000)
    clf.fit(X_train_s, y_train)
    y_score = clf.predict_proba(X_test_s)[:, 1]
    auc_score = roc_auc_score(y_test, y_score)
    return clf, scaler, float(auc_score)



def main() -> None:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "fraud_detection")
    registered_model_name = os.getenv("REGISTERED_MODEL_NAME", "fraud_detector")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    # get the data and train the model
    df = generate_synthetic_data()
    clf, scaler, auc_score = train_model(df)


    with mlflow.start_run()as run:
        mlflow.log_metric("auc",auc_score)
        x = df[["amount", "num_items", "merchant_risk", "hour"]]
        signature=infer_signature(x,clf.predict_proba(StandardScaler().fit_transform(x.values))[:,1])

        # Combine scaler + model into a single pyfunc
        import cloudpickle
        import tempfile

        class SklearnWrapper(mlflow.pyfunc.PythonModel):
            def __init__(self,scaler,model):
                self.scaler=scaler
                self.model=model
            def predict(self,conext,model_input):
                x=np.array(model_input,dtype=float)
                xs = self.scaler.transform(x)
                if hasattr(self.model,"predict_proba"):
                    return self.model.predict_proba(xs)
                return self.model.predict(xs)
        wrapped=SklearnWrapper(scaler,clf)

        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=wrapped,
            registered_model_name=registered_model_name,
            signature=signature,
        )

        # Transition latest model version to Production
        client = mlflow.tracking.MlflowClient()
        versions = client.get_latest_versions(registered_model_name)
        if versions:
            v = sorted(versions, key=lambda m: int(m.version))[-1]
            client.transition_model_version_stage(
                name=registered_model_name,
                version=v.version,
                stage="Production",
                archive_existing_versions=True,
            )
    print("Training complete. AUC=", auc_score)

if __name__=="__main__":
    main()
