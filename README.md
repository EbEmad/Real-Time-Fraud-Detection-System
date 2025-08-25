## Real-Time Fraud Detection System

This project demonstrates an end-to-end real-time fraud detection pipeline using Kafka, Python ML, MLflow, Airflow, Postgres, and Grafana.

### Components
- Kafka (with Kafka UI)
- Producer (simulated credit card transactions)
- Fraud Service (FastAPI + MLflow model inference)
- Postgres (store predictions)
- MLflow Tracking Server (with Postgres backend)
- Airflow (periodic retraining and model registry)
- Grafana (visualization)

### Quickstart
1. Copy environment variables and adjust if needed:
   ```bash
   cp .env.example .env
   ```

2. Start the stack:
   ```bash
   docker compose up -d --build
   ```

3. URLs:
   - Kafka UI: `http://localhost:8085`
   - Fraud API: `http://localhost:8000/docs`
   - MLflow UI: `http://localhost:5000`
   - Airflow UI: `http://localhost:8080` (admin/admin)
   - Grafana: `http://localhost:3000` (admin/`GRAFANA_ADMIN_PASSWORD`)

4. Observe data:
   - Producer writes to Kafka topic `transactions`
   - Fraud service consumes, infers, and writes to Postgres table `predictions`
   - Grafana shows dashboards from the predictions table

### Airflow retraining
Airflow DAG `train_and_register` runs a training job that logs to MLflow and promotes the latest model version to Production. You can trigger the DAG manually in the UI.

### Development
- Services are in `services/*`
- Airflow DAGs in `airflow/dags`
- Grafana provisioning in `grafana/*`

### Teardown
```bash
docker compose down -v
```




