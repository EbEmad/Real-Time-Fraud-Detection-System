-- Create additional databases for MLflow and Airflow if missing
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow') THEN
      EXECUTE 'CREATE DATABASE mlflow';
   END IF;
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
      EXECUTE 'CREATE DATABASE airflow';
   END IF;
END
$$;

-- Create predictions table in the default database ($POSTGRES_DB)
CREATE TABLE IF NOT EXISTS public.predictions (
  id BIGSERIAL PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  transaction_id VARCHAR(64) NOT NULL,
  amount DOUBLE PRECISION NOT NULL,
  features JSONB NOT NULL,
  prediction INTEGER NOT NULL,
  proba DOUBLE PRECISION,
  model_version TEXT,
  raw_payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_predictions_time ON public.predictions (event_time);
CREATE INDEX IF NOT EXISTS idx_predictions_txid ON public.predictions (transaction_id);
CREATE INDEX IF NOT EXISTS idx_predictions_pred ON public.predictions (prediction);

-- Daily metrics table
CREATE TABLE IF NOT EXISTS public.predictions_daily_metrics (
  day DATE PRIMARY KEY,
  num_predictions BIGINT NOT NULL,
  fraud_rate DOUBLE PRECISION,
  avg_amount DOUBLE PRECISION,
  avg_proba DOUBLE PRECISION
);

