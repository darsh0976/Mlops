# Mini MLOps Pipeline (Technical Assessment)

This repository implements a deterministic batch pipeline for cryptocurrency OHLCV data using a config-driven workflow, structured logging, machine-readable metrics, and Docker packaging.

## Features Implemented
- Deterministic execution via config-based seed.
- Required CLI interface:
  - `python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log`
- Config validation (`seed`, `window`, `version`).
- CSV validation (`close` column required, empty/invalid file handling).
- Rolling mean computation on `close`.
- Signal generation (`1` if `close > rolling_mean`, else `0`).
- Metrics output in required JSON schema.
- Structured run logging to a file.
- Error JSON output on failure.
- Dockerfile for batch execution.

## Setup Instructions
```bash
# Install dependencies
pip install -r requirements.txt
```

## Local Execution Instructions
```bash
# Run locally
python run.py --input data.csv --config config.yaml \
    --output metrics.json --log-file run.log
```

## Docker Instructions
```bash
# Build the Docker image
docker build -t mlops-task .

# Run the container
docker run --rm mlops-task
```

## Expected Metrics Output
```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.5006,
  "latency_ms": 65,
  "seed": 42,
  "status": "success"
}
```

## Error Output Format
```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong"
}
```

## Files
- `run.py` – main batch pipeline.
- `config.yaml` – runtime configuration.
- `data.csv` – sample OHLCV dataset (10,000 rows).
- `requirements.txt` – Python dependencies.
- `Dockerfile` – containerized batch job.
- `metrics.json` – example successful metrics output.
- `run.log` – example successful run log.

## Dependencies
- pandas
- numpy
- PyYAML
- pytest (for local tests)
