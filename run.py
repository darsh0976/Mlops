import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from typing import Any, Dict, List


class PipelineError(Exception):
    """Domain-specific pipeline error."""


def parse_simple_yaml(path: str) -> Dict[str, Any]:
    """Parse a simple key: value YAML file used in this assessment."""
    data: Dict[str, Any] = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                raise PipelineError("Invalid configuration file structure")
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                value = int(value)
            data[key] = value
    return data


def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops_task")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def load_and_validate_config(config_path: str) -> Dict[str, Any]:
    if not os.path.isfile(config_path):
        raise PipelineError(f"Configuration file not found: {config_path}")

    try:
        config = parse_simple_yaml(config_path)
    except OSError as exc:
        raise PipelineError(f"Unable to read configuration file: {exc}") from exc

    required_keys = ["seed", "window", "version"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise PipelineError(f"Invalid configuration file structure: missing keys {missing}")

    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    if not isinstance(seed, int):
        raise PipelineError("Invalid configuration file structure: 'seed' must be an integer")
    if not isinstance(window, int) or window <= 0:
        raise PipelineError("Invalid configuration file structure: 'window' must be a positive integer")
    if not isinstance(version, str) or not version:
        raise PipelineError("Invalid configuration file structure: 'version' must be a non-empty string")

    return {"seed": seed, "window": window, "version": version}


def load_and_validate_data(input_path: str) -> List[Dict[str, Any]]:
    if not os.path.isfile(input_path):
        raise PipelineError(f"Missing input file: {input_path}")
    if not os.access(input_path, os.R_OK):
        raise PipelineError(f"Input file is not readable: {input_path}")

    if os.path.getsize(input_path) == 0:
        raise PipelineError("Empty input file")

    try:
        with open(input_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise PipelineError("Empty input file")
            if "close" not in reader.fieldnames:
                raise PipelineError("Missing required columns in dataset: ['close']")

            rows: List[Dict[str, Any]] = []
            for row in reader:
                row_copy = dict(row)
                try:
                    row_copy["close"] = float(row_copy["close"])
                except (TypeError, ValueError) as exc:
                    raise PipelineError("Invalid CSV file format: non-numeric close value") from exc
                rows.append(row_copy)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError(f"Invalid CSV file format: {exc}") from exc

    if not rows:
        raise PipelineError("Empty input file")
    return rows


def compute_signals(closes: List[float], window: int) -> List[int]:
    signals: List[int] = []
    running_sum = 0.0
    buffer: List[float] = []

    for close in closes:
        buffer.append(close)
        running_sum += close

        if len(buffer) > window:
            running_sum -= buffer.pop(0)

        if len(buffer) < window:
            signals.append(0)
        else:
            rolling_mean = running_sum / window
            signals.append(1 if close > rolling_mean else 0)

    return signals


def run_pipeline(input_path: str, config_path: str, logger: logging.Logger) -> Dict[str, Any]:
    config = load_and_validate_config(config_path)
    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    random.seed(seed)
    logger.info("Config loaded: seed=%s, window=%s, version=%s", seed, window, version)

    rows = load_and_validate_data(input_path)
    rows_processed = len(rows)
    logger.info("Data loaded: %s rows", rows_processed)

    closes = [row["close"] for row in rows]
    logger.info("Rolling mean calculated with window=%s", window)
    signals = compute_signals(closes, window)
    logger.info("Signals generated")

    signal_rate = sum(signals) / rows_processed
    logger.info("Metrics: signal_rate=%.4f, rows_processed=%s", signal_rate, rows_processed)

    return {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": round(signal_rate, 4),
        "seed": seed,
        "status": "success",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mini MLOps batch pipeline")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--config", required=True, help="Configuration YAML file path")
    parser.add_argument("--output", required=True, help="Output metrics JSON file path")
    parser.add_argument("--log-file", required=True, help="Log file path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = time.time()
    logger = setup_logger(args.log_file)

    logger.info("Job started")

    try:
        metrics = run_pipeline(args.input, args.config, logger)
        latency_ms = int((time.time() - start) * 1000)
        metrics["latency_ms"] = latency_ms

        write_json(args.output, metrics)
        print(json.dumps(metrics, indent=2))
        logger.info("Job completed successfully in %sms", latency_ms)
        return 0
    except Exception as exc:
        logger.exception("Job failed: %s", exc)
        version = "v1"
        try:
            cfg = load_and_validate_config(args.config)
            version = cfg.get("version", "v1")
        except Exception:
            pass

        error_payload = {
            "version": version,
            "status": "error",
            "error_message": str(exc),
        }
        write_json(args.output, error_payload)
        print(json.dumps(error_payload, indent=2))
        return 1


if __name__ == "__main__":
    sys.exit(main())
