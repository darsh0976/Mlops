# MLOps Pipeline - Inner Workings Explanation

## Overview

This document details the inner workings of each function and how they collectively form a deterministic batch processing pipeline for cryptocurrency OHLCV data. The pipeline loads configuration, validates input data, computes technical trading signals, and outputs metrics in JSON format with comprehensive error handling.

---

## Architecture & Big Picture

```
┌─────────────────────────────────────────────────────────┐
│                    main() Entry Point                    │
│  Orchestrates pipeline, handles errors, measures time  │
└──────────────────────┬──────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
  ┌─────────────┐           ┌──────────────────┐
  │ parse_args()│           │ setup_logger()   │
  │   (CLI)     │           │  (Logging)       │
  └─────────────┘           └──────────────────┘
        │                             │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌──────────────────────────┐
        │  run_pipeline()          │
        │  Core processing logic   │
        └──────┬──────────┬────────┘
               │          │
        ┌──────▼─┐  ┌─────▼──────┐
        │ CONFIG │  │ DATA LOAD  │
        └──────┬─┘  └─────┬──────┘
               │          │
        ┌──────▼─┐  ┌─────▼──────────────┐
        │ Valid  │  │ compute_signals()  │
        │ Config │  │ (Core Algorithm)   │
        └────────┘  └─────┬──────────────┘
                          │
                ┌─────────▼─────────┐
                │  Return Metrics   │
                │  (JSON Output)    │
                └───────────────────┘
```

---

## Function-by-Function Breakdown

### 1. `main() -> int`

**Purpose:** Entry point and orchestrator for the entire batch pipeline.

**Inner Workings:**
1. Parses command-line arguments using `parse_args()`
2. Records start time for latency measurement
3. Establishes logger via `setup_logger()` (writes to both file and stdout)
4. Calls `run_pipeline()` within try-except block
5. On **success**: Calculates latency_ms, appends to metrics, writes JSON, logs completion
6. On **failure**: Attempts to extract version from config (best-effort), writes error JSON with error message
7. Returns exit code (0 = success, 1 = failure)

**Error Handling:** Catches all exceptions; attempts to preserve version number for error response.

**Data Flow:**
```
Command-line args → parsed args → pipeline execution → metrics/error JSON → exit code
```

---

### 2. `parse_args() -> argparse.Namespace`

**Purpose:** Parse and validate command-line arguments for the pipeline.

**Inner Workings:**
1. Creates ArgumentParser with description
2. Registers 4 required arguments:
   - `--input`: Path to CSV data file
   - `--config`: Path to YAML configuration file
   - `--output`: Destination for metrics JSON
   - `--log-file`: Destination for log output
3. All arguments marked as `required=True`
4. Returns parsed Namespace object with validated arguments

**Validation Level:** Syntax validation only (argparse ensures all required args present).

---

### 3. `setup_logger(log_file: str) -> logging.Logger`

**Purpose:** Configure a dual-output logger for structured logging.

**Inner Workings:**
1. Gets or creates logger named "mlops_task"
2. Sets level to INFO (all INFO+ messages logged)
3. Clears any existing handlers (idempotent design)
4. Creates formatter: `"%(asctime)s - %(levelname)s - %(message)s"`
5. Adds FileHandler to write logs to specified file
6. Adds StreamHandler to write logs to stdout simultaneously
7. Returns configured logger instance

**Dual Output:** Every log entry appears in both file and console, maintaining audit trail while providing real-time visibility.

---

### 4. `run_pipeline(input_path: str, config_path: str, logger: Logger) -> Dict[str, Any]`

**Purpose:** Core pipeline orchestration - loads config, validates data, computes signals.

**Inner Workings:**
1. **Load & Validate Config:** Calls `load_and_validate_config()`, extracts seed/window/version
2. **Set Seed:** Initializes `random.seed(seed)` for deterministic execution
3. **Log Configuration:** Records loaded config parameters
4. **Load & Validate Data:** Calls `load_and_validate_data()`, receives list of row dicts with numeric 'close'
5. **Extract Close Prices:** Builds list of float values from 'close' column
6. **Compute Signals:** Calls `compute_signals()` with closes and window, gets binary signal list
7. **Calculate Metrics:**
   - signal_rate = sum(signals) / rows_processed (proportion of 1s)
   - Rounds to 4 decimal places
8. **Return Success Metrics:** Dict with version, rows_processed, metric name, signal_rate value, seed, status

**Key Property:** Entire process is deterministic - same inputs always produce identical outputs due to fixed seed.

---

### 5. `load_and_validate_config(config_path: str) -> Dict[str, Any]`

**Purpose:** Load YAML config file with comprehensive validation.

**Inner Workings:**
1. **File Existence Check:** Raises PipelineError if file doesn't exist
2. **Parse YAML:** Calls `parse_simple_yaml()` to read key-value pairs
   - Catches OSError, re-raises as PipelineError with clean message
3. **Required Keys Check:** Verifies presence of ["seed", "window", "version"]
   - If missing, lists which keys are absent
4. **Type Validation:**
   - `seed`: Must be int (any value)
   - `window`: Must be positive int (> 0)
   - `version`: Must be non-empty string
5. **Return:** Validated dict with correctly typed values

**Error Messages:** Specific enough to identify problem, generic enough to not expose paths.

**Example Valid Config:**
```yaml
seed: 42
window: 5
version: "v1"
```

---

### 6. `load_and_validate_data(input_path: str) -> List[Dict[str, Any]]`

**Purpose:** Load CSV file with comprehensive validation and type conversion.

**Inner Workings:**
1. **File Existence Check:** Raises if file missing
2. **Readability Check:** Verifies file is readable (os.access)
3. **Empty File Check:** Rejects zero-byte files
4. **CSV Parsing:**
   - Opens file with UTF-8 encoding, newline='' (proper CSV handling)
   - Creates csv.DictReader to parse headers and rows
   - Checks fieldnames not None (verifies file has header row)
5. **Column Validation:** Ensures 'close' column exists
   - If missing, raises informative PipelineError
6. **Type Conversion:**
   - For each row, converts 'close' value to float
   - Catches TypeError/ValueError, raises PipelineError
   - Other row data remains as strings (original types)
7. **Empty Data Check:** After iteration, ensures at least 1 row loaded
8. **Return:** List of row dictionaries with 'close' as float, other fields as strings

**Robustness:** Handles files with headers but no data rows, non-CSV files, missing columns, non-numeric values.

---

### 7. `compute_signals(closes: List[float], window: int) -> List[int]`

**Purpose:** Generate binary trading signals based on rolling mean comparison.

**Algorithm:** Streaming window algorithm (O(n) time, O(window) space)

**Inner Workings:**
1. **Initialize:** Empty signals list, running_sum = 0.0, empty buffer
2. **For each close price:**
   - Append to buffer
   - Add to running_sum
   - If buffer exceeds window: Remove oldest value from sum (buffer.pop(0))
   - If buffer < window: Append 0 signal (insufficient data, convention)
   - If buffer == window:
     - Calculate rolling_mean = running_sum / window
     - Compare: 1 if close > rolling_mean, else 0
     - Append signal to list
3. **Return:** List of signals, same length as closes

**Example (window=3, closes=[10, 11, 12, 11, 10, 9, 10, 11]):**
```
Step 1: buffer=[10],    sum=10,   < 3 → signal=0
Step 2: buffer=[10,11], sum=21,   < 3 → signal=0
Step 3: buffer=[10,11,12], sum=33, mean=11 → 12>11 → signal=1
Step 4: buffer=[11,12,11], sum=34, mean=11.33 → 11<11.33 → signal=0
... continues ...
```

**Properties:**
- First (window-1) signals always 0
- Deterministic given same input
- Efficient streaming computation (no recomputation of rolling means)

---

### 8. `parse_simple_yaml(path: str) -> Dict[str, Any]`

**Purpose:** Parse simple key: value YAML format used in this assessment.

**Inner Workings:**
1. **Initialize:** Empty dict for results
2. **Read File:** Open with UTF-8 encoding
3. **For each line:**
   - Strip whitespace
   - Skip empty lines and comments (lines starting with #)
   - If no ':' found: Raise PipelineError
   - Split on first ':' into key and value
   - Strip both further
4. **Type Coercion:**
   - If value wrapped in quotes: Remove them (string literal)
   - If value is all digits (or negative int): Convert to int
   - Otherwise: Keep as string
5. **Store & Return:** Dict with parsed key-value pairs

**Example:**
```yaml
seed: 42          → ("seed", 42)         [int]
window: 5         → ("window", 5)        [int]
version: "v1"     → ("version", "v1")    [str, quotes stripped]
```

---

### 9. `write_json(path: str, payload: Dict[str, Any]) -> None`

**Purpose:** Serialize dict to pretty-printed JSON file.

**Inner Workings:**
1. Opens file at path for writing with UTF-8 encoding
2. Uses json.dump() with indent=2 for 2-space formatting
3. Closes file automatically (context manager)
4. No return value

**Output Format:** Human-readable JSON with consistent indentation.

**Example Success Output:**
```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.5006,
  "seed": 42,
  "status": "success",
  "latency_ms": 23
}
```

---

### 10. `PipelineError` Exception Class

**Purpose:** Domain-specific exception for pipeline validation/processing errors.

**Inner Workings:**
- Inherits from base Exception
- Allows catching pipeline-specific errors separately from system errors
- Provides clean error messages to users

**Usage:** Raised by validation functions, caught in main() and converted to error JSON.

---

## Error Handling Flow

The pipeline implements a **layered error handling** strategy:

```
load_and_validate_config()  ──┐
                             │ ┌──────────────────┐
load_and_validate_data()    ──┼─→ PipelineError  │
                             │ └┬─────────────────┘
compute_signals()           ──┘   │
                                   │
                     ┌─────────────▼───────────┐
                     │ run_pipeline() catches  │
                     │ Re-raises as-is         │
                     └────────────┬────────────┘
                                  │
                     ┌────────────▼─────────────┐
                     │ main() catches all       │
                     │ Converts to error JSON  │
                     │ Logs exception trace    │
                     └─────────────────────────┘
```

**Error JSON Output:**
```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Missing required columns: close"
}
```

---

## Data Types & Conversions

| Stage | Data Structure | Details |
|-------|---|---|
| Config | `Dict[str, Any]` | {"seed": int, "window": int, "version": str} |
| CSV Rows | `List[Dict[str, Any]]` | List of row dicts; 'close' is float, others are str |
| Close Prices | `List[float]` | Extracted from rows, numeric only |
| Signals | `List[int]` | Binary [0 or 1], length matches closes |
| Metrics | `Dict[str, Any]` | Mixed types: int, float, str |

---

## Determinism Guarantees

The pipeline is **fully deterministic** because:

1. **Fixed Random Seed:** `random.seed()` ensures reproducible behavior
2. **No External State:** Doesn't rely on system time, environment variables (except for CLI args)
3. **Algorithmic Stability:** Rolling mean computation is stateless
4. **No Floating-Point Surprises:** Signal comparison is deterministic (comparison operators, not approximations)

**Invariant:** Given same config and data files, pipeline always produces identical signal_rate and metrics.

---

## Performance Characteristics

| Function | Time Complexity | Space Complexity |
|----------|---|---|
| parse_simple_yaml() | O(n) lines | O(k) keys |
| load_and_validate_config() | O(k) | O(k) |
| load_and_validate_data() | O(n rows) | O(n rows) |
| compute_signals() | O(n prices) | O(window) |
| run_pipeline() | O(n) | O(n) |
| Overall | O(n) | O(n) |

**Typical Performance:** 10,000 rows processed in ~20-30ms on mid-range hardware.

---

## Logging Strategy

Log entries are generated at key checkpoints:

```
Job started
Config loaded: seed=42, window=5, version=v1
Data loaded: 10000 rows
Rolling mean calculated with window=5
Signals generated
Metrics: signal_rate=0.5006, rows_processed=10000
Job completed successfully in 23ms
```

**On Error:**
```
Job started
Config loaded: seed=42, window=5, version=v1
ERROR - Job failed: Missing input file
```

---

## Testing Strategy

The pipeline is validated through integration tests:

- **Success Path:** Valid config + valid data → metrics JSON with success status
- **Missing File:** Invalid paths → error JSON with meaningful message
- **Missing Column:** Data without 'close' → error JSON with specific error
- **Invalid Config:** Missing required fields → error JSON with validation details
- **Determinism Check:** Same inputs run twice → identical metrics output

---

## Extensibility Points

Future enhancements could hook into:

1. **Signal Computation:** Replace or add to `compute_signals()` for different indicators
2. **Validation Rules:** Extend `load_and_validate_config()` for additional config patterns
3. **Output Format:** Adapt `write_json()` or add new output formatters
4. **Metrics Calculation:** Add aggregations beyond signal_rate in `run_pipeline()`
5. **Logging:** Customize formatter or add handlers in `setup_logger()`

---

## Summary

The pipeline is a **clean, single-responsibility** batch processor where:
- **main()** orchestrates
- **parse_args()** extracts CLI inputs
- **setup_logger()** enables observability
- **run_pipeline()** conducts the workflow
- Validation functions ensure data quality
- **compute_signals()** performs core analysis
- Type hints and docstrings provide clarity
- Error handling preserves version info and provides context

All components work together to deliver **deterministic, observable, robust batch processing** of cryptocurrency data with trading signal generation.
