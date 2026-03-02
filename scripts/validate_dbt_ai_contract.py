#!/usr/bin/env python3
"""
validate_dbt_ai_contract.py

Purpose
-------
Validate that dbt schema YAML files include a consistent AI/LLM-friendly contract.

Why this script exists
----------------------
dbt tests enforce data behavior (nulls, uniqueness, etc.), but they do not enforce that
your metadata contract is complete and consistent across models/columns.
This script fills that gap by checking required `meta` keys and allowed semantic types.

How to run
----------
python scripts/validate_dbt_ai_contract.py

Exit codes
----------
0 -> validation passed
1 -> validation failed (missing keys, bad semantic types, malformed schema structure)
"""

from pathlib import Path
import sys
import yaml

# ---------------------------------------------------------------------------
# Contract definitions
# ---------------------------------------------------------------------------
# These sets are the required metadata keys.
# Keeping these centralized makes it easy to evolve your standard later.

# Required keys every model-level `meta` block must contain.
REQUIRED_MODEL_META = {
    "ai_contract_version",  # explicit contract version for future migrations
    "layer",                # bronze_stage / silver_stage / gold_mart
    "owner_team",           # ownership for accountability and routing
    "grain",                # row-level grain contract, e.g. one row per coin_id + date
    "refresh_cadence",      # operational expectation (daily/hourly/etc.)
    "pii",                  # governance signal (true/false)
}

# Required keys every column-level `meta` block must contain.
REQUIRED_COLUMN_META = {
    "semantic_type",  # canonical semantic category for AI and documentation
    "nullable",       # explicit nullability policy
    "unit",           # e.g. USD, percent, null for identifiers
    "valid_min",      # lower domain bound where relevant
    "valid_max",      # upper domain bound where relevant
}

# Controlled vocabulary for semantic_type.
# Restricting this list prevents drift and improves downstream automation.
ALLOWED_SEMANTIC_TYPES = {
    "identifier",
    "date",
    "timestamp",
    "currency_usd",
    "percentage",
    "count",
    "ratio",
    "json",
    "string",
    "boolean",
}

# Files we consider schema contracts.
# If your project adds other locations, extend this list.
SCHEMA_GLOBS = [
    "models/**/schema.yml",
    "models/**/schema.yaml",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_yaml(path: Path):
    """
    Parse YAML file safely and return a dict-like object.

    Why:
    - safe_load prevents execution of arbitrary YAML tags.
    - returning {} on empty files allows consistent downstream checks.
    """
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def validate_schema_file(path: Path):
    """
    Validate one schema.yml file and return a list of human-readable errors.

    Validation scope:
    - file has `models` list
    - each model has required model-level meta keys
    - each column has required column-level meta keys
    - semantic_type is in allowed controlled vocabulary
    """
    errors = []
    data = load_yaml(path)

    models = data.get("models", [])
    if not isinstance(models, list):
        return [f"{path}: 'models' must be a list"]

    for m in models:
        model_name = m.get("name", "<missing-model-name>")

        # --- model-level meta checks ---
        model_meta = m.get("meta")
        if not isinstance(model_meta, dict):
            errors.append(f"{path}::{model_name}: missing model-level meta")
            model_meta = {}

        missing_model_keys = REQUIRED_MODEL_META - set(model_meta.keys())
        if missing_model_keys:
            errors.append(
                f"{path}::{model_name}: missing model meta keys: {sorted(missing_model_keys)}"
            )

        # --- column-level checks ---
        columns = m.get("columns", [])
        if not isinstance(columns, list):
            errors.append(f"{path}::{model_name}: 'columns' must be a list")
            continue

        for c in columns:
            col_name = c.get("name", "<missing-column-name>")

            col_meta = c.get("meta")
            if not isinstance(col_meta, dict):
                errors.append(
                    f"{path}::{model_name}.{col_name}: missing column-level meta"
                )
                col_meta = {}

            missing_col_keys = REQUIRED_COLUMN_META - set(col_meta.keys())
            if missing_col_keys:
                errors.append(
                    f"{path}::{model_name}.{col_name}: missing column meta keys: {sorted(missing_col_keys)}"
                )

            semantic_type = col_meta.get("semantic_type")
            if semantic_type and semantic_type not in ALLOWED_SEMANTIC_TYPES:
                errors.append(
                    f"{path}::{model_name}.{col_name}: invalid semantic_type '{semantic_type}'"
                )

    return errors


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main():
    """
    Discover schema files, validate each file, print all errors, and return status code.

    Why this structure:
    - We aggregate all failures in one run so developer gets complete feedback.
    - Non-zero exit code allows easy CI/pre-commit enforcement.
    """
    root = Path(".")
    schema_files = []

    for pattern in SCHEMA_GLOBS:
        schema_files.extend(root.glob(pattern))

    schema_files = sorted(set(schema_files))
    if not schema_files:
        print("No schema.yml files found under models/.")
        return 1

    all_errors = []
    for sf in schema_files:
        all_errors.extend(validate_schema_file(sf))

    if all_errors:
        print("AI contract validation failed:\n")
        for e in all_errors:
            print(f"- {e}")
        return 1

    print("AI contract validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())