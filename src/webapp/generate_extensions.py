from typing import Union, List, Dict, Optional, Any
import pandas as pd
import tempfile
import os
import json

from validation import validate_dataset, normalize_col, HardValidationError


def load_json(path: str) -> Any:
    """Load JSON from a file, returning {} on failure."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def infer_column_schema(series: pd.Series, cate_threshold: int = 10) -> dict:
    """
    Infer a minimal Pandera-style schema for a pandas Series.
    Categorical-like columns are marked as 'category' but NOT constrained to a fixed set of values.
    """

    non_null = series.dropna()
    has_nulls = bool(series.isna().any())
    uniques = non_null.unique().tolist()

    checks: List[Dict[str, Any]] = []
    
    # candidate for categorical, but relaxed (no fixed categories)
    if 1 < len(uniques) <= cate_threshold:
        return {
            "dtype": "category",
            "coerce": True,
            "nullable": True,
            "required": True,
            "aliases": [],
            "checks": [],  # no 'isin' constraint
        }

    # numeric / datetime / bool / string fallback
    dt = series.dtype
    if pd.api.types.is_integer_dtype(dt):
        dtype = "Int64" if has_nulls else "int64"
        checks = []
    elif pd.api.types.is_float_dtype(dt):
        dtype = "float64"
        checks = []
    elif pd.api.types.is_bool_dtype(dt):
        dtype = "boolean"
        checks = []
    elif pd.api.types.is_datetime64_any_dtype(dt):
        dtype = "datetime64[ns]"
        checks = []
    else:
        dtype = "string"
        checks = [{"type": "str_length", "kwargs": {"min_value": 1}}]

    return {
        "dtype": dtype,
        "coerce": True,
        "nullable": True,
        "required": True,
        "aliases": [],
        "checks": checks,
    }


def generate_extension_schema(
    df: Union[pd.DataFrame, str],
    models: Union[str, List[str]],
    institution_id: str,
    base_schema: Dict,  # <- reference only, not mutated
    existing_extension: Optional[Dict] = None,  # <- merged into/returned
) -> Dict:
    """
    - Use validate_dataset(...) with base_schema (+ existing_extension if provided)
      to detect columns not represented there.
    - Infer specs for those "extra" columns and add only those to the extension.
    - Return the extension dict (no writes, base_schema untouched).
    """
    # Load/normalize DF (keep a path for validate_dataset which expects a filename)
    orig_path: Optional[str] = None
    if isinstance(df, str):
        orig_path = df
        df = pd.read_csv(df)
    df = df.rename(columns=lambda c: normalize_col(c))

    # Ensure validate_dataset gets a path
    tmp_path = None
    data_path = orig_path
    if data_path is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
        data_path = tmp_path

    # Validate to discover extras (not in base or existing extension)
    extras: List[str] = []
    try:
        validate_dataset(
            filename=data_path,
            base_schema=base_schema,
            ext_schema=existing_extension,  # columns already in extension won't be "extra"
            models=models,
            institution_id=institution_id,
        )
    except HardValidationError as e:
        extras = e.extra_columns or []
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # Nothing new to add
    if not extras:
        return existing_extension or {
            "version": base_schema.get("version", "1.0.0"),
            "institutions": {institution_id: {"data_models": {}}},
        }

    # Keep only extras actually present in the (normalized) DF
    extras = [c for c in extras if c in df.columns]

    # Infer minimal specs for each extra column
    inferred = {col: infer_column_schema(df[col]) for col in extras}

    # Start from provided extension (or a fresh skeleton); base_schema is NOT modified
    extension = existing_extension or {
        "version": base_schema.get("version", "1.0.0"),
        "institutions": {institution_id: {"data_models": {}}},
    }

    inst_block = (
        extension.setdefault("institutions", {})
        .setdefault(institution_id, {})
        .setdefault("data_models", {})
    )

    model_list = [models] if isinstance(models, str) else list(models or [])

    # Merge ONLY the inferred extras into the extension
    for model in model_list:
        cols_block = inst_block.setdefault(model, {}).setdefault("columns", {})
        # don’t overwrite anything that might already exist in extension
        for col, spec in inferred.items():
            if col not in cols_block:
                cols_block[col] = spec

    return extension
