"""File validation functions for various schemas. (Record by record validation happens in the
pipelines, this is for general file validation.)
"""

import io
import os
import json
import re
from typing import Union, List, Dict, Optional, Any, BinaryIO, cast
import logging

import pandas as pd
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaErrors
from thefuzz import fuzz
from .validation_helper import (
    _header_pass,
    _pandas_dtype_and_parse_dates,
    _build_exact_schema,
)

logger = logging.getLogger(__name__)


def validate_file_reader(
    filename: str,
    allowed_schema: list[str],
    base_schema: dict,
    inst_schema: Optional[Dict[Any, Any]] = None,
) -> dict[str, Any]:
    """Validates given a filename."""
    return validate_dataset(filename, base_schema, inst_schema, allowed_schema)


class HardValidationError(Exception):
    def __init__(
        self,
        missing_required: Optional[List[str]] = None,
        extra_columns: Optional[List[str]] = None,
        schema_errors: Any = None,
        failure_cases: Any = None,
    ):
        self.missing_required = missing_required or []
        self.extra_columns = extra_columns or []
        self.schema_errors = schema_errors
        self.failure_cases = failure_cases
        parts = []
        if self.missing_required:
            parts.append(f"Missing required columns: {self.missing_required}")
        if self.extra_columns:
            parts.append(f"Unexpected columns: {self.extra_columns}")
        if self.schema_errors is not None:
            parts.append(f"Schema errors: {self.schema_errors}")
        super().__init__("; ".join(parts))


def normalize_col(name: str) -> str:
    name = name.strip().lower()  # Lowercase and trim whitespace
    name = re.sub(r"[^a-z0-9_]", "_", name)  # Replace non-alphanum with underscore
    name = re.sub(r"_+", "_", name)  # Collapse multiple underscores
    return name.strip("_")  # Remove leading/trailing underscores


def load_json(path: str) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        raise FileNotFoundError(f"Failed to load JSON schema at {path}: {e}")


def rename_columns_to_match_schema(
    df: pd.DataFrame,
    canon_to_aliases: Dict[str, List[str]],
    threshold: int = 90,
) -> pd.DataFrame:
    """
    Rename incoming columns using fuzzy match against schema-defined column names and aliases.

    Args:
        df: Incoming dataframe
        canon_to_aliases: Mapping from canonical column names to list of aliases (including the canonical name itself)
        threshold: Fuzzy match score threshold to rename

    Returns:
        A new DataFrame with renamed columns
    """
    from collections import defaultdict

    new_column_names = {}
    log_info = defaultdict(list)

    schema_names = []
    for canon, aliases in canon_to_aliases.items():
        for name in aliases:
            schema_names.append((name, canon))  # (alias_or_name, canonical_name)

    for incoming_col in df.columns:
        best_score = 0
        best_match = None
        best_canon = None

        for schema_col, canon in schema_names:
            score = fuzz.ratio(incoming_col.lower(), schema_col.lower())
            if score > best_score:
                best_score = score
                best_match = schema_col
                best_canon = canon

        if best_score >= threshold and incoming_col != best_canon:
            new_column_names[incoming_col] = best_canon
            log_info[incoming_col].append(
                f"Renamed '{incoming_col}' -> '{best_canon}' (matched on '{best_match}', score={best_score})"
            )

    for k, v in log_info.items():
        logging.info(" | ".join(v))

    return df.rename(columns=new_column_names)


def merge_model_columns(
    base_schema: dict,
    extension_schema: Any,
    institution: str,
    model: str,
) -> Dict[str, dict]:
    base_models = base_schema.get("base", {}).get("data_models", {})
    if model not in base_models:
        if logging:
            logging.error(f"Model '{model}' not found in base schema")
        raise KeyError(f"Model '{model}' not in base schema")
    merged = dict(base_models[model].get("columns", {}))
    if extension_schema:
        inst_block = extension_schema.get("institutions", {}).get(institution, {})
        ext_models = inst_block.get("data_models", {})
        if model in ext_models:
            merged.update(ext_models[model].get("columns", {}))
    return merged


def build_schema(specs: Dict[str, dict]) -> DataFrameSchema:
    columns = {}
    for canon, spec in specs.items():
        names = [canon] + spec.get("aliases", [])
        pattern = r"^(?:" + "|".join(map(re.escape, names)) + r")$"
        checks = []
        for chk in spec.get("checks", []):
            factory = getattr(Check, chk["type"])
            checks.append(factory(*chk.get("args", []), **chk.get("kwargs", {})))

        columns[pattern] = Column(
            name=pattern,
            regex=True,
            dtype=spec["dtype"],
            nullable=spec["nullable"],
            required=spec.get("required", False),
            checks=checks or None,
            coerce=spec.get("coerce", False),
        )
    return DataFrameSchema(columns, strict=False)


# --------------------- Actual Validation Layer ------------------------------

Src = Union[str, os.PathLike[str], BinaryIO, io.TextIOWrapper]


def _read_sample(buf: BinaryIO, n: int) -> bytes:
    pos = buf.tell() if buf.seekable() else None
    chunk = buf.read(n)  # -> bytes for BinaryIO
    if pos is not None:
        buf.seek(pos)
    return chunk


def sniff_encoding(src: Src, sample_bytes: int = 1_048_576) -> str:
    """
    Best-guess encoding via BOM detection + utf-8 trial.
    Works with a filesystem path, a binary stream, or a TextIOWrapper.
    Restores stream position if seekable. Raises if latin-1 would be used (by default).
    """
    # --- read a small binary sample ---
    if isinstance(src, (str, os.PathLike)):
        with open(src, "rb") as f:
            chunk: bytes = f.read(sample_bytes)
    elif isinstance(src, io.TextIOWrapper):
        # Text wrapper => use underlying binary buffer, cast to BinaryIO for mypy
        chunk = _read_sample(cast(BinaryIO, src.buffer), sample_bytes)
    else:
        # Already a binary stream
        chunk = _read_sample(cast(BinaryIO, src), sample_bytes)

    # --- BOMs first ---
    if chunk.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if chunk.startswith(b"\xff\xfe\x00\x00"):
        return "utf-32le"
    if chunk.startswith(b"\x00\x00\xfe\xff"):
        return "utf-32be"
    if chunk.startswith(b"\xff\xfe"):
        return "utf-16le"
    if chunk.startswith(b"\xfe\xff"):
        return "utf-16be"

    # --- utf-8 strict on sample ---
    try:
        chunk.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        raise UnicodeError(
            "file is not UTF-8/UTF-16/UTF-32; please re-export as UTF-8."
        )


def validate_dataset(
    filename: str,
    base_schema: dict,
    ext_schema: Optional[Dict[Any, Any]] = None,
    models: Union[str, List[str], None] = None,
    institution_id: str = "pdp",
) -> Dict[str, Any]:
    # 0) encoding
    try:
        enc = sniff_encoding(filename)  # latin-1 NOT allowed by default
    except UnicodeError as ex:
        raise HardValidationError(schema_errors="decode_error", failure_cases=[str(ex)])

    # 1) merge requested models
    if models is None:
        model_list: List[str] = []
    elif isinstance(models, str):
        model_list = [models]
    else:
        model_list = list(models)

    merged_specs: Dict[str, dict] = {}
    for m in model_list:
        specs = merge_model_columns(base_schema, ext_schema, institution_id, m.lower())
        merged_specs.update(specs)

    if not merged_specs:
        # nothing to validate; short-circuit
        return {
            "validation_status": "passed",
            "schemas": model_list,
            "missing_optional": [],
            "unknown_extra_columns": [],
        }

    # 2) HEADER-ONLY PASS: map columns & find missing/extras cheaply
    raw_cols, raw_to_canon, missing_required, missing_optional, unknown_extra = (
        _header_pass(filename, enc, merged_specs, fuzzy_threshold=90)
    )

    if missing_required:
        logger.error("Missing required columns: %s", missing_required)
        raise HardValidationError(missing_required=missing_required)

    # 3) selective typed load
    present_canons = sorted(set(raw_to_canon.values()))
    # choose one raw column per present canonical
    canon_to_raw: Dict[str, str] = {}
    for raw, canon in raw_to_canon.items():
        # prefer the raw header that's already exactly canonical if present
        if canon not in canon_to_raw or normalize_col(raw) == canon:
            canon_to_raw[canon] = raw

    raw_usecols = list(canon_to_raw.values())

    # dtype & parse_dates maps (by canonical); convert to raw keys for read_csv
    canon_dtype_map, parse_dates_canons = _pandas_dtype_and_parse_dates(merged_specs)
    raw_dtype_map = {
        canon_to_raw[c]: dt for c, dt in canon_dtype_map.items() if c in canon_to_raw
    }
    parse_dates_raw = [canon_to_raw[c] for c in parse_dates_canons if c in canon_to_raw]

    read_kwargs = dict(
        encoding=enc,
        usecols=raw_usecols,
        dtype=raw_dtype_map or None,
        parse_dates=parse_dates_raw or None,
        memory_map=True,  # often helps on local/posix filesystems
        engine="c",  # default fast path; keep behavior stable
    )
    # optional speed-up if pyarrow is available; behavior stays correct
    try:
        import pyarrow  # noqa: F401

        read_kwargs["engine"] = "pyarrow"
        # pandas>=2: dtype_backend speeds strings/ints; ignore if not supported
        try:
            read_kwargs["dtype_backend"] = "pyarrow"
        except TypeError:
            pass
    except Exception:
        pass

    df = pd.read_csv(
        filename, **{k: v for k, v in read_kwargs.items() if v is not None}
    )

    # 4) rename raw headers -> canon once (no DataFrame-wide fuzzy work)
    df = df.rename(columns=canon_to_raw)  # temporarily raw->canon? Not quite.
    # The above renames raw names to canonical because keys are canonical? Fix:
    df = df.rename(columns={raw: canon for canon, raw in canon_to_raw.items()})

    # 5) REQUIRED FIRST (fail-fast), then OPTIONALS (collect soft errors)
    required_canons = [
        c for c in present_canons if merged_specs[c].get("required", False)
    ]
    optional_canons = [
        c for c in present_canons if not merged_specs[c].get("required", False)
    ]

    # Build schemas with exact names only (faster than regex patterns)
    if required_canons:
        req_schema = _build_exact_schema(merged_specs, required_canons)
        try:
            req_schema.validate(df[required_canons], lazy=False)
        except SchemaErrors as err:
            logger.error("Required column validation failed.")
            raise HardValidationError(
                schema_errors=err.schema_errors,
                failure_cases=err.failure_cases.to_dict(orient="records"),
            )

    opt_failures: List[str] = []
    failure_cases_records: List[dict] = []
    if optional_canons:
        opt_schema = _build_exact_schema(merged_specs, optional_canons)
        try:
            opt_schema.validate(df[optional_canons], lazy=True)
        except SchemaErrors as err:
            opt_failures = sorted(set(err.failure_cases["column"]))
            failure_cases_records = err.failure_cases.to_dict(orient="records")

    # 6) return — status depends on soft errors / extras
    if opt_failures or missing_optional or unknown_extra:
        return {
            "validation_status": "passed_with_soft_errors",
            "schemas": model_list,
            "missing_optional": missing_optional,
            "optional_validation_failures": opt_failures,
            "failure_cases": failure_cases_records,
            "unknown_extra_columns": unknown_extra,
        }

    return {
        "validation_status": "passed",
        "schemas": model_list,
        "missing_optional": [],
        "unknown_extra_columns": [],
    }
