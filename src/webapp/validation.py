"""File validation functions for various schemas.
Record-by-record validation happens in the pipelines; this module performs
general file validation with performance-focused improvements.

Key speed-ups (without losing accuracy):
- Header-only pass to discover/resolve columns before full load
- Selective, typed CSV read via `usecols` and dtype mapping
- Exact-name Pandera schemas (avoid regex column matching)
- Fuzzy matching only for unresolved headers; use rapidfuzz if available
- Precompiled regexes and set-based membership checks inside Pandera checks
"""

from __future__ import annotations

import io
import os
import json
import re
import logging
from functools import lru_cache
from typing import Union, List, Dict, Optional, Any, BinaryIO, cast, Tuple

import pandas as pd
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaErrors

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Public entry points
# --------------------------------------------------------------------------- #


def validate_file_reader(
    filename: Union[str, os.PathLike[str], BinaryIO, io.TextIOWrapper],
    allowed_schema: list[str],
    base_schema: dict,
    inst_schema: Optional[Dict[Any, Any]] = None,
) -> dict[str, Any]:
    """Validates a dataset given a filename and schema selection."""
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


# --------------------------------------------------------------------------- #
# Utilities
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=4096)
def normalize_col(name: str) -> str:
    """Normalize a column name: trim, lowercase, non-alnum->'_', collapse '_'s."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def load_json(path: str) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        raise FileNotFoundError(f"Failed to load JSON schema at {path}: {e}") from e


def merge_model_columns(
    base_schema: dict,
    extension_schema: Any,
    institution: str,
    model: str,
) -> Dict[str, dict]:
    """
    Merge base model columns with institution-specific extension, if present.
    """
    base_models = base_schema.get("base", {}).get("data_models", {})
    if model not in base_models:
        logger.error("Model '%s' not found in base schema", model)
        raise KeyError(f"Model '{model}' not in base schema")
    merged = dict(base_models[model].get("columns", {}))
    if extension_schema:
        inst_block = extension_schema.get("institutions", {}).get(institution, {})
        ext_models = inst_block.get("data_models", {})
        if model in ext_models:
            merged.update(ext_models[model].get("columns", {}))
    return merged


# --------------------------------------------------------------------------- #
# Encoding sniffing (mypy-friendly)
# --------------------------------------------------------------------------- #

Src = Union[str, os.PathLike[str], BinaryIO, io.TextIOWrapper]


def _read_sample(buf: BinaryIO, n: int) -> bytes:
    pos = buf.tell() if buf.seekable() else None
    chunk = buf.read(n)
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


def _reset_to_start_if_possible(src: Src) -> None:
    """Best-effort reset to the beginning for file-like objects."""
    try:
        if hasattr(src, "seek") and callable(getattr(src, "seek")):
            src.seek(0)  # type: ignore[attr-defined]
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Fast header pass & mapping
# --------------------------------------------------------------------------- #


def _spec_alias_lookup(
    merged_specs: Dict[str, dict],
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    Build:
      - alias2canon: normalized alias -> canonical
      - canon_to_aliases_norm: canonical -> list of normalized aliases (incl. canonical)
    """
    alias2canon: Dict[str, str] = {}
    canon_to_aliases_norm: Dict[str, List[str]] = {}
    for canon, spec in merged_specs.items():
        aliases = [canon] + spec.get("aliases", [])
        normed = [normalize_col(a) for a in aliases]
        canon_to_aliases_norm[canon] = normed
        for a in normed:
            alias2canon[a] = canon
    return alias2canon, canon_to_aliases_norm


def _fuzzy_map_unresolved(
    unresolved: List[Tuple[str, str]],  # [(raw_header, normalized_header)]
    choices: List[str],  # normalized aliases
    alias2canon: Dict[str, str],
    threshold: int = 90,
) -> Dict[str, str]:  # raw_header -> canonical
    """
    Fuzzy-match only the unresolved headers, using RapidFuzz if available, otherwise thefuzz.
    """
    mapping: Dict[str, str] = {}
    try:
        from rapidfuzz import process, fuzz as rf_fuzz  # type: ignore

        for raw, norm in unresolved:
            hit = process.extractOne(
                norm, choices, scorer=rf_fuzz.ratio, score_cutoff=threshold
            )
            if hit:
                best_alias, score, _ = hit
                mapping[raw] = alias2canon[best_alias]  # type: ignore[index]
    except Exception:
        # fallback to thefuzz if rapidfuzz is unavailable
        try:
            from thefuzz import fuzz as tf_fuzz  # type: ignore
        except Exception:
            # If neither library is available, do not fuzz-map anything.
            return mapping
        for raw, norm in unresolved:
            best_score = 0
            best_alias = None
            for alias in choices:
                s = tf_fuzz.ratio(norm, alias)
                if s > best_score:
                    best_score, best_alias = s, alias
            if best_alias and best_score >= threshold:
                mapping[raw] = alias2canon[best_alias]
    return mapping


def _header_pass(
    filename: Src,
    encoding: str,
    merged_specs: Dict[str, dict],
    fuzzy_threshold: int = 90,
) -> Tuple[List[str], Dict[str, str], List[str], List[str], List[str]]:
    """
    Read only the header. Return:
      - raw_cols: list of column names as in file
      - raw_to_canon: mapping raw header -> canonical (after exact+fuzzy)
      - missing_required: list of canonical columns missing
      - missing_optional: list of optional canonical columns missing
      - unknown_extra: normalized headers that don't map to any alias
    """
    header_df = pd.read_csv(filename, encoding=encoding, nrows=0)
    raw_cols = list(header_df.columns)

    alias2canon, canon_to_aliases_norm = _spec_alias_lookup(merged_specs)
    known_aliases = set(alias2canon.keys())

    # exact (normalized) mapping first
    raw_to_canon: Dict[str, str] = {}
    unresolved: List[Tuple[str, str]] = []

    for raw in raw_cols:
        norm = normalize_col(raw)
        if norm in alias2canon:
            raw_to_canon[raw] = alias2canon[norm]
        else:
            unresolved.append((raw, norm))

    # fuzzy match only for unresolved headers
    if unresolved:
        choices = list(known_aliases)
        fuzzy_map = _fuzzy_map_unresolved(
            unresolved, choices, alias2canon, threshold=fuzzy_threshold
        )
        raw_to_canon.update(fuzzy_map)

    incoming_canons = set(raw_to_canon.values())
    missing_required = [
        c
        for c, spec in merged_specs.items()
        if spec.get("required", False) and c not in incoming_canons
    ]
    missing_optional = [
        c
        for c, spec in merged_specs.items()
        if not spec.get("required", False) and c not in incoming_canons
    ]
    # normalized headers that remain unmapped and aren't known aliases
    unknown_extra = sorted(
        {norm for (_, norm) in unresolved if norm not in known_aliases}
    )

    return raw_cols, raw_to_canon, missing_required, missing_optional, unknown_extra


def _pandas_dtype_and_parse_dates(
    merged_specs: Dict[str, dict],
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Conservative mapping from spec dtype -> pandas read_csv dtype/parse_dates.
    Keeps behavior stable while avoiding heavy inference.
    """
    dtype_map: Dict[str, Any] = {}
    parse_dates: List[str] = []

    for canon, spec in merged_specs.items():
        dt = str(spec.get("dtype"))
        if dt in {"string", "str", "object"}:
            dtype_map[canon] = "string"
        elif dt in {"int", "int64", "Int64"}:
            dtype_map[canon] = "Int64"  # nullable integers are safer for dirty data
        elif dt in {"float", "float64"}:
            dtype_map[canon] = "float64"
        elif "datetime" in dt or "date" in dt:
            parse_dates.append(canon)
        elif dt in {"bool", "boolean"}:
            dtype_map[canon] = "boolean"
        elif dt == "category":
            dtype_map[canon] = "category"
        else:
            # leave to pandas inference
            pass

    return dtype_map, parse_dates


def _build_exact_schema(
    specs: Dict[str, dict], only_canons: List[str]
) -> DataFrameSchema:
    """
    Build a Pandera schema with exact column names (no regex).
    This avoids regex matching overhead during validation.
    """
    cols: Dict[str, Column] = {}
    for canon in only_canons:
        spec = specs[canon]
        checks = []
        for chk in spec.get("checks", []):
            args = list(chk.get("args", []))
            # precompile regex patterns once
            if (
                chk["type"] in {"str_matches", "matches"}
                and args
                and isinstance(args[0], str)
            ):
                args[0] = re.compile(args[0])
            # set-based membership for faster 'isin'
            if chk["type"] in {"isin", "is_in"} and args and isinstance(args[0], list):
                args[0] = set(args[0])

            factory = getattr(Check, chk["type"])
            checks.append(factory(*args, **chk.get("kwargs", {})))

        cols[canon] = Column(
            name=canon,
            regex=False,
            dtype=spec["dtype"],
            nullable=spec["nullable"],
            required=True,  # present-by-construction
            checks=checks or None,
            coerce=spec.get("coerce", False),
        )
    return DataFrameSchema(cols, strict=False)


# --------------------------------------------------------------------------- #
# Main validation
# --------------------------------------------------------------------------- #


def validate_dataset(
    filename: Src,
    base_schema: dict,
    ext_schema: Optional[Dict[Any, Any]] = None,
    models: Union[str, List[str], None] = None,
    institution_id: str = "pdp",
) -> Dict[str, Any]:
    """
    Validate a dataset against merged base/extension schemas.

    Steps:
      1) Detect encoding (BOM/UTF-8)
      2) Merge requested models' column specs
      3) Header-only pass to map columns (exact + fuzzy) and detect missing/extra
      4) Selective, typed read via pandas (skip unused columns)
      5) Fail-fast validation for required columns; collect soft errors for optional
    """
    # --- 1) encoding ---
    try:
        enc = sniff_encoding(filename)  # latin-1 NOT allowed by default
    except UnicodeError as ex:
        raise HardValidationError(schema_errors="decode_error", failure_cases=[str(ex)])

    # Ensure both header and full reads start at the beginning for file-like handles
    _reset_to_start_if_possible(filename)

    # --- 2) merge requested models ---
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

    # --- 3) HEADER-ONLY PASS ---
    raw_cols, raw_to_canon, missing_required, missing_optional, unknown_extra = (
        _header_pass(filename, enc, merged_specs, fuzzy_threshold=90)
    )

    if missing_required:
        logger.error("Missing required columns: %s", missing_required)
        raise HardValidationError(missing_required=missing_required)

    # Reset again before the real read (important for file-like objects)
    _reset_to_start_if_possible(filename)

    # Choose one raw header per canonical; prefer exact canonical names when available
    canon_to_raw: Dict[str, str] = {}
    for raw, canon in raw_to_canon.items():
        # Prefer if normalized raw equals canonical name
        if canon not in canon_to_raw or normalize_col(raw) == canon:
            canon_to_raw[canon] = raw

    present_canons = sorted(canon_to_raw.keys())
    raw_usecols = list(canon_to_raw.values())

    # dtype & parse_dates maps (by canonical) -> convert to raw keys for read_csv
    canon_dtype_map, parse_dates_canons = _pandas_dtype_and_parse_dates(merged_specs)
    raw_dtype_map = {
        canon_to_raw[c]: dt for c, dt in canon_dtype_map.items() if c in canon_to_raw
    }
    parse_dates_raw = [canon_to_raw[c] for c in parse_dates_canons if c in canon_to_raw]

    # --- 4) Selective, typed read ---
    # Default to fast C engine; try pyarrow if available.
    engine = "c"
    try:
        import pyarrow  # noqa: F401
        engine = "pyarrow"
    except Exception:
        pass

    read_kwargs: Dict[str, Any] = dict(
        encoding=enc,
        usecols=raw_usecols,
        dtype=raw_dtype_map or None,
        engine=engine,
    )
    # memory_map works for path-like with the C engine
    if engine == "c" and isinstance(filename, (str, os.PathLike)):
        read_kwargs["memory_map"] = True
        # only C engine supports parse_dates consistently across versions
        if parse_dates_raw:
            read_kwargs["parse_dates"] = parse_dates_raw

    df = pd.read_csv(
        filename, **{k: v for k, v in read_kwargs.items() if v is not None}
    )

    # If we used the pyarrow engine, perform datetime parsing post-read (keeps accuracy)
    if engine == "pyarrow" and parse_dates_canons:
        for canon in parse_dates_canons:
            raw = str(canon_to_raw.get(canon))
            if raw and raw in df.columns:
                # coerce invalids to NaT; Pandera will flag according to nullability/checks
                df[raw] = pd.to_datetime(df[raw], errors="coerce")

    # Rename raw headers -> canonical names exactly once
    df = df.rename(columns={raw: canon for canon, raw in canon_to_raw.items()})

    # --- 5) Validation: required fail-fast, optional lazy (collect soft errors) ---
    required_canons = [
        c for c in present_canons if merged_specs[c].get("required", False)
    ]
    optional_canons = [
        c for c in present_canons if not merged_specs[c].get("required", False)
    ]

    # Build exact-name schemas (faster than regex)
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
            # Columns are canonical already, so failure_cases['column'] are canonical names
            opt_failures = sorted(set(err.failure_cases["column"]))
            failure_cases_records = err.failure_cases.to_dict(orient="records")

    logger.info("missing_optional = %s", missing_optional)

    # Success (with potential soft issues)
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
