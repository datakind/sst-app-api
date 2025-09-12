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
    name = name.strip("_")  # Remove leading/trailing underscores
    return name


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
    try:
        enc = sniff_encoding(filename)  # latin-1 is NOT allowed by default
    except UnicodeError as ex:
        raise HardValidationError(schema_errors="decode_error", failure_cases=[str(ex)])

    # ensure a file-like starts at beginning, then one real read
    if hasattr(filename, "seek"):
        try:
            filename.seek(0)
        except Exception:
            pass

    df = pd.read_csv(filename, encoding=enc)

    df = df.rename(columns={c: normalize_col(c) for c in df.columns})
    incoming = set(df.columns)

    # 2) merge requested models
    if models is None:
        model_list = []
    elif isinstance(models, str):
        model_list = [models]
    else:
        model_list = list(models)  # <- ensures it's not a set

    merged_specs: Dict[str, dict] = {}
    for m in model_list:
        specs = merge_model_columns(base_schema, ext_schema, institution_id, m.lower())
        merged_specs.update(specs)

    canon_to_aliases = {
        canon: [normalize_col(alias) for alias in [canon] + spec.get("aliases", [])]
        for canon, spec in merged_specs.items()
    }
    df = rename_columns_to_match_schema(df, canon_to_aliases)
    df.columns = [
        normalize_col(c) for c in df.columns
    ]  # Final normalization after renaming

    incoming = set(df.columns)

    # 3) build canon → set(normalized names)
    canon_to_norms: Dict[str, set] = {
        canon: {normalize_col(alias) for alias in [canon] + spec.get("aliases", [])}
        for canon, spec in merged_specs.items()
    }

    pattern_to_canon = {
        r"^(?:"
        + "|".join(map(re.escape, [canon] + spec.get("aliases", [])))
        + r")$": canon
        for canon, spec in merged_specs.items()
    }

    # 4) find extra / missing
    all_norms = set().union(*canon_to_norms.values()) if canon_to_norms else set()
    extra_columns = sorted(incoming - all_norms)

    missing_required = [
        canon
        for canon, norms in canon_to_norms.items()
        if merged_specs[canon].get("required", False) and norms.isdisjoint(incoming)
    ]

    missing_optional = [
        canon
        for canon, norms in canon_to_norms.items()
        if not merged_specs[canon].get("required", False) and norms.isdisjoint(incoming)
    ]

    # Hard-fail on missing required or any extra columns
    if missing_required:
        if logging:
            logging.error(
                f"Missing required or extra columns detected, missing_required = {missing_required}, extra_columns = {extra_columns}"
            )
        raise HardValidationError(missing_required=missing_required)
    unknown_extra = extra_columns

    # 5) build Pandera schema & validate (hard-fail on any error)
    schema = build_schema(merged_specs)
    try:
        schema.validate(df, lazy=True)
    except SchemaErrors as err:
        # TODO: Log validation failure for DS to review
        failed_normals = set(err.failure_cases["column"])
        failed_canons = {pattern_to_canon.get(p, p) for p in failed_normals}

        # split into required vs optional failures
        req_failures = [
            c for c in failed_canons if merged_specs.get(c, {}).get("required", False)
        ]
        opt_failures = [
            c
            for c in failed_canons
            if not merged_specs.get(c, {}).get("required", False)
        ]

        if req_failures:
            if logging:
                logging.error(
                    f"Schema validation failed on required columns, schema_errors = {err.schema_errors}, failure_cases = {err.failure_cases.to_dict(orient='records')}"
                )
            raise HardValidationError(
                schema_errors=err.schema_errors,
                failure_cases=err.failure_cases.to_dict(orient="records"),
            )
        else:
            if logging:
                logging.info(f"missing_optional = {missing_optional}")
            print("Optional column validation errors on: ", opt_failures)
            return {
                "validation_status": "passed_with_soft_errors",
                "schemas": model_list,
                "missing_optional": missing_optional,
                "optional_validation_failures": opt_failures,
                "failure_cases": err.failure_cases.to_dict(orient="records"),
            }
    if logging:
        logging.info(f"missing_optional = {missing_optional}")
    # 6) success (with possible soft misses)
    return {
        "validation_status": (
            "passed_with_soft_errors"
            if (missing_optional or unknown_extra)
            else "passed"
        ),
        "schemas": model_list,
        "missing_optional": missing_optional,
        "unknown_extra_columns": unknown_extra,
    }
