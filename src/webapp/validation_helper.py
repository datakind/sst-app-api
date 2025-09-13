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

logger = logging.getLogger(__name__)


# ---------- normalization is pure; cache it ----------
@lru_cache(maxsize=4096)
def normalize_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def _spec_alias_lookup(
    merged_specs: Dict[str, dict]
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    Build fast lookups:
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
        from rapidfuzz import process, fuzz as rf_fuzz  # much faster

        for raw, norm in unresolved:
            hit = process.extractOne(
                norm, choices, scorer=rf_fuzz.ratio, score_cutoff=threshold
            )
            if hit:
                best_alias, score, _ = hit
                mapping[raw] = alias2canon[best_alias]
    except Exception:
        # fallback to thefuzz if rapidfuzz is unavailable
        from thefuzz import fuzz as tf_fuzz

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
    filename: str,
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
    incoming_norms: List[str] = []

    for raw in raw_cols:
        norm = normalize_col(raw)
        incoming_norms.append(norm)
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

    # derive presence/missing/extras from header only
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
    unknown_extra = sorted({n for (_, n) in unresolved if n not in known_aliases})

    return raw_cols, raw_to_canon, missing_required, missing_optional, unknown_extra


def _pandas_dtype_and_parse_dates(
    merged_specs: Dict[str, dict]
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Best-effort mapping from your spec dtype -> pandas read_csv dtype/parse_dates.
    We keep it conservative to avoid accuracy loss.
    """
    dtype_map: Dict[str, Any] = {}
    parse_dates: List[str] = []

    for canon, spec in merged_specs.items():
        dt = str(spec.get("dtype"))
        # conservative mappings
        if dt in {"string", "str", "object"}:
            dtype_map[canon] = "string"
        elif dt in {"int", "int64", "Int64"}:
            # nullable integers are much safer for dirty data
            dtype_map[canon] = "Int64"
        elif dt in {"float", "float64"}:
            dtype_map[canon] = "float64"
        elif "datetime" in dt or "date" in dt:  # pandera often uses datetime64[ns]
            parse_dates.append(canon)  # let pandas parse as datetime
        elif dt in {"bool", "boolean"}:
            dtype_map[canon] = "boolean"
        elif dt == "category":
            dtype_map[canon] = "category"
        else:
            # leave unmapped types to pandas inference (keeps behavior)
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
            # small speedup opportunities:
            #  - precompile regex patterns for str_matches
            args = list(chk.get("args", []))
            if (
                chk["type"] in {"str_matches", "matches"}
                and args
                and isinstance(args[0], str)
            ):
                args[0] = re.compile(args[0])
            factory = getattr(Check, chk["type"])
            checks.append(factory(*args, **chk.get("kwargs", {})))

        cols[canon] = Column(
            name=canon,
            regex=False,
            dtype=spec["dtype"],
            nullable=spec["nullable"],
            required=True,  # present-by-construction here
            checks=checks or None,
            coerce=spec.get("coerce", False),
        )
    return DataFrameSchema(cols, strict=False)
