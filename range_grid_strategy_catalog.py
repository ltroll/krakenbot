#!/usr/bin/env python3

"""Discovery and validation for selectable range-grid strategy profiles."""

import json
import os
import re
from pathlib import Path

from range_grid_guardrails import validate_strategy_config


STRATEGY_FILENAME_PATTERN = re.compile(
    r"^range_grid_strategy_[A-Za-z0-9][A-Za-z0-9_.-]*\.json$"
)


def strategy_directory(path=None):
    configured = path or os.getenv("RANGE_GRID_STRATEGY_DIRECTORY")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parent


def normalize_strategy_filename(value):
    if value in (None, ""):
        return None
    filename = str(value).strip()
    if not STRATEGY_FILENAME_PATTERN.fullmatch(filename):
        raise ValueError(
            "strategy profile must be a range_grid_strategy_*.json filename"
        )
    return filename


def strategy_label(filename):
    label = filename.removeprefix("range_grid_strategy_").removesuffix(".json")
    return label.replace("_", " ").strip().title()


def _bool_value(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def strategy_catalog_entry(path):
    filename = path.name
    errors = []
    payload = {}
    try:
        with path.open(encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            errors.append("strategy file must contain a JSON object")
        else:
            payload = loaded
            errors.extend(validate_strategy_config(payload))
    except (OSError, ValueError, TypeError) as exc:
        errors.append(str(exc))

    return {
        "filename": filename,
        "label": strategy_label(filename),
        "valid": not errors,
        "errors": errors,
        "paper_trading_enabled": _bool_value(
            payload.get("paper_trading_enabled"),
            False,
        ),
        "operating_mode": str(
            payload.get("operating_mode", "range_plus_llm")
        ),
        "grid_anchor": str(payload.get("grid_anchor", "low")),
        "range_window_hours": payload.get("range_window_hours"),
        "max_grid_size": payload.get("max_grid_size"),
        "profit_target_pct": payload.get("profit_target_pct"),
        "minimum_order_floor_usd": payload.get("minimum_order_floor_usd"),
    }


def strategy_catalog(path=None):
    directory = strategy_directory(path)
    if not directory.is_dir():
        return []
    return [
        strategy_catalog_entry(profile_path)
        for profile_path in sorted(directory.glob("range_grid_strategy_*.json"))
        if STRATEGY_FILENAME_PATTERN.fullmatch(profile_path.name)
    ]


def validate_strategy_profile_selection(filename, path=None):
    normalized = normalize_strategy_filename(filename)
    if normalized is None:
        return None
    directory = strategy_directory(path)
    profile_path = (directory / normalized).resolve()
    if profile_path.parent != directory:
        raise ValueError("strategy profile must be inside the strategy directory")
    if not profile_path.is_file():
        raise ValueError(f"strategy profile not found: {normalized}")
    entry = strategy_catalog_entry(profile_path)
    if not entry["valid"]:
        raise ValueError(
            f"strategy profile is invalid: {normalized}: "
            + "; ".join(entry["errors"])
        )
    return normalized
