"""Deterministic composition and identification of range-grid strategies."""

from copy import deepcopy
import hashlib
import json


EFFECTIVE_STRATEGY_SCHEMA_VERSION = 1
EFFECTIVE_STRATEGY_COMPOSITION_MODE = "deep_route_over_base"


def _require_mapping(value, name):
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a dictionary")


def merge_strategy_configs(base_config, route_config=None):
    """Return a deep merge in which routed values override base values.

    Source-specific maps are merged recursively. This makes the fallback
    behavior explicit instead of relying on individual callers to decide
    whether a missing route field should fall back to the base profile.
    """

    _require_mapping(base_config, "base_config")
    if route_config is None:
        return deepcopy(base_config)
    _require_mapping(route_config, "route_config")

    def merge(base_value, route_value):
        if isinstance(base_value, dict) and isinstance(route_value, dict):
            result = deepcopy(base_value)
            for key, value in route_value.items():
                if key in result:
                    result[key] = merge(result[key], value)
                else:
                    result[key] = deepcopy(value)
            return result
        return deepcopy(route_value)

    return merge(base_config, route_config)


def strategy_config_fingerprint(config):
    """Return a stable fingerprint for a JSON-compatible strategy payload."""

    _require_mapping(config, "config")
    encoded = json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _changed_paths(base_config, route_config):
    paths = []

    def walk(base_value, route_value, prefix):
        if isinstance(route_value, dict):
            base_map = base_value if isinstance(base_value, dict) else {}
            for key in sorted(route_value):
                child = f"{prefix}.{key}" if prefix else str(key)
                walk(base_map.get(key), route_value[key], child)
            return
        if base_value != route_value:
            paths.append(prefix)

    walk(base_config, route_config, "")
    return paths


def resolve_effective_strategy(
    base_config,
    route_config=None,
    *,
    buy_source=None,
    base_label=None,
    route_label=None,
    route_file=None,
):
    """Resolve and describe the exact strategy payload for a buy source."""

    _require_mapping(base_config, "base_config")
    if route_config is not None:
        _require_mapping(route_config, "route_config")

    effective_config = merge_strategy_configs(base_config, route_config)
    route_payload = route_config if route_config is not None else None
    return {
        "schema_version": EFFECTIVE_STRATEGY_SCHEMA_VERSION,
        "composition_mode": EFFECTIVE_STRATEGY_COMPOSITION_MODE,
        "buy_source": buy_source,
        "base_label": base_label,
        "route_label": route_label,
        "route_file": route_file,
        "base_fingerprint": strategy_config_fingerprint(base_config),
        "route_fingerprint": (
            strategy_config_fingerprint(route_payload)
            if route_payload is not None
            else None
        ),
        "effective_fingerprint": strategy_config_fingerprint(effective_config),
        "route_override_paths": (
            _changed_paths(base_config, route_payload)
            if route_payload is not None
            else []
        ),
        "payload": effective_config,
    }
