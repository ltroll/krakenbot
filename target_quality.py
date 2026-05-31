#!/usr/bin/env python3

import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests


TARGET_QUALITY_MATCH_TOLERANCE_PCT = 0.0005


def parse_iso8601(value):
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed


def normalize_profit_target_pct(raw_value):
    if raw_value is None:
        return None

    try:
        return float(raw_value) / 100.0
    except Exception:
        return None


def quality_source_is_url(source):
    if not source:
        return False

    parsed = urlparse(str(source))
    return parsed.scheme in ("http", "https")


def load_target_quality_snapshot(source, max_age_minutes, now=None, timeout=5):
    now = now or datetime.now(timezone.utc)

    if not source:
        return {
            "available": False,
            "reason": "missing_target_quality_source",
            "targets": []
        }

    try:
        if quality_source_is_url(source):
            response = requests.get(source, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        else:
            if not os.path.exists(source):
                return {
                    "available": False,
                    "reason": "target_quality_missing",
                    "targets": []
                }
            with open(source, encoding="utf-8") as f:
                payload = json.load(f)
    except Exception as exc:
        return {
            "available": False,
            "reason": "target_quality_unavailable",
            "error": str(exc),
            "targets": []
        }

    if not isinstance(payload, dict):
        return {
            "available": False,
            "reason": "target_quality_invalid_payload",
            "targets": []
        }

    if payload.get("status") != "ok":
        return {
            "available": False,
            "reason": "target_quality_status_not_ok",
            "status": payload.get("status"),
            "targets": []
        }

    snapshot_time = parse_iso8601(payload.get("timestamp"))
    if snapshot_time is None:
        return {
            "available": False,
            "reason": "target_quality_missing_timestamp",
            "targets": []
        }

    age_minutes = (now - snapshot_time).total_seconds() / 60.0
    if age_minutes > max_age_minutes:
        return {
            "available": False,
            "reason": "target_quality_stale",
            "age_minutes": age_minutes,
            "targets": []
        }

    targets = payload.get("targets")
    if not isinstance(targets, list):
        return {
            "available": False,
            "reason": "target_quality_missing_targets",
            "targets": []
        }

    return {
        "available": True,
        "reason": None,
        "payload": payload,
        "timestamp": snapshot_time.isoformat(),
        "age_minutes": age_minutes,
        "targets": targets
    }


def match_quality_target(
    candidate_buy_price,
    targets,
    tolerance_pct=TARGET_QUALITY_MATCH_TOLERANCE_PCT
):
    try:
        candidate_buy_price = float(candidate_buy_price)
    except Exception:
        return None

    if candidate_buy_price <= 0:
        return None

    valid_targets = []
    for target in targets or []:
        if not isinstance(target, dict):
            continue
        try:
            buy_price = float(target.get("buy_price"))
        except Exception:
            continue

        valid_targets.append((buy_price, target))

    if not valid_targets:
        return None

    exact_matches = [
        target
        for buy_price, target in valid_targets
        if round(buy_price, 2) == round(candidate_buy_price, 2)
    ]
    if exact_matches:
        return min(
            exact_matches,
            key=lambda target: abs(float(target["buy_price"]) - candidate_buy_price)
        )

    nearest_buy_price, nearest_target = min(
        valid_targets,
        key=lambda item: abs(item[0] - candidate_buy_price)
    )
    distance_pct = abs(nearest_buy_price - candidate_buy_price) / candidate_buy_price
    if distance_pct <= tolerance_pct:
        return nearest_target

    return None


def evaluate_quality_target(
    matched_target,
    *,
    min_samples,
    min_ev_pct,
    min_4h_fill_probability,
    allowed_recommendations
):
    if not isinstance(matched_target, dict):
        return {
            "allowed": False,
            "reason": "no_matching_target_quality",
            "matched_buy_price": None,
            "recommendation": None,
            "matched_sample_count": None,
            "fill_probability_4h": None,
            "best_expected_value_pct_per_signal": None,
            "best_profit_target_pct": None,
        }

    recommendation = matched_target.get("recommendation")
    matched_sample_count = matched_target.get("matched_sample_count")
    best_ev = matched_target.get("best_expected_value_pct_per_signal")
    best_profit_target_pct = matched_target.get("best_profit_target_pct")
    fill_prob_4h = (
        matched_target.get("fill_probability", {})
        .get("4h", {})
        .get("fill_probability")
    )

    try:
        matched_sample_count = int(matched_sample_count)
    except Exception:
        matched_sample_count = None

    try:
        best_ev = float(best_ev)
    except Exception:
        best_ev = None

    try:
        fill_prob_4h = float(fill_prob_4h)
    except Exception:
        fill_prob_4h = None

    try:
        matched_buy_price = float(matched_target.get("buy_price"))
    except Exception:
        matched_buy_price = None

    allowed_recommendations = {
        str(value).strip()
        for value in allowed_recommendations
        if str(value).strip()
    }

    if matched_sample_count is None or matched_sample_count < min_samples:
        reason = "quality_low_sample_count"
        allowed = False
    elif best_ev is None or best_ev < min_ev_pct:
        reason = "quality_low_expected_value"
        allowed = False
    elif fill_prob_4h is None or fill_prob_4h < min_4h_fill_probability:
        reason = "quality_low_4h_fill_probability"
        allowed = False
    elif recommendation not in allowed_recommendations:
        reason = "quality_recommendation_not_allowed"
        allowed = False
    else:
        reason = "quality_allowed"
        allowed = True

    return {
        "allowed": allowed,
        "reason": reason,
        "matched_buy_price": matched_buy_price,
        "recommendation": recommendation,
        "matched_sample_count": matched_sample_count,
        "fill_probability_4h": fill_prob_4h,
        "best_expected_value_pct_per_signal": best_ev,
        "best_profit_target_pct": best_profit_target_pct,
    }


def unavailable_quality_decision(snapshot, fail_closed):
    if snapshot.get("available"):
        return {
            "allowed": True,
            "reason": "quality_available",
            "blocked": False,
        }

    reason = snapshot.get("reason") or "target_quality_unavailable"
    if fail_closed:
        return {
            "allowed": False,
            "reason": reason,
            "blocked": True,
        }

    return {
        "allowed": True,
        "reason": reason,
        "blocked": False,
    }
