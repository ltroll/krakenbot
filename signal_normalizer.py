import os


def infer_asset_id_from_pair(pair):
    pair = (pair or "").upper()
    if "ETH" in pair or "XETH" in pair:
        return "ETH"
    if "SOL" in pair:
        return "SOL"
    if "XBT" in pair or "BTC" in pair:
        return "BTC"
    return "BTC"


def selected_signal_asset_id(pair=None, explicit_asset_id=None):
    return (
        explicit_asset_id
        or os.getenv("LLM_TARGET_ASSET_ID")
        or os.getenv("SENTIMENT_ASSET_ID")
        or os.getenv("SIGNAL_ASSET_ID")
        or os.getenv("ASSET_ID")
        or infer_asset_id_from_pair(pair)
    ).upper()


def select_asset_signal(signal, asset_id=None, pair=None):
    if not isinstance(signal, dict):
        return signal

    assets = signal.get("assets")
    if not isinstance(assets, dict):
        return signal

    selected_asset_id = selected_signal_asset_id(pair=pair, explicit_asset_id=asset_id)
    selected = assets.get(selected_asset_id)
    if not isinstance(selected, dict):
        return {}

    result = dict(selected)
    result.setdefault("processed_at", signal.get("processed_at"))
    result.setdefault("freshness", signal.get("freshness"))
    result.setdefault("schema_version", signal.get("single_asset_schema_version"))
    result.setdefault("multi_asset_schema_version", signal.get("schema_version"))
    result.setdefault("asset_id", selected_asset_id)
    return result


def normalize_price_regime(price_regime):
    if not isinstance(price_regime, dict):
        return {}

    normalized = dict(price_regime)
    aliases = {
        "range_position": "range_position_24h",
        "realized_volatility_pct": "realized_volatility_24h_pct",
        "price_high": "price_high_24h",
        "price_low": "price_low_24h",
        "price_mean": "price_mean_24h",
        "price_median": "price_median_24h",
        "price_return_24h_pct": "return_24h_pct",
    }
    for source, target in aliases.items():
        if target not in normalized and source in normalized:
            normalized[target] = normalized[source]
    return normalized


def normalize_market_structure(market_structure):
    if not isinstance(market_structure, dict):
        return {}

    normalized = dict(market_structure)
    aliases = {
        "nearest_support": "support_price",
        "support": "support_price",
        "nearest_resistance": "resistance_price",
        "resistance": "resistance_price",
        "upside_pct": "upside_to_resistance_pct",
        "resistance_distance_pct": "upside_to_resistance_pct",
        "downside_pct": "downside_to_support_pct",
        "support_distance_pct": "downside_to_support_pct",
        "risk_reward": "risk_reward_to_structure",
        "range_position": "range_position_24h",
    }
    for source, target in aliases.items():
        if target not in normalized and source in normalized:
            normalized[target] = normalized[source]
    return normalized


def normalize_source_status(source_status):
    if not isinstance(source_status, dict):
        return {}

    normalized = dict(source_status)
    if "asset_price" in normalized and "market_data" not in normalized:
        normalized["market_data"] = normalized["asset_price"]
    if "asset_price_regime" in normalized and "price_regime" not in normalized:
        normalized["price_regime"] = normalized["asset_price_regime"]
    return normalized


def normalize_signal_payload(signal, asset_id=None, pair=None):
    if not isinstance(signal, dict):
        return {
            "execution_signal": float(signal),
            "confidence": 1.0,
            "target_prices": []
        }

    signal = select_asset_signal(signal, asset_id=asset_id, pair=pair)
    if not isinstance(signal, dict):
        return {
            "execution_signal": 0.0,
            "confidence": 0.0,
            "target_prices": []
        }

    source_status = normalize_source_status(signal.get("source_status"))

    action_policy = signal.get("action_policy")
    if not isinstance(action_policy, dict):
        action_policy = {}

    risk_context = signal.get("risk_context")
    if not isinstance(risk_context, dict):
        risk_context = {}

    active_strategy = signal.get("active_strategy")
    if not isinstance(active_strategy, dict):
        active_strategy = {}

    price_regime = signal.get("price_regime")
    if not isinstance(price_regime, dict):
        price_regime = signal.get("asset_price_regime")
    price_regime = normalize_price_regime(price_regime)

    market_structure = signal.get("market_structure")
    if not isinstance(market_structure, dict):
        market_structure = signal.get("asset_market_structure")
    market_structure = normalize_market_structure(market_structure)

    target_prices = signal.get("target_prices")
    if not isinstance(target_prices, list):
        target_prices = []

    asset = signal.get("asset")
    if not isinstance(asset, dict):
        asset = {}

    asset_price = signal.get("asset_price")

    return {
        "asset_id": signal.get("asset_id"),
        "asset_symbol": asset.get("symbol") or signal.get("asset_id"),
        "asset_name": asset.get("name"),
        "asset_price": asset_price,
        "asset_sentiment": signal.get("asset_sentiment"),
        "btc_price": signal.get("btc_price", asset_price),
        "btc_sentiment": signal.get("btc_sentiment", signal.get("asset_sentiment")),
        "regulatory_risk": signal.get("regulatory_risk"),
        "macro_tightening_bias": signal.get("macro_tightening_bias"),
        "execution_signal": float(signal.get("execution_signal", 0)),
        "confidence": float(signal.get("confidence", 0)),
        "direction_bias": signal.get("direction_bias"),
        "risk_multiplier": signal.get("risk_multiplier"),
        "smoothed_risk_multiplier": signal.get("smoothed_risk_multiplier"),
        "liquidity_risk": signal.get("liquidity_risk"),
        "btc_relative_strength": signal.get("btc_relative_strength"),
        "eth_relative_strength": signal.get("eth_relative_strength"),
        "mean_reversion_opportunity": signal.get("mean_reversion_opportunity"),
        "flow_pressure": signal.get("flow_pressure"),
        "market_interpretation": signal.get("market_interpretation"),
        "signal_utility": signal.get("signal_utility"),
        "asset_price_record": signal.get("asset_price_record"),
        "asset_pipeline": signal.get("asset_pipeline"),
        "raw_btc_sentiment": signal.get(
            "raw_btc_sentiment",
            signal.get("asset_sentiment")
        ),
        "raw_confidence": signal.get("raw_confidence"),
        "raw_direction_bias": signal.get("raw_direction_bias"),
        "fear_greed_index": signal.get("fear_greed_index"),
        "signal_status": signal.get("signal_status"),
        "bot_action_allowed": signal.get("bot_action_allowed"),
        "action_recommendation": signal.get("action_recommendation"),
        "action_policy": action_policy,
        "risk_context": risk_context,
        "active_strategy": active_strategy,
        "contributor_count": signal.get("contributor_count"),
        "active_observation_count": signal.get("active_observation_count"),
        "reason": signal.get("reason"),
        "processed_at": signal.get("processed_at"),
        "freshness": signal.get("freshness")
        if isinstance(signal.get("freshness"), dict)
        else {},
        "schema_version": signal.get("schema_version"),
        "multi_asset_schema_version": signal.get("multi_asset_schema_version"),
        "price_regime": price_regime,
        "market_structure": market_structure,
        "asset_price_regime": signal.get("asset_price_regime"),
        "source_status": source_status,
        "target_prices": target_prices,
    }
