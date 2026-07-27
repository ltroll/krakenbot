def normalize_kraken_pair(pair):
    return str(pair or "").upper().replace("/", "").replace("-", "")


def infer_asset_id_from_pair(pair):
    normalized = normalize_kraken_pair(pair)
    if "ETH" in normalized or "XETH" in normalized:
        return "ETH"
    if "SOL" in normalized:
        return "SOL"
    if "XBT" in normalized or "BTC" in normalized:
        return "BTC"
    return ""


def default_asset_balance_keys(asset_id):
    normalized = str(asset_id or "").upper()
    if normalized == "BTC":
        return ["XXBT", "XBT", "BTC"]
    if normalized == "ETH":
        return ["XETH", "ETH"]
    if normalized == "SOL":
        return ["SOL"]
    return [normalized] if normalized else []


def parse_asset_balance_keys(raw_value, asset_id):
    configured = [
        key.strip()
        for key in str(raw_value or "").split(",")
        if key.strip()
    ]
    return configured or default_asset_balance_keys(asset_id)


def asset_balance_from_kraken(balance_result, balance_keys):
    if not isinstance(balance_result, dict):
        return None

    for key in balance_keys:
        try:
            value = float(balance_result.get(key))
        except (TypeError, ValueError):
            continue
        if value >= 0:
            return value

    return None


def _pair_tokens_for_asset(asset_id):
    normalized = str(asset_id or "").upper()
    if normalized == "BTC":
        return ("XBT", "BTC", "XXBT")
    if normalized == "ETH":
        return ("ETH", "XETH")
    if normalized == "SOL":
        return ("SOL",)
    return (normalized,) if normalized else ()


def kraken_pair_matches(pair, configured_pair, asset_id=None):
    normalized_pair = normalize_kraken_pair(pair)
    normalized_configured = normalize_kraken_pair(configured_pair)
    if normalized_pair == normalized_configured:
        return True

    normalized_asset_id = str(asset_id or infer_asset_id_from_pair(configured_pair)).upper()
    if not normalized_asset_id:
        return False

    quote_matches = "USD" in normalized_pair or "ZUSD" in normalized_pair
    if not quote_matches:
        return False

    return any(token in normalized_pair for token in _pair_tokens_for_asset(normalized_asset_id))


def kraken_result_for_pair(result, configured_pair, asset_id=None):
    if not isinstance(result, dict):
        return None

    normalized_configured = normalize_kraken_pair(configured_pair)
    for key, value in result.items():
        if normalize_kraken_pair(key) == normalized_configured:
            return value

    for key, value in result.items():
        if kraken_pair_matches(key, configured_pair, asset_id=asset_id):
            return value

    return None
