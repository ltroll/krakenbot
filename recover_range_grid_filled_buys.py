#!/usr/bin/env python3

"""Safely re-adopt confirmed Kraken buy fills that fell out of bot state."""

import argparse
import fcntl
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from export_kraken_orders import KrakenClient
from range_grid_assets import infer_asset_id_from_pair, kraken_pair_matches
from range_grid_order_safety import (
    atomic_write_json,
    load_json_with_backup,
    order_execution,
)


def unix_to_iso(value):
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def query_orders(client, txids):
    result = client.private(
        "QueryOrders",
        {"txid": ",".join(txids), "trades": "true"},
    )
    missing = [txid for txid in txids if txid not in result]
    if missing:
        raise RuntimeError(
            "Kraken did not return requested orders: " + ", ".join(missing)
        )
    return {txid: result[txid] for txid in txids}


def recovered_buy_state(
    txid,
    order,
    *,
    configured_pair,
    buy_source,
    net_profit_target_pct,
    base_profit_target_pct,
):
    description = order.get("descr") if isinstance(order, dict) else {}
    description = description if isinstance(description, dict) else {}
    if description.get("type") != "buy":
        raise ValueError(f"{txid} is not a buy order")
    asset_id = infer_asset_id_from_pair(configured_pair)
    if not kraken_pair_matches(
        description.get("pair"),
        configured_pair,
        asset_id=asset_id,
    ):
        raise ValueError(
            f"{txid} pair {description.get('pair')!r} does not match "
            f"{configured_pair}"
        )

    execution = order_execution(order)
    try:
        execution_price = float(order.get("price"))
    except (TypeError, ValueError):
        execution_price = execution["average_price"]
    status = str(order.get("status") or "").lower()
    if status not in {"closed", "canceled", "expired"}:
        raise ValueError(f"{txid} is not terminal; Kraken status is {status!r}")
    if execution["executed_volume"] <= 0:
        raise ValueError(f"{txid} has no executed volume")
    if execution_price is None or execution["cost"] is None:
        raise ValueError(f"{txid} is missing execution price or cost")

    base_target = (
        net_profit_target_pct
        if base_profit_target_pct is None
        else base_profit_target_pct
    )
    multiplier = (
        net_profit_target_pct / base_target
        if base_target > 0
        else 1.0
    )
    return {
        "txid": txid,
        "trade_id": txid,
        "client_order_id": order.get("cl_ord_id"),
        "volume": execution["executed_volume"],
        "price": execution_price,
        "buy_cost": execution["cost"],
        "buy_fee": execution["fee"],
        "placed_at": unix_to_iso(order.get("opentm")),
        "filled_at": unix_to_iso(order.get("closetm")),
        "terminal_status": status,
        "sell_pct_override": net_profit_target_pct,
        "base_sell_profit_target_pct": base_target,
        "locked_sell_profit_target_pct": net_profit_target_pct,
        "fear_greed_index_at_target_lock": None,
        "fear_greed_profit_target_multiplier": multiplier,
        "fear_greed_profit_target_reason": "manual_orphan_recovery",
        "fear_greed_profit_target_policy": {},
        "buy_source": buy_source,
        "grid_slot": f"recovered:{txid}",
        "entry_placement_mode": "recovered_fill",
        "operator_controlled": False,
        # The supplied recovery target is an exact operator decision. This
        # prevents later sell repricing from changing the recovered exit.
        "operator_profit_target_override_applied": True,
        "recovery_reason": "confirmed_closed_buy_missing_matching_sell",
    }


def adopt_recovered_buys(state, recovered_orders):
    if not isinstance(state, dict):
        raise ValueError("state must be a JSON object")
    open_buys = state.setdefault("open_buy_orders", {})
    open_sells = state.setdefault("open_sell_orders", {})
    processed_buys = state.setdefault("processed_fills", {}).setdefault(
        "buy",
        {},
    )

    existing_trade_ids = {
        str(order.get("trade_id") or order.get("txid") or "")
        for order in list(open_buys.values()) + list(open_sells.values())
        if isinstance(order, dict)
    }
    adopted = []
    for order in recovered_orders:
        txid = str(order["txid"])
        if txid in existing_trade_ids or txid in processed_buys:
            raise ValueError(f"{txid} is already tracked or processed")
        price = float(order["price"])
        state_key = f"{price:.8f}".rstrip("0").rstrip(".")
        if state_key in open_buys:
            raise ValueError(
                f"state already has an open buy at recovery key {state_key}"
            )
        open_buys[state_key] = dict(order)
        existing_trade_ids.add(txid)
        adopted.append({
            "state_key": state_key,
            "txid": txid,
            "volume": order["volume"],
            "price": order["price"],
            "buy_cost": order["buy_cost"],
            "buy_fee": order["buy_fee"],
            "net_profit_target_pct": order["locked_sell_profit_target_pct"],
        })
    return adopted


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Verify terminal Kraken buy fills and restore them to range-grid "
            "state so the bot can place their matching sells. Dry-run by default."
        )
    )
    parser.add_argument("--txid", action="append", required=True)
    parser.add_argument("--net-profit-target-pct", type=float, required=True)
    parser.add_argument("--base-profit-target-pct", type=float)
    parser.add_argument("--buy-source", default="range_median")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if not 0 <= args.net_profit_target_pct <= 1:
        raise ValueError("--net-profit-target-pct must be between 0 and 1")
    if (
        args.base_profit_target_pct is not None
        and not 0 <= args.base_profit_target_pct <= 1
    ):
        raise ValueError("--base-profit-target-pct must be between 0 and 1")

    load_dotenv(args.env_file, override=True)
    configured_pair = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
    state_file = os.getenv(
        "RANGE_GRID_STATE_FILE",
        os.getenv("BOT_STATE_FILE", "last_state.json"),
    )
    backup_file = os.getenv(
        "RANGE_GRID_STATE_BACKUP_FILE",
        f"{state_file}.bak",
    )
    lock_file = os.getenv(
        "RANGE_GRID_LOCK_FILE",
        os.getenv("BOT_LOCK_FILE", f"{state_file}.lock"),
    )

    client = KrakenClient(os.getenv("KRAKEN_API_URL"))
    kraken_orders = query_orders(client, args.txid)
    recovered = [
        recovered_buy_state(
            txid,
            kraken_orders[txid],
            configured_pair=configured_pair,
            buy_source=args.buy_source,
            net_profit_target_pct=args.net_profit_target_pct,
            base_profit_target_pct=args.base_profit_target_pct,
        )
        for txid in args.txid
    ]

    lock_handle = None
    try:
        if args.apply:
            lock_handle = open(lock_file, "a+", encoding="utf-8")
            try:
                fcntl.flock(
                    lock_handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError as exc:
                raise RuntimeError(
                    "range-grid bot is running; stop its service before --apply"
                ) from exc

        state, source, recovery_errors = load_json_with_backup(
            state_file,
            backup_file,
        )
        if state is None:
            raise RuntimeError(f"state file not found: {state_file}")
        adopted = adopt_recovered_buys(state, recovered)
        result = {
            "mode": "apply" if args.apply else "dry_run",
            "state_file": os.path.abspath(state_file),
            "state_source": source,
            "state_recovery_errors": recovery_errors,
            "adopted": adopted,
        }
        if args.apply:
            atomic_write_json(state_file, state, backup_file)
        print(json.dumps(result, indent=2))
    finally:
        if lock_handle is not None:
            lock_handle.close()


if __name__ == "__main__":
    main()
