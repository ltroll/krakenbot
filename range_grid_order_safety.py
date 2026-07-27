import json
import os
import tempfile


class StateRecoveryError(RuntimeError):
    pass


def _fsync_directory(path):
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        directory_fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_json_temp(directory, payload, prefix):
    fd, temp_path = tempfile.mkstemp(prefix=prefix, suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        return temp_path
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def atomic_write_json(path, payload, backup_path=None):
    target_path = os.path.abspath(path)
    directory = os.path.dirname(target_path) or "."
    os.makedirs(directory, exist_ok=True)
    backup_path = os.path.abspath(backup_path or f"{target_path}.bak")
    temp_path = _write_json_temp(directory, payload, ".range-grid-state-")

    try:
        if os.path.exists(target_path):
            try:
                with open(target_path, "r", encoding="utf-8") as handle:
                    current_payload = json.load(handle)
            except (OSError, ValueError, TypeError):
                current_payload = None

            if current_payload is not None:
                backup_directory = os.path.dirname(backup_path) or "."
                os.makedirs(backup_directory, exist_ok=True)
                backup_temp = _write_json_temp(
                    backup_directory,
                    current_payload,
                    ".range-grid-state-backup-",
                )
                try:
                    os.replace(backup_temp, backup_path)
                    _fsync_directory(backup_directory)
                finally:
                    if os.path.exists(backup_temp):
                        os.unlink(backup_temp)

        os.replace(temp_path, target_path)
        _fsync_directory(directory)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def load_json_with_backup(path, backup_path=None):
    target_path = os.path.abspath(path)
    backup_path = os.path.abspath(backup_path or f"{target_path}.bak")
    errors = []

    for source, candidate in (("primary", target_path), ("backup", backup_path)):
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                raise ValueError("state payload must be a JSON object")
            return payload, source, errors
        except (OSError, ValueError, TypeError) as exc:
            errors.append({"source": source, "path": candidate, "error": str(exc)})

    if errors:
        raise StateRecoveryError(
            "Unable to load range-grid state or backup: "
            + "; ".join(
                f"{entry['source']}={entry['error']}" for entry in errors
            )
        )
    return None, None, []


def numeric_value(value, default=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed


def order_execution(order, fallback_volume=None, fallback_price=None):
    order = order if isinstance(order, dict) else {}
    order_volume = numeric_value(order.get("vol"), fallback_volume)
    executed_volume = max(0.0, numeric_value(order.get("vol_exec"), 0.0) or 0.0)
    if order_volume is None:
        order_volume = max(executed_volume, numeric_value(fallback_volume, 0.0) or 0.0)
    order_volume = max(0.0, order_volume)
    executed_volume = min(executed_volume, order_volume) if order_volume else executed_volume

    cost = numeric_value(order.get("cost"), None)
    fee = max(0.0, numeric_value(order.get("fee"), 0.0) or 0.0)
    average_price = numeric_value(order.get("price"), None)
    if executed_volume > 0 and cost is not None and cost > 0:
        average_price = cost / executed_volume
    if average_price is None:
        average_price = numeric_value(fallback_price, None)
    if cost is None and executed_volume > 0 and average_price is not None:
        cost = executed_volume * average_price

    return {
        "status": order.get("status"),
        "order_volume": order_volume,
        "executed_volume": executed_volume,
        "remaining_volume": max(0.0, order_volume - executed_volume),
        "average_price": average_price,
        "cost": cost,
        "fee": fee,
    }


def order_limit_price(order, fallback=None):
    order = order if isinstance(order, dict) else {}
    description = order.get("descr")
    if isinstance(description, dict):
        value = numeric_value(description.get("price"), None)
        if value is not None:
            return value
    return numeric_value(order.get("limitprice"), numeric_value(fallback, None))


def allocate_position_costs(
    original_volume,
    executed_volume,
    total_cost=None,
    total_fee=0.0,
):
    original_volume = max(0.0, numeric_value(original_volume, 0.0) or 0.0)
    executed_volume = max(0.0, numeric_value(executed_volume, 0.0) or 0.0)
    ratio = (
        min(1.0, executed_volume / original_volume)
        if original_volume > 0
        else 0.0
    )
    total_cost = numeric_value(total_cost, None)
    total_fee = max(0.0, numeric_value(total_fee, 0.0) or 0.0)
    executed_cost = total_cost * ratio if total_cost is not None else None
    executed_fee = total_fee * ratio
    return {
        "execution_ratio": ratio,
        "executed_cost": executed_cost,
        "executed_fee": executed_fee,
        "remaining_cost": (
            max(0.0, total_cost - executed_cost)
            if total_cost is not None
            else None
        ),
        "remaining_fee": max(0.0, total_fee - executed_fee),
    }
