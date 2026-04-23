import sys

with open('telegram_bot.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: Low fuel alert NO STOP found
target1 = '''    else:
        lines += ["", "❌ No fuel stops found on route.", "Dispatcher has been notified."]
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — {fuel_pct:.0f}% — NO STOP FOUND on route")

    if fuel_pct <= 15 and best_stop:
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* critically low — {fuel_pct:.0f}%")

    result = _send_to_truck(vehicle_name, "\\n".join(lines))
    return result if isinstance(result, dict) else {"truck_group": None, "truck_msg_id": result, "dispatcher_msg_id": None}'''

replace1 = '''    else:
        return {"truck_group": None, "truck_msg_id": None, "dispatcher_msg_id": None}

    if fuel_pct <= 15 and best_stop:
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* critically low — {fuel_pct:.0f}%")

    result = _send_to_truck(vehicle_name, "\\n".join(lines))
    return result if isinstance(result, dict) else {"truck_group": None, "truck_msg_id": result, "dispatcher_msg_id": None}'''

# Fix 2: Emergency alert NO STOP found
target2 = '''    else:
        lines += [
            "❌ *NO FUEL STOPS found within range.*",
            f"Range remaining: ~{range_miles:.0f} miles",
            "Dispatcher has been notified — immediate assistance needed.",
        ]

    # Always notify dispatcher on emergency
    _send_to_dispatcher("\\n".join(lines))
    result = _send_to_truck(vehicle_name, "\\n".join(lines))
    return result if isinstance(result, dict) else {
        "truck_group": None,
        "truck_msg_id": result,
        "dispatcher_msg_id": None
    }'''

replace2 = '''    else:
        return {
            "truck_group": None,
            "truck_msg_id": None,
            "dispatcher_msg_id": None
        }

    # Always notify dispatcher on emergency
    _send_to_dispatcher("\\n".join(lines))
    result = _send_to_truck(vehicle_name, "\\n".join(lines))
    return result if isinstance(result, dict) else {
        "truck_group": None,
        "truck_msg_id": result,
        "dispatcher_msg_id": None
    }'''

content = content.replace(target1, replace1)
content = content.replace(target2, replace2)

with open('telegram_bot.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Patched.")
