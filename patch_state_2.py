import re

with open('state_machine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove all local imports that shadow globals and cause UnboundLocalError
content = re.sub(r'from truck_stop_finder import haversine_miles(?:, reachable_miles| as _hav)?\n', '', content)
content = re.sub(r'from truck_stop_finder import find_current_stop\n', '', content)
content = re.sub(r'from route_briefing import plan_route_briefing, format_route_briefing\n', '', content)
content = re.sub(r'from route_briefing import plan_route_briefing\n', '', content)
content = re.sub(r'from route_briefing import format_next_stop\n', '', content)
content = re.sub(r' *from truck_stop_finder import haversine_miles\n', '', content)

# Fix the format_route_briefing signature in _fire_alert
wrong_call = '''            msg = format_route_briefing(
                vname, state.get("qm_trip_id", "Unknown"),
                state.get("qm_origin_city", "Unknown"),
                state.get("qm_dest_city", "Unknown"),
                plan, mpg
            )'''
right_call = '''            msg = format_route_briefing(
                plan=plan,
                truck_name=vname,
                route=route,
                fuel_pct=fuel,
                mpg=mpg
            )'''
content = content.replace(wrong_call, right_call)

# Ensure _send_to_dispatcher is replaced if any got left behind
# In _process_truck, there was a raw _send_to_dispatcher(msg)
content = re.sub(r'^\s*dispatcher_msg_id = _send_to_dispatcher\(msg\)\s*$', '', content, flags=re.MULTILINE)
# Also check for any other leftover `_send_to_dispatcher`
content = re.sub(r'^\s*_send_to_dispatcher\([^)]+\)\s*$', '', content, flags=re.MULTILINE)

# If we stripped the only _send_to_dispatcher assignment, we might have left dispatcher_msg_id unbound.
# Just to be safe, replace `dispatcher_msg_id = _send_to_dispatcher(msg)` completely in the specific spot it broke.
# Actually, the user logs show `dispatcher_msg_id = _send_to_dispatcher(msg)` was causing an error.
# Let's replace the whole assignment block if it wasn't replaced properly.

b1_target = '''                truck_msg_id      = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)'''
b1_replace = '''                res = _send_to_truck(vname, msg)
                truck_msg_id = res.get("truck_msg_id")
                dispatcher_msg_id = res.get("dispatcher_msg_id")'''
content = content.replace(b1_target, b1_replace)

b1_target_2 = '''                truck_msg_id = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)'''
content = content.replace(b1_target_2, b1_replace)

b1_target_3 = '''                truck_msg_id      = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)'''
content = content.replace(b1_target_3, b1_replace)


with open('state_machine.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Patch applied.")
