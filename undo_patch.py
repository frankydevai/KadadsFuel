import re

with open('state_machine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Restore imports
content = re.sub(r'from telegram_bot import _send_to_truck', 'from telegram_bot import _send_to, _send_to_dispatcher', content)

# Block 1 (around line 303)
b1_target = '''                res = _send_to_truck(vname, msg)
                truck_msg_id = res.get("truck_msg_id")
                dispatcher_msg_id = res.get("dispatcher_msg_id")'''
b1_replace = '''                truck_msg_id = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)'''
content = content.replace(b1_target, b1_replace)

# Block 2 (around line 317)
b2_target = '''                    _send_to_truck(vname, bw_msg)'''
b2_replace = '''                    if truck_group:
                        _send_to(truck_group, bw_msg)
                    _send_to_dispatcher(bw_msg)'''
# Replace all generic usages
content = content.replace(b2_target, b2_replace)
content = content.replace('_send_to_truck(vname, msg)', '''if truck_group:
                            _send_to(truck_group, msg)
                        _send_to_dispatcher(msg)''')
content = content.replace('_send_to_truck(vname, loss_msg)', '''if truck_group:
                    _send_to(truck_group, loss_msg)
                _send_to_dispatcher(loss_msg)''')

# Block 3 (around line 406)
b3_target = '''                        res = _send_to_truck(vname, hdr + msg)
                        state["prev_briefing_truck_msg_id"]      = res.get("truck_msg_id")
                        state["prev_briefing_dispatcher_msg_id"] = res.get("dispatcher_msg_id")'''
b3_replace = '''                        tmid = _send_to(truck_group, hdr + msg) if truck_group else None
                        dmid = _send_to_dispatcher(hdr + msg)
                        state["prev_briefing_truck_msg_id"]      = tmid
                        state["prev_briefing_dispatcher_msg_id"] = dmid'''
content = content.replace(b3_target, b3_replace)


with open('state_machine.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("state_machine.py undo successful.")
