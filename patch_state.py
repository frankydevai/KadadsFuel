import re

with open('state_machine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Pattern to replace explicit imports of _send_to, _send_to_dispatcher
# with _send_to_truck
content = re.sub(r'from telegram_bot import _send_to, _send_to_dispatcher', 'from telegram_bot import _send_to_truck', content)
content = re.sub(r'from telegram_bot import _send_to_dispatcher, _send_to', 'from telegram_bot import _send_to_truck', content)

# Block 1 (around line 303)
b1_target = '''                truck_msg_id      = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)
                state["briefing_msg_id"] = truck_msg_id
                state["briefing_disp_msg_id"] = dispatcher_msg_id'''
b1_replace = '''                res = _send_to_truck(vname, msg)
                state["briefing_msg_id"] = res.get("truck_msg_id")
                state["briefing_disp_msg_id"] = res.get("dispatcher_msg_id")'''

content = content.replace(b1_target, b1_replace)

# Block 2 (around line 317)
b2_target = '''                    if truck_group:
                        _send_to(truck_group, bw_msg)
                    _send_to_dispatcher(bw_msg)'''
b2_replace = '''                    _send_to_truck(vname, bw_msg)'''

content = content.replace(b2_target, b2_replace)

# Block 3 (around line 406)
b3_target = '''                        tmid = _send_to(truck_group, hdr + msg) if truck_group else None
                        dmid = _send_to_dispatcher(hdr + msg)
                        state["briefing_msg_id"] = tmid
                        state["briefing_disp_msg_id"] = dmid'''
b3_replace = '''                        res = _send_to_truck(vname, hdr + msg)
                        state["briefing_msg_id"] = res.get("truck_msg_id")
                        state["briefing_disp_msg_id"] = res.get("dispatcher_msg_id")'''

content = content.replace(b3_target, b3_replace)

# Block 4 (around line 577)
b4_target = '''                        if truck_group:
                            _send_to(truck_group, msg)
                        _send_to_dispatcher(msg)'''
b4_replace = '''                        _send_to_truck(vname, msg)'''
content = content.replace(b4_target, b4_replace)

# Block 5 (around line 824)
b5_target = '''            if truck_group:
                _send_to(truck_group, msg)
            _send_to_dispatcher(msg)'''
b5_replace = '''            _send_to_truck(vname, msg)'''
content = content.replace(b5_target, b5_replace)

# Block 6 (around line 905)
b6_target = '''                if truck_group:
                    _send_to(truck_group, loss_msg)
                _send_to_dispatcher(loss_msg)'''
b6_replace = '''                _send_to_truck(vname, loss_msg)'''
content = content.replace(b6_target, b6_replace)

# Block 7 (around line 1112)
b7_target = '''                        truck_group = get_truck_group(vname)
                        if truck_group:
                            _send_to(truck_group, msg)
                        _send_to_dispatcher(msg)'''
b7_replace = '''                        _send_to_truck(vname, msg)'''
content = content.replace(b7_target, b7_replace)

with open('state_machine.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("state_machine.py successfully patched.")
