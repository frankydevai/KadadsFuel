import re

with open('state_machine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add to top level imports if not already there
if 'from route_briefing import' not in content:
    content = content.replace('from telegram_bot import (', 'from route_briefing import plan_route_briefing, format_route_briefing, format_next_stop\nfrom telegram_bot import (')

if 'reachable_miles' not in content[:2000]:
    content = content.replace('haversine_miles,', 'haversine_miles, reachable_miles,')

# Now strip ALL LOCAL IMPORTS of these inside functions!
# This is what caused the UnboundLocalError earlier.
content = re.sub(r'^\s*from route_briefing import plan_route_briefing, format_route_briefing\n', '', content, flags=re.MULTILINE)
content = re.sub(r'^\s*from route_briefing import plan_route_briefing\n', '', content, flags=re.MULTILINE)
content = re.sub(r'^\s*from route_briefing import format_next_stop\n', '', content, flags=re.MULTILINE)
content = re.sub(r'^\s*from truck_stop_finder import haversine_miles(?:, reachable_miles| as _hav)?\n', '', content, flags=re.MULTILINE)
content = re.sub(r'^\s*from truck_stop_finder import find_current_stop\n', '', content, flags=re.MULTILINE)

# Just in case `_hav` was left behind by removing the import but not the usage:
content = re.sub(r'\b_hav\(', 'haversine_miles(', content)

with open('state_machine.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Applied import fixes.")
