/* One request at a time; live labels come from the authenticated API. */
'use strict';
shell();
document.getElementById('content').innerHTML=`
<div class="connection-tools"><label><span class="sr-only">Search truck, driver or group</span><input id="connection-search" class="driver-search" placeholder="Search truck, driver or group name…"></label>
<select id="connection-filter" aria-label="Connection filter"><option value="all">All connections</option><option value="review">Needs review</option><option value="linked">Linked</option><option value="unlinked">Not linked</option><option value="paused">Paused</option></select>
<button class="btn" id="connection-reload">Refresh page data</button><button class="btn primary" id="connection-sync">Sync group names</button></div>
<div class="connection-meta"><span id="connection-summary">Loading connections…</span><span id="connection-time" role="status" aria-live="polite">Updates every 10 seconds</span></div>
<div id="connection-scope" class="connection-warning" role="status"></div>
<div id="connection-notice" role="status" aria-live="polite"></div><div id="connection-stale" class="connection-warning" hidden>Vehicle data is stale. Driver connections can still be refreshed; fuel and movement below are last recorded values.</div>
<div class="card driver-card"><div class="table-wrap"><table><thead><tr><th>Truck</th><th>Driver</th><th>Telegram group</th><th>Connection</th><th>Fuel</th><th>Last vehicle update</th><th>Alerts</th><th>Action</th></tr></thead><tbody id="connection-rows"></tbody></table></div></div>`;
const el=id=>document.getElementById(`connection-${id}`), esc=DU.esc;
let operating={}, data=[], fetching=null, mutating=false, timer=null, lastFetch=null, fetchError=false;
const linked=r=>Boolean(r.telegram_group_id);
const review=r=>r.assignment_status==='conflict';
function notice(message,bad=false){el('notice').textContent=message;el('notice').className=message?'connection-warning'+(bad?' connection-error':''):'';}
function date(value){const d=new Date(value);return value&&!Number.isNaN(d.valueOf())?d.toLocaleString():'Not available';}
function draw(){
 const q=el('search').value.trim().toLowerCase(),filter=el('filter').value;
 const visible=data.filter(r=>[r.unit_number,r.driver_name,r.telegram_group_name,r.telegram_group_id].join(' ').toLowerCase().includes(q)).filter(r=>filter==='all'||filter==='review'&&review(r)||filter==='linked'&&linked(r)||filter==='unlinked'&&!linked(r)||filter==='paused'&&r.alerts_paused);
 el('summary').textContent=`${data.length} trucks · ${data.filter(linked).length} linked · ${data.filter(review).length} need review · ${data.filter(r=>r.alerts_paused).length} paused`;
 el('stale').hidden=!data.some(r=>r.telemetry_stale);
 el('rows').innerHTML=visible.map(r=>{
 const state=review(r)?'Needs review':!linked(r)?'Not linked':r.alerts_paused?'Paused':r.assignment_status==='ready'?'Verified':'Linked';
 const rawFuel=r.fuel_pct, fuel=rawFuel===null||rawFuel===undefined?null:Number(rawFuel);
 const alerts=operating.telegram_messaging_mode==='silent'?'Silent test':r.automatic_processing_enabled===false?'Outside test':!linked(r)?'Stopped':r.alerts_paused?'Paused':review(r)?'Needs review':'Enabled';
 return `<tr><td><a class="unit-link" href="/truck?id=${encodeURIComponent(r.truck_id)}">#${esc(r.unit_number)}</a><span class="connection-sub">${r.samsara_vehicle_id?'Vehicle linked':'Vehicle missing'}</span></td><td>${esc(r.driver_name||'Unassigned')}</td><td><strong class="group-title">${esc(r.telegram_group_name||'No group name available')}</strong><span class="connection-sub">${esc(r.telegram_group_id||'Not connected')}</span></td><td><span class="pill ${state==='Verified'?'lime':state==='Needs review'?'red':''}">${state}</span><span class="connection-sub">Changed ${esc(date(r.assignment_updated_at))}</span></td><td>${fuel!==null&&Number.isFinite(fuel)?`${Math.round(fuel)}%${r.telemetry_stale?'<span class="connection-sub">Stale</span>':''}`:'—'}</td><td>${esc(date(r.last_seen_at))}<span class="connection-sub">${esc(r.status||'Unknown')}</span></td><td>${alerts}</td><td><button class="btn ${linked(r)?'danger':'primary'}" data-unit="${esc(r.unit_number)}" data-action="${linked(r)?'disconnect':'connect'}" ${mutating?'disabled':''}>${linked(r)?'Disconnect':'Connect'}</button></td></tr>`;
 }).join('')||'<tr><td colspan="8" class="empty">No connections match this search.</td></tr>';
}
function schedule(){clearTimeout(timer);if(!document.hidden&&!mutating)timer=setTimeout(()=>load(),10000);}
function load(){
 if(fetching)return fetching;
 fetching=(async()=>{try{const result=await DU.req('/api/drivers',{signal:AbortSignal.timeout(30000)});if(!Array.isArray(result.drivers))throw new Error('Invalid connection response');data=result.drivers;operating=result.operating_scope||{};el('scope').textContent=[operating.full_fleet===false?'Testing trucks '+(operating.test_truck_units||[]).join(', '):'',operating.auto_link_enabled===false?'Auto-link off':'',operating.telegram_messaging_mode==='silent'?'Telegram messages silent':''].filter(Boolean).join(' · ');lastFetch=new Date();if(fetchError){notice('');fetchError=false;}draw();el('time').textContent=`Refreshed ${lastFetch.toLocaleTimeString()} · every 10 seconds`;}catch(e){fetchError=true;el('time').textContent=`Refresh failed${lastFetch?' · last success '+lastFetch.toLocaleTimeString():''}`;notice(e.message||'Could not refresh connections.',true);}finally{fetching=null;schedule();}})();return fetching;
}
async function mutate(action){
 if(mutating)return;mutating=true;clearTimeout(timer);el('sync').disabled=el('reload').disabled=true;draw();
 try{if(fetching)await fetching;await action();await load();}catch(e){notice(e.message||'Connection update failed.',true);}finally{mutating=false;el('sync').disabled=el('reload').disabled=false;draw();schedule();}
}
el('rows').addEventListener('click',e=>{
 const button=e.target.closest('button[data-unit]');if(!button||mutating)return;const unit=button.dataset.unit;
 if(button.dataset.action==='disconnect'){
  if(!confirm(`Disconnect truck ${unit} from its Telegram group?`))return;
  mutate(async()=>{await DU.req(`/api/drivers/${encodeURIComponent(unit)}/connection`,{method:'DELETE'});notice(`Truck ${unit} disconnected.`);});
 }else{
  const input=prompt(`Telegram group ID for truck ${unit}. The group title must contain this truck number and the driver name.`);
  if(input===null)return;const id=Number(input.trim());if(!/^-\d+$/.test(input.trim())||!Number.isSafeInteger(id)||id>=0){notice('Enter a valid negative Telegram group ID.',true);return;}
  mutate(async()=>{await DU.req(`/api/drivers/${encodeURIComponent(unit)}/connection`,{method:'PUT',body:JSON.stringify({telegram_group_id:id})});notice(`Truck ${unit} connection verified and saved.`);});
 }
});
el('sync').onclick=()=>mutate(async()=>{notice('Reading current Telegram group names…');const r=await DU.req('/api/drivers/refresh',{method:'POST',signal:AbortSignal.timeout(120000)});notice(`${operating.auto_link_enabled===false?'Existing groups checked; auto-link is off.':'Group names refreshed.'} ${r.verified_connections} active connections verified.`);});
el('reload').onclick=()=>load();el('search').oninput=draw;el('filter').onchange=draw;
document.addEventListener('visibilitychange',()=>{if(document.hidden)clearTimeout(timer);else if(!mutating)load();});
load();
