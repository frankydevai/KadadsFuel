// Simulations run only in the explicit local preview mode defined by app.js.
if(PREVIEW){
 const request=DU.req.bind(DU),at=new Date(Date.now()-2*3600e3).toISOString(),done=new Date(Date.now()-3600e3).toISOString(),date=new Date().toISOString().slice(0,10),key='a'.repeat(32);
 const base={truck_unit:'100',load_id:'SIM-100',driver_name:'Simulation Driver',driver_key:key,station_name:'Simulated Fort Worth Pilot',station_city:'Fort Worth',station_state:'TX',planned_gallons:80,fill_to_full:false,recommended_at:at,messaging_mode:'silent',scored:false,scoring_exclusion:'silent_test_advice',detected_gallons:null,fueling_confirmed:false,fueled_elsewhere:false,price_status:'pending'};
 const stops=[{...base,id:45,outcome:'pending',status:'pending',fill_to_full:true,replaces_id:42},
 {...base,id:44,truck_unit:'200',load_id:'SIM-200',outcome:'unconfirmed',status:'lost',fueled_elsewhere:true,detected_gallons:70},
 {...base,id:43,outcome:'visited',status:'saved',visited_at:done,fueling_confirmed:true,detected_gallons:80},
 {...base,id:42,outcome:'missed',status:'lost',missed_at:done,fueled_elsewhere:true,detected_gallons:80,actual_station_name:'Simulated Alternate Stop',price_status:'contracted_estimate',extra_cost:24,alternative_saving:0,actual_price:3.8,planned_price:3.5,actual_price_date:date,planned_price_date:date,replacement_id:45},
 {...base,id:41,outcome:'visited',status:'skipped',visited_at:done,fueling_confirmed:false}];
 DU.req=async(path,options={})=>{
  const q=new URL(path,'http://preview.local').searchParams;
  if(path.startsWith('/api/fuel-advice/stops')){
   const matched=stops.filter(r=>(!q.get('truck_unit')||q.get('truck_unit')===r.truck_unit)&&(!q.get('load_id')||q.get('load_id')===r.load_id)&&(!q.get('driver_key')||q.get('driver_key')===r.driver_key)),summary={total:matched.length,scored:0,price_pending:matched.filter(r=>r.fueled_elsewhere&&r.price_status==='pending').length};
   for(const k of ['visited','missed','pending','unconfirmed'])summary[k]=matched.filter(r=>r.outcome===k).length;
   const rows=matched.filter(r=>(!q.get('outcome')||q.get('outcome')===r.outcome)&&(!q.get('before_id')||r.id<Number(q.get('before_id')))),limit=Number(q.get('limit')||50);
   return {stops:rows.slice(0,limit),summary,holds:[],next_cursor:rows.length>limit?rows[limit-1].id:null,messaging_mode:'silent',non_fueling_stops:q.get('driver_key')||q.get('truck_unit')&&q.get('truck_unit')!=='100'?[]:[{truck_unit:'100',load_id:'SIM-100',details:{duration_minutes:75,last_seen:done,at_advised_stop:false}}]};
  }
  if(path.startsWith('/api/driver-scores'))return {messaging_mode:'silent',summary:{visited:0,missed:0,excluded:5,score:null,estimated_extra_cost:0},drivers:[{driver_key:key,driver_name:'Simulation Driver',truck_units:['100','200'],observed_visited:2,observed_missed:1,visited:0,missed:0,fueled:0,score:null,excluded:5,estimated_extra_cost:0,price_pending:1}],truncated:false};
  return request(path,options);
 };
}
