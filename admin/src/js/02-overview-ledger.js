let gcpBillingState=null;
let accountBalanceState=null;
let cachedProvidersOverview=null;
let cachedGcpOverview=null;

function renderRestHours(r){
  if(!r||!r.config)return;
  const c=r.config;
  const mode=({sleep:'定时睡眠中',day_rest:'白天小憩中',awake:'清醒中'})[r.rest_kind]||'清醒中';
  text('rest-overview-badge',mode+(c.enabled?' · '+c.rest_hours:' · 定时睡眠已关闭'));
  $('rest-enabled').checked=Boolean(c.enabled);
  $('rest-day-enabled').checked=c.day_rest_enabled!==false;
  $('rest-owner-private').checked=Boolean(c.allow_owner_private);
  const parts=String(c.rest_hours||'23:00-08:00').split('-');
  $('rest-start').value=(parts[0]||'23:00').slice(0,5);
  $('rest-end').value=(parts[1]||'08:00').slice(0,5);
  text('rest-hours-status',mode+(r.day_rest_until?' · 预计 '+lifeFmt(r.day_rest_until,r.timezone)+' 醒来':'')+' · 当前时间 '+r.local_time+'（'+zoneLabel(r.timezone)+'）· Owner 私聊'+(c.allow_owner_private?'照常响应':'同步休息')+' · 来源 '+(r.source==='admin'?'后台 SQLite（重启保留）':'.env 初始值'));
  previewWakeWindow();
  clearDirty('life-config-form');
}

function monStatusPill(status){
  if(status==='in_sync')return pill('Monitoring 已对齐','ok');
  if(status==='normal_lag')return pill('Monitoring 1~3分钟延迟','warn');
  if(status==='drift_detected')return pill('Monitoring 存在偏差','err');
  return pill('Monitoring 未对账','muted');
}

function resolveUnifiedBalance(bal,gcp,gSummary){
  const bq=gcp?.bigquery||gSummary||null;
  const gcpSynced=Boolean(bal?.gcp_synced??bq?.synced);
  const manualCfg=Boolean(bal?.manual_configured??(bal?.set_usd!=null||(bal?.mode?['manual','merged'].includes(bal.mode):bal?.configured)));
  const mode=bal?.mode||(manualCfg&&gcpSynced?'merged':gcpSynced?'gcp':manualCfg?'manual':'none');
  const gcpSettled=bal?.gcp_settled_remaining_usd??bq?.settled_remaining_usd??null;
  const gcpUnsettled=bal?.gcp_unsettled_local_usd??bq?.unsettled_local_usd??null;
  const gcpRealtime=bal?.gcp_realtime_remaining_usd??bq?.realtime_remaining_usd??null;
  const unifiedRem=bal?.remaining_usd!=null?bal.remaining_usd:(gcpSynced&&gcpRealtime!=null?Math.max(0,Number(gcpRealtime)).toFixed(4):null);
  const manualRem=bal?.manual_remaining_usd??(manualCfg?bal?.remaining_usd:null);
  const setUsd=bal?.set_usd??null;
  const exhausted=Boolean(bal?.exhausted||(mode==='gcp'&&bq?.exhausted));
  return{mode,manualCfg,gcpSynced,unifiedRem,manualRem,setUsd,spentMicro:bal?.spent_since_set_microdollars??0,updatedAt:bal?.updated_at??null,gcpSettled,gcpUnsettled,gcpRealtime,exhausted,bq};
}

function renderOverviewRoutingGcp(d,prov,gcp){
  const box=$('overview-routing-gcp'),pills=$('overview-routing-pills'),strip=$('overview-gcp-strip');
  pills.replaceChildren();strip.replaceChildren();strip.hidden=true;text('overview-gcp-note','');
  const pCfg=prov?.config||d.provider_routing||null,pCreds=prov?.credentials||null;
  const gSummary=d.budget_status?.gcp_billing||null;
  const gCfg=gcp?.config||null,bq=gcp?.bigquery||null,mon=gcp?.monitoring||null;
  const u=resolveUnifiedBalance(d.budget_status?.account_balance,gcp,gSummary);
  const routingActive=Boolean(pCfg&&(pCfg.vertex_enabled||pCreds?.gemini_free_configured||pCreds?.vertex_api_key_configured||pCreds?.gcp_service_account_configured));
  const gcpEnabled=Boolean(gSummary?.bigquery_enabled||gSummary?.monitoring_enabled||gSummary?.configured||gCfg?.bigquery_enabled||gCfg?.monitoring_enabled||bq?.synced);
  const balActive=u.mode!=='none';
  if(!routingActive&&!gcpEnabled&&!balActive){box.hidden=true;return;}
  box.hidden=false;
  if(pCfg){
    pills.append(
      pCreds?.gemini_free_configured?pill('Free Tier 门卫就绪','free'):pill('Free Tier 未配（走 Paid）','muted'),
      pill((d.primary_model||'hydrogen').toUpperCase()+' · '+(pCfg.vertex_enabled?'Vertex AI':'Gemini API'),pCfg.vertex_enabled?'vertex':'paid'),
      pCreds?.gcp_service_account_configured?pill('GCP SA 已绑定','ok'):pill('GCP SA 未配置','muted')
    );
  }
  const bqView=u.bq;
  const bqActive=Boolean(gCfg?.bigquery_enabled||gSummary?.bigquery_enabled||u.gcpSynced);
  const monStatus=mon?.status||gSummary?.monitoring_status;
  if(u.mode==='merged')pills.append(pill('统一余额: GCP + 手动合并','ok'));
  else if(u.mode==='gcp')pills.append(pill('统一余额: GCP 赠金实时','ok'));
  else if(u.mode==='manual')pills.append(pill('统一余额: 手动校准','paid'));
  if(bqActive&&bqView){
    const gcpExh=Boolean(bqView.exhausted||gSummary?.exhausted||(u.gcpRealtime!=null&&Number(u.gcpRealtime)<=0&&gCfg?.enforce_credit_hard_stop));
    pills.append(u.exhausted?pill('统一余额已触及硬熔断','err'):(gcpExh&&u.mode==='merged')?pill('GCP 赠金耗尽 · 手动余额接管中','warn'):pill('BigQuery 账单同步启用中','ok'));
  }else if(u.exhausted){
    pills.append(pill('统一余额已耗尽','err'));
  }
  if(gCfg?.monitoring_enabled||gSummary?.monitoring_enabled||(monStatus&&monStatus!=='not_synced'))pills.append(monStatusPill(monStatus));
  if(balActive||(bqActive&&bqView)){
    strip.hidden=false;
    const modeNote=u.mode==='merged'?'GCP 赠金 + 手动补充（不重复扣减本地增量）':u.mode==='gcp'?'BigQuery 已结算剩余 − 本地未结算增量':u.mode==='manual'?'手动校准余额 − 校准后消费':'等待同步或设定';
    strip.append(kpiCard('当前统一可用余额',balActive?usd(u.unifiedRem,4):'未同步',modeNote,u.exhausted?'err':(balActive?'ok':'')));
    if((u.gcpSynced||bqActive)&&bqView){
      strip.append(
        kpiCard('GCP 赠金实时余额',usd(u.gcpRealtime,4),'已结算 '+usd(u.gcpSettled,4)+' − 未结算 '+usd(u.gcpUnsettled,6),Number(u.gcpRealtime??0)<=0?'warn':'accent'),
        u.manualCfg?kpiCard('手动补充余额',usd(u.setUsd??u.manualRem,2),'合并模式下与 GCP 赠金叠加（无重复扣减）','accent'):kpiCard('初始赠金 / 已抵扣',usd(bqView.initial_credits_usd,2)+' / '+usd(bqView.credits_used_usd,4),'UNNEST(credits) 统计',''),
        kpiCard('本地未结算增量',usd(u.gcpUnsettled,6),'latest_usage_end_ts 后本地付费消耗',Number(u.gcpUnsettled||0)>0?'warn':'')
      );
      const ts=bqView.latest_usage_end_ts??gSummary?.latest_usage_end_ts;
      text('overview-gcp-note',(u.mode==='merged'?('合成口径：GCP 实时 + 手动补充 '+usd(u.setUsd??u.manualRem,2)+'（不重复扣除本地增量） · '):'')+(ts?'BigQuery 最新账单水位：'+dateTime(ts):'尚未拉取到 BigQuery 账单水位')+(d.budget_status?.hard_stop?' · 当前系统处于预算/余额熔断状态':''));
    }else if(u.manualCfg){
      strip.append(
        kpiCard('手动校准基准',usd(u.setUsd,2),'校准时间 '+dateTime(u.updatedAt),'accent'),
        kpiCard('校准后已消耗',money(u.spentMicro),'联网搜索费用不计入扣减','')
      );
      text('overview-gcp-note','当前仅启用手动校准余额；联网搜索费用不计入扣减'+(d.budget_status?.hard_stop?' · 当前系统处于预算/余额熔断状态':''));
    }
  }
}

const laneNames={chat:'主对话',patrol:'B 站巡逻',memory:'记忆',memory_flex:'记忆 Flex',admin:'管理测试',proactive:'主动私信',search:'联网检索'};
const percent=new Intl.NumberFormat('zh-CN',{style:'percent',maximumFractionDigits:1});
function renderLanes(lanes,total){
  const rows=[...lanes].sort((a,b)=>b.amount-a.amount||b.calls-a.calls);
  $('lanes-empty').hidden=rows.length>0;
  $('lane-stats').replaceChildren(...rows.map(lane=>{
    const label=laneNames[lane.lane]||lane.lane;
    const row=el('div',undefined,'lane'),track=el('dd'),bar=el('progress');
    bar.max=100;
    bar.value=total>0?100*lane.amount/total:0;
    bar.setAttribute('aria-label',label+'占当日分项费用的比例');
    track.append(bar);
    const amount=el('dd',undefined,'lane-amount');
    amount.append(el('span',usd(lane.amount/1e6)),el('small',lane.calls+' 次'));
    row.append(el('dt',label),track,amount);
    return row;
  }));
}

async function fetchOverviewPayload(){
  try{
    return await api('global/overview');
  }catch(e){
    if(e.name==='AbortError'||!token)throw e;
    if(/HTTP (?:404|405)|not found/i.test(e.message))return api('global/dashboard');
    throw e;
  }
}

async function dashboard(initial,{lightweight=false}={}){
  const signal=sessionRequests.signal;
  let d,prov=cachedProvidersOverview,gcp=cachedGcpOverview;
  if(lightweight){
    d=initial??await fetchOverviewPayload();
    if(!token||signal.aborted)return;
  }else{
    const [dashRes,provRes,gcpRes]=await Promise.allSettled([
      initial??fetchOverviewPayload(),
      api('global/providers'),
      api('gcp-billing')
    ]);
    if(dashRes.status!=='fulfilled')throw dashRes.reason;
    if(!token||signal.aborted)return;
    d=dashRes.value;
    if(provRes.status==='fulfilled'){prov=provRes.value;cachedProvidersOverview=prov;}
    if(gcpRes.status==='fulfilled'){gcp=gcpRes.value;cachedGcpOverview=gcp;}
  }
  displayZone=d.usage.timezone||d.usage.budget_timezone||'Asia/Shanghai';
  text('sidebar-zone',zoneLabel(displayZone));
  stampDashboard();
  renderRestHours(d.rest_hours);
  showNotebookAvailability(Boolean(d.notebook?.enabled));
  const usage=d.usage,lanes=usage.lanes||[];
  const laneTotal=lanes.reduce((sum,lane)=>sum+lane.amount,0);
  const calls=lanes.reduce((sum,lane)=>sum+lane.calls,0);
  const metric=code=>(d.metrics||[]).filter(m=>m.source==='production'&&m.code===code).reduce((sum,m)=>sum+m.n,0);
  text('stat-calls',calls.toLocaleString('zh-CN'));
  text('stat-injection',metric('prompt_injection').toLocaleString('zh-CN'));
  text('stat-noise',metric('noise_ignored').toLocaleString('zh-CN'));
  const hours=Math.floor(d.uptime_seconds/3600),minutes=Math.floor(d.uptime_seconds%3600/60);
  text('stat-uptime',hours>=24?Math.floor(hours/24)+'天 '+(hours%24)+'时':hours?hours+'时 '+minutes+'分':minutes+'分');
  const day=usage.day_local||usage.day||usage.day_utc||'';
  $('overview-day').dateTime=day;
  text('overview-day',day+' · '+zoneLabel(displayZone));
  const pipe=d.pipeline||{};
  text('pipeline-state','守门 '+(pipe.gate??0)+' / 推理 '+(pipe.generating??0));
  text('image-cache',((pipe.image_cache_bytes??0)/1048576).toFixed(1)+' MiB');
  const budget=d.budget_status||{};
  const spent=(budget.daily??laneTotal)/1e6;
  const limit=budget.daily_limit!=null?budget.daily_limit/1e6:Number(d.daily_limit_usd);
  const ratio=limit>0?spent/limit:0;
  const balance=resolveUnifiedBalance(budget.account_balance,gcp,budget.gcp_billing);
  const stopped=d.budget_circuit_open||budget.hard_stop||balance.exhausted;
  text('budget-spend',usd(spent));
  text('budget-limit','日上限 '+usd(limit,limit>0&&limit<0.01?4:2));
  $('budget-meter').hidden=!(limit>0);
  $('budget').value=Math.min(100,ratio*100);
  $('budget').dataset.level=stopped?'stop':ratio>=0.85?'warning':'normal';
  text('budget-percent',percent.format(ratio));
  const budgetNotes=[];
  if(stopped)budgetNotes.push(limit===0?'日预算为零，已暂停扣费':'预算已熔断');
  if(balance.mode!=='none')budgetNotes.push('统一可用余额 '+usd(balance.unifiedRem));
  text('budget-label',budgetNotes.join(' · '));
  $('budget-label').hidden=budgetNotes.length===0;
  text('budget-left',usd(Math.max(0,limit-spent)));
  text('rate-label',d.minute_requests+' / '+d.minute_limit);
  renderLanes(lanes,laneTotal);
  renderOverviewRoutingGcp(d,prov,gcp);
}

function appendGrantRow(g){
  const row=el('div',undefined,'grant-row');
  const nameLabel=el('label','赠金名称匹配 (name_match)');
  const nameIn=el('input');nameIn.type='text';nameIn.maxLength=128;nameIn.placeholder='例如 GenAI App Builder Trial';nameIn.value=g?.name_match||'';
  nameLabel.append(nameIn);
  const usdLabel=el('label','初始额度 USD (initial_usd)');
  const usdIn=el('input');usdIn.type='number';usdIn.min='0';usdIn.max='1000000';usdIn.step='0.01';usdIn.value=g?.initial_usd??'';
  usdLabel.append(usdIn);
  const rm=el('button','删除','danger');rm.type='button';
  rm.addEventListener('click',()=>{row.remove();markDirty('gcp-bq-form');});
  row.append(nameLabel,usdLabel,rm);
  $('gcp-grants-list').append(row);
}
function renderGrantRows(grants){
  $('gcp-grants-list').replaceChildren();
  (grants||[]).forEach(g=>appendGrantRow(g));
}
function collectGrantRows(){
  const grants=[];
  for(const row of $('gcp-grants-list').querySelectorAll('.grant-row')){
    const [nameIn,usdIn]=row.querySelectorAll('input');
    const name_match=nameIn.value.trim();
    const initial_usd=usdIn.value.trim();
    if(!name_match&&!initial_usd)continue;
    if(!name_match)throw Error('请填写赠金规则的 name_match');
    const val=Number(initial_usd);
    if(!Number.isFinite(val)||val<0)throw Error('赠金初始额度须为非负数');
    grants.push({name_match,initial_usd:val.toFixed(2)});
  }
  return grants;
}
function buildBqSqlPreview(tbl){
  const safe=(tbl||'project.dataset.gcp_billing_export_v1_XXXXXX').replace(/`/g,'');
  return 'SELECT\n  COALESCE(NULLIF(c.full_name, \'\'), c.name, c.id) AS credit_name,\n  ROUND(ABS(SUM(c.amount)), 4) AS used_usd\nFROM\n  `'+safe+'`,\n  UNNEST(credits) AS c\nGROUP BY\n  credit_name\nORDER BY\n  used_usd DESC;\n\nSELECT\n  ROUND(COALESCE(SUM(cost), 0), 4) AS gross_cost_usd,\n  ROUND(COALESCE(SUM(cost) + SUM((SELECT COALESCE(SUM(c.amount), 0) FROM UNNEST(credits) AS c)), 0), 4) AS net_cost_usd,\n  UNIX_SECONDS(MAX(usage_end_time)) AS latest_usage_end_ts,\n  ANY_VALUE(currency) AS currency\nFROM\n  `'+safe+'`;';
}
function renderGcpBilling(d){
  if(!d||!d.config)return;
  gcpBillingState=d;
  cachedGcpOverview=d;
  const c=d.config,creds=d.credentials||{},bq=d.bigquery||{},mon=d.monitoring||{};
  $('gcp-bq-enabled').checked=Boolean(c.bigquery_enabled);
  $('gcp-hard-stop').checked=Boolean(c.enforce_credit_hard_stop);
  $('gcp-billing-project').value=c.billing_project||'';
  $('gcp-billing-table').value=c.billing_table||'';
  $('gcp-bq-location').value=c.bigquery_location||'US';
  $('gcp-initial-credits').value=c.initial_credits_usd??0;
  $('gcp-bq-interval').value=c.bigquery_sync_interval_seconds??1800;
  renderGrantRows(c.credit_grants||[]);

  $('gcp-mon-enabled').checked=Boolean(c.monitoring_enabled);
  $('gcp-mon-project').value=c.monitoring_project||'';
  $('gcp-mon-interval').value=c.monitoring_sync_interval_seconds??300;

  const bqExhausted=Boolean(bq.exhausted||(c.enforce_credit_hard_stop&&bq.realtime_remaining_usd!=null&&Number(bq.realtime_remaining_usd)<=0));
  const rescuedByManual=Boolean(bqExhausted&&accountBalanceState?.mode==='merged'&&!accountBalanceState?.exhausted);
  const exhausted=Boolean(bqExhausted&&!rescuedByManual);
  $('gcp-bq-pills').replaceChildren(
    c.bigquery_enabled?pill('BigQuery 同步已启用','ok'):pill('BigQuery 自动同步关闭','muted'),
    creds.service_account_configured?pill('Service Account 已就绪 ('+(creds.service_account_source||'shared')+')','ok'):pill('缺少 Service Account','err'),
    exhausted?pill('赠金硬熔断已触发','err'):rescuedByManual?pill('赠金已耗尽（手动余额接管）','warn'):c.enforce_credit_hard_stop?pill('硬熔断保护开启','warn'):pill('仅监测不硬熔断','muted')
  );
  text('gcp-bq-status','配置来源：'+(d.source||'env')+' · 最近查询：'+dateTime(bq.last_sync_at||bq.synced_at)+(bq.full_table?' · 表 '+bq.full_table:'')+(bq.last_error?' · 最近错误：'+bq.last_error:''));
  text('gcp-sa-status',creds.service_account_configured?('已配置（来源：'+(creds.service_account_source||'shared')+' · 邮箱：'+(creds.service_account_email||'已隐藏')+' · 项目：'+(creds.service_account_project||'未指定')+'）'):'尚未配置 Service Account JSON，可在「全局模型通道」或此处上传。');

  const rtRem=bq.realtime_remaining_usd;
  $('gcp-kpi-strip').replaceChildren(
    kpiCard('初始赠金总额 (Initial)',usd(bq.initial_credits_usd??c.initial_credits_usd,2),'配置的 Credits 初始额度',''),
    kpiCard('BigQuery 已抵扣 (Credits Used)',usd(bq.credits_used_usd,4),'UNNEST(credits) 累计已结算抵扣',''),
    kpiCard('BigQuery 结算后余额',usd(bq.settled_remaining_usd,4),'Initial - Credits Used','accent'),
    kpiCard('本地未结算增量 (Unsettled)',usd(bq.unsettled_local_usd,6),'latest_usage_end_ts 后 SQLite 消费',Number(bq.unsettled_local_usd||0)>0?'warn':''),
    kpiCard('实时预估剩余余额 (Realtime)',usd(rtRem,4),exhausted?'已触及硬熔断上限':rescuedByManual?'赠金已用完，由手动补充余额接管':'Settled - Unsettled Delta',exhausted?'err':rescuedByManual?'warn':(rtRem!=null?'ok':''))
  );
  const lagSec=bq.latest_usage_end_ts?Math.max(0,Math.round(Date.now()/1000-bq.latest_usage_end_ts)):null;
  text('gcp-bq-lag-info',(bq.latest_usage_end_ts?'账单最新 usage_end_time：'+dateTime(bq.latest_usage_end_ts)+'（延迟约 '+Math.floor(lagSec/60)+' 分钟）':'尚未同步 BigQuery 明细水位')+(bq.gross_cost_usd!=null?' · 毛费用 '+usd(bq.gross_cost_usd)+' · 净费用 '+usd(bq.net_cost_usd)+' '+(bq.currency||'USD'):''));
  text('gcp-bq-sql',bq.sql_preview||buildBqSqlPreview(bq.full_table||c.billing_table));
  table('gcp-credits-table',['赠金项目 (credit_name)','已抵扣 (used_usd)','初始额度 (initial_usd)','剩余额度 (remaining_usd)'],(bq.credits||[]).map(r=>[r.credit_name||'—',usd(r.used_usd),r.initial_usd!=null?usd(r.initial_usd,2):'未绑定规则',r.remaining_usd!=null?usd(r.remaining_usd):'—']));

  const official=mon.official||{},localV=mon.local_vertex_paid||mon.local||{},localAll=mon.local_all_providers||{};
  $('gcp-mon-pills').replaceChildren(
    c.monitoring_enabled?pill('Monitoring 定期对账开启','ok'):pill('Monitoring 定期对账关闭','muted'),
    monStatusPill(mon.status),
    mon.drift_ratio!=null?pill('偏差率 '+(Number(mon.drift_ratio)*100).toFixed(2)+'%',mon.status==='drift_detected'?'err':'muted'):null
  );
  text('gcp-mon-status','最近对账：'+dateTime(mon.last_sync_at||mon.synced_at)+(mon.window_start_utc?' · 对账窗口 '+mon.window_start_utc+' ~ '+(mon.window_end_utc||'now'):'')+(mon.delta_tokens!=null?' · 总 Token 差额 (本地 - 官方)：'+mon.delta_tokens:'')+(mon.last_error?' · 错误：'+mon.last_error:''));
  const cmpRows=[];
  if(mon.synced||mon.last_sync_at||mon.synced_at||official.total_tokens!=null||localV.total_tokens!=null){
    const dNum=(a,b)=>(a!=null&&b!=null)?(Number(a)-Number(b)):'—';
    cmpRows.push(
      ['GCP Cloud Monitoring 官方 (online_serving/token_count)',official.input_tokens??'—',official.output_tokens??'—',official.thought_tokens??'—',official.cached_tokens??'—',official.total_tokens??'—'],
      ['本地 SQLite · Vertex AI (vertex_paid)',localV.input_tokens??0,localV.output_tokens??0,localV.thought_tokens??0,localV.cached_tokens??0,localV.total_tokens??((localV.input_tokens||0)+(localV.output_tokens||0)+(localV.thought_tokens||0))],
      ['本地 SQLite · 全通道参考 (all_providers)',localAll.input_tokens??'—',localAll.output_tokens??'—',localAll.thought_tokens??'—',localAll.cached_tokens??'—',localAll.total_tokens??'—'],
      ['对账差额 (Local Vertex - Official)',dNum(localV.input_tokens,official.input_tokens),dNum(localV.output_tokens,official.output_tokens),dNum(localV.thought_tokens,official.thought_tokens),dNum(localV.cached_tokens,official.cached_tokens),mon.delta_tokens??dNum(localV.total_tokens,official.total_tokens)]
    );
  }
  table('gcp-mon-compare-table',['数据源 / 口径','Input Tokens','Output Tokens','Thought Tokens','Cached Tokens','Total Tokens'],cmpRows);
  const monModels=official.by_model||mon.by_model||[];
  table('gcp-mon-models-table',['模型 (model_user_id)','Input','Output','Thought','Cached','Total'],monModels.map(m=>[m.model||m.model_user_id||'unknown',m.input_tokens??0,m.output_tokens??0,m.thought_tokens??0,m.cached_tokens??0,m.total_tokens??0]));
  for(const id of ['gcp-sync-bq','gcp-sync-monitoring','gcp-save-config','gcp-reset-config','gcp-save-mon','gcp-save-sa','gcp-clear-sa'])$(id).disabled=false;
  clearDirty('gcp-bq-form');
  clearDirty('gcp-mon-form');
}
function collectGcpConfigPayload(){
  const initial=Number($('gcp-initial-credits').value);
  const bqInt=Number($('gcp-bq-interval').value);
  const monInt=Number($('gcp-mon-interval').value);
  if(!Number.isFinite(initial)||initial<0)throw Error('初始赠金总额须为非负数字');
  if(!Number.isInteger(bqInt)||bqInt<60)throw Error('BigQuery 同步间隔须为 ≥60 秒的整数');
  if(!Number.isInteger(monInt)||monInt<60)throw Error('Monitoring 对账间隔须为 ≥60 秒的整数');
  return{
    bigquery_enabled:$('gcp-bq-enabled').checked,
    billing_project:$('gcp-billing-project').value.trim(),
    billing_table:$('gcp-billing-table').value.trim(),
    bigquery_location:$('gcp-bq-location').value.trim()||'US',
    initial_credits_usd:initial.toFixed(2),
    credit_grants:collectGrantRows(),
    bigquery_sync_interval_seconds:bqInt,
    enforce_credit_hard_stop:$('gcp-hard-stop').checked,
    monitoring_enabled:$('gcp-mon-enabled').checked,
    monitoring_project:$('gcp-mon-project').value.trim(),
    monitoring_sync_interval_seconds:monInt
  };
}
function renderUnifiedBalance(bal,gcp,gSummary){
  const u=resolveUnifiedBalance(bal,gcp,gSummary);
  if(bal)accountBalanceState=bal;
  const modePills={
    merged:pill('合成模式 · GCP + 手动补充','ok'),
    gcp:pill('GCP 赠金实时模式','ok'),
    manual:pill('手动校准模式','paid'),
    none:pill('余额未配置','muted')
  };
  $('unified-balance-pills').replaceChildren(
    modePills[u.mode]||pill('模式: '+u.mode,'muted'),
    u.gcpSynced?pill('BigQuery 已同步','ok'):pill('BigQuery 未同步','muted'),
    u.manualCfg?pill('手动余额已配置','paid'):pill('手动余额未设定','muted'),
    u.exhausted?pill('统一余额已耗尽 / 触发熔断','err'):(u.mode!=='none'?pill('当前可用余额正常','ok'):null)
  );
  $('unified-balance-strip').replaceChildren(
    kpiCard('当前统一可用余额',u.mode==='none'?'未配置':usd(u.unifiedRem,4),u.mode==='merged'?'GCP 赠金实时 + 手动补充（无重复扣减）':u.mode==='gcp'?'BigQuery 已结算 − 本地未结算增量':u.mode==='manual'?'手动校准余额 − 校准后消费':'请同步 BigQuery 或设定手动余额',u.exhausted?'err':(u.mode==='none'?'':'ok')),
    kpiCard('GCP 赠金实时余额',u.gcpSynced?usd(u.gcpRealtime,4):'未同步',u.gcpSynced?('已结算 '+usd(u.gcpSettled,4)+' − 本地未结算 '+usd(u.gcpUnsettled,6)):'配置 BigQuery 账单导出后自动核算',u.gcpSynced?(Number(u.gcpRealtime??0)<=0?'warn':'accent'):''),
    kpiCard('手动校准 / 补充余额',u.manualCfg?(u.mode==='merged'?usd(u.setUsd??u.manualRem,2):usd(u.manualRem??u.setUsd,4)):'未设定',u.manualCfg?(u.mode==='merged'?('已作为额外现金合并（不重复扣减本地增量）'):('基准 '+usd(u.setUsd,2)+' · 校准后已消耗 '+money(u.spentMicro))):'可在下方设定基础余额或补充现金',u.manualCfg?'accent':'')
  );
  const noteMap={
    merged:'合成计算口径（merged）：当前统一可用余额 = max(0, (GCP 已结算剩余 − 本地未结算增量) + 手动补充余额 '+(u.setUsd!=null?usd(u.setUsd,2):'$0.00')+')；本地未结算增量仅在 GCP 实时余额中扣除一次，绝不重复扣减，赠金耗尽时自动消耗手动补充余额。',
    gcp:'计算口径（gcp）：当前统一可用余额 = max(0, BigQuery 已结算剩余 − latest_usage_end_ts 后本地未结算增量)，不重复扣除本地增量；如需叠加额外充值现金，可在下方设置补充余额。',
    manual:'计算口径（manual）：当前未同步 GCP BigQuery 账单，按 手动校准余额 − 校准后本地付费消耗 实时计算（联网搜索不计入扣减）。',
    none:'当前未配置手动余额且未同步 GCP BigQuery 账单；可通过下方表单设置余额或配置 BigQuery 导出表。'
  };
  text('unified-balance-note',noteMap[u.mode]||noteMap.none);
  if(u.manualCfg){
    text('balance-status',u.mode==='merged'?('手动补充余额：'+usd(u.setUsd,2)+'（于 '+dateTime(u.updatedAt)+' 设定）· 合并后当前统一可用余额：'+usd(u.unifiedRem,4)+'（不重复扣除本地增量）'):('预估当前剩余余额：'+usd(u.manualRem,4)+'（上次于 '+dateTime(u.updatedAt)+' 校准为 '+usd(u.setUsd,2)+' · 校准后已消耗 '+money(u.spentMicro)+'）'));
    if(document.activeElement!==$('balance-input')){
      const inputNum=u.mode==='merged'?(u.setUsd??u.manualRem):(u.manualRem??u.unifiedRem);
      if(inputNum!=null)$('balance-input').value=Number(inputNum).toFixed(2);
    }
  }else{
    text('balance-status',u.gcpSynced?'尚未设定手动校准 / 补充余额（当前由 GCP 赠金实时余额驱动）':'尚未设定当前账户余额');
  }
  for(const id of ['save-balance','clear-balance'])$(id).disabled=false;
  clearDirty('balance-form');
}

$('gcp-add-grant').addEventListener('click',()=>{appendGrantRow({name_match:'',initial_usd:''});markDirty('gcp-bq-form');});
async function refreshUnifiedAfterGcp(g){
  gcpBillingState=g;
  const bal=await api('account-balance').catch(()=>accountBalanceState);
  if(bal)accountBalanceState=bal;
  renderGcpBilling(g);
  renderUnifiedBalance(accountBalanceState,g,null);
}
$('refresh-gcp-billing').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('ledger','刷新账单状态')))return;
  const g=await api('gcp-billing');
  await refreshUnifiedAfterGcp(g);
  notice('已刷新统一余额与 GCP 账单状态');
}));
$('gcp-sync-bq').addEventListener('click',e=>busy(e.target,async()=>{const g=await api('gcp-billing/sync-bigquery','POST');await refreshUnifiedAfterGcp(g);notice('BigQuery 账单同步完成');}));
$('gcp-sync-monitoring').addEventListener('click',e=>busy(e.target,async()=>{const g=await api('gcp-billing/sync-monitoring','POST');renderGcpBilling(g);notice('Cloud Monitoring 对账完成');}));
$('gcp-bq-form').addEventListener('submit',e=>{e.preventDefault();busy($('gcp-save-config'),async()=>{const g=await api('gcp-billing','PUT',collectGcpConfigPayload());await refreshUnifiedAfterGcp(g);notice('BigQuery 与对账配置已保存');});});
$('gcp-mon-form').addEventListener('submit',e=>{e.preventDefault();busy($('gcp-save-mon'),async()=>{const g=await api('gcp-billing','PUT',collectGcpConfigPayload());renderGcpBilling(g);notice('Cloud Monitoring 配置已保存');});});
$('gcp-reset-config').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复 .env 默认 GCP Billing 与对账配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  const g=await api('gcp-billing','DELETE');
  await refreshUnifiedAfterGcp(g);
  notice('已恢复 .env 默认 GCP 账单配置');
}));
$('gcp-sa-file').addEventListener('change',e=>busy($('gcp-save-sa'),()=>loadJsonFileInto(e.target,$('gcp-sa-json'))));
$('gcp-sa-form').addEventListener('submit',e=>{e.preventDefault();busy($('gcp-save-sa'),async()=>{
  const raw=$('gcp-sa-json').value.trim();
  if(!raw)throw Error('请先粘贴或导入 Service Account JSON');
  try{const parsed=JSON.parse(raw);if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw 0;}catch{throw Error('Service Account JSON 格式无效，请检查内容');}
  const g=await api('gcp-billing/credentials','POST',{gcp_service_account_json:raw,clear:false});
  $('gcp-sa-json').value='';$('gcp-sa-file').value='';clearDirty('gcp-sa-form');
  renderGcpBilling(g);notice('独立 Service Account JSON 已加密保存');
});});
$('gcp-clear-sa').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('清除独立覆盖的 Service Account JSON，回退共享「全局模型通道」凭据？',{danger:true,confirmLabel:'清除覆盖'})))return;
  const g=await api('gcp-billing/credentials','POST',{gcp_service_account_json:'',clear:true});
  $('gcp-sa-json').value='';$('gcp-sa-file').value='';clearDirty('gcp-sa-form');
  renderGcpBilling(g);notice('已清除独立 Service Account 覆盖');
}));

async function ledger(){
  const [dashRes,polRes,gcpRes,balRes]=await Promise.allSettled([api('global/dashboard'),api('budget'),api('gcp-billing'),api('account-balance')]);
  if(dashRes.status!=='fulfilled')throw dashRes.reason;
  if(polRes.status!=='fulfilled')throw polRes.reason;
  const d=dashRes.value,policy=polRes.value;
  if(!token)return;
  const gcpVal=gcpRes.status==='fulfilled'?gcpRes.value:null;
  const balVal=balRes.status==='fulfilled'?balRes.value:(d.budget_status?.account_balance||null);
  if(gcpVal)gcpBillingState=gcpVal;
  if(balVal)accountBalanceState=balVal;
  const usage=d.usage,periods=[['today','今日 '+zoneLabel(usage.timezone||usage.budget_timezone||'Asia/Shanghai')],['month','本月 '+zoneLabel(usage.timezone||usage.budget_timezone||'Asia/Shanghai')],['lifetime','累计可追溯']];
  const modelLabels={hydrogen:'Hydrogen',neon:'Neon',carbon:'Carbon',memory_flex:'Hydrogen Flex',neon_flex:'Neon Flex',carbon_flex:'Carbon Flex',decision:'Decisions',unknown:'历史未归属（Gemini）'};
  const thinkingStatLabels={low:'low · 轻量',medium:'medium · 均衡',high:'high · 深入',xhigh:'xhigh · 最高',minimal:'minimal · 极简',none:'none · 无思考（门卫）',unknown:'历史未记录档位'};
  table('token-ledger',['范围','API 尝试 / 有用量','输入','缓存输入','输出','思考','观测缓存 / Interactions 输入'],periods.map(([key,label])=>{const t=usage.token_usage?.[key];if(!t)return[label,'未知','未知','未知','未知','未知','未知'];const cache=t.cache_hit_ratio===null||t.cache_hit_ratio===undefined?'未知':(100*t.cache_hit_ratio).toFixed(1)+'%';return[label,t.calls+' / '+t.known_usage_calls+(t.unknown_usage_calls?'（'+t.unknown_usage_calls+' 次用量未知）':''),t.known_usage_calls?t.input_tokens:'未知',t.cache_known_calls?t.cached_tokens:'未知',t.known_usage_calls?t.output_tokens:'未知',t.thought_known_calls?t.thought_tokens:'未知',cache+(t.cache_coverage_complete===false?'（下界）':'')+' · 覆盖 '+t.cache_known_calls+'/'+t.interaction_calls+' 次 Interactions'];}));
  table('provider-ledger',['范围','API 通道','调用次数','输入','输出','思考','缓存 Token','预算扣账','已确认估算'],periods.flatMap(([key,label])=>(usage.token_usage?.[key]?.by_provider||[]).map(p=>[label,providerBadge(p.provider),p.calls,p.input_tokens??0,p.output_tokens??0,p.thought_tokens??0,p.cached_tokens??0,p.provider==='gemini_free'?'$0.000000 (Free)':money(p.charged_amount),p.estimated_amount!=null?money(p.estimated_amount):'未知'])));
  table('work-ledger',['动作','动作编号','所属任务','模型调用','已确认估算','含预留扣账'],(usage.recent_work||[]).map(w=>[
    w.kind==='legacy'?'升级前未记录归属':w.kind,w.work_id||'—',w.root_id&&w.root_id!==w.work_id?w.root_id:'—',w.calls,
    w.known_calls?money(w.estimated_microdollars)+(w.known_calls<w.calls?'（部分）':''):'未知',money(w.charged_microdollars)
  ]));
  table('cost-ledger',['范围','预算扣账（含在途 / 中断估算）','已确认账单估算','估算覆盖'],periods.map(([key,label])=>{const c=usage.cost_estimates?.[key];if(!c)return[label,'未知','未知','未知'];const diff=c.estimated_microdollars!=null&&c.charged_microdollars>c.estimated_microdollars?'（中断/在途 '+money(c.charged_microdollars-c.estimated_microdollars)+'）':'';return[label,money(c.charged_microdollars),money(c.estimated_microdollars),c.complete?'完整':c.estimated_calls+' 次已确认 / '+c.unknown_estimate_calls+' 次未知'+diff];}));
  table('model-ledger',['范围','模型','调用','输入 / 缓存 / 思考','预算扣账','已确认估算'],periods.flatMap(([key,label])=>(usage.token_usage?.[key]?.by_model||[]).map(m=>[label,modelLabels[m.model_lane]||m.model_lane,m.calls,m.model_lane==='unknown'?'未记录 / 未记录 / 未记录':m.input_tokens+' / '+m.cached_tokens+' / '+m.thought_tokens,money(m.charged_amount),m.estimated_calls?money(m.estimated_amount):'未知'])));
  table('thinking-ledger',['范围','模型','思考档位','调用','输入 / 缓存 / 输出 / 思考','预算扣账','已确认估算'],periods.flatMap(([key,label])=>(usage.token_usage?.[key]?.by_thinking||[]).map(t=>[label,modelLabels[t.model_lane]||t.model_lane,thinkingStatLabels[t.thinking_level]||t.thinking_level,t.calls,t.model_lane==='unknown'?'未记录':t.input_tokens+' / '+t.cached_tokens+' / '+t.output_tokens+' / '+t.thought_tokens,money(t.charged_amount),t.estimated_calls?money(t.estimated_amount):'未知'])));
  table('effective-cache',['范围','合资格调用','有效覆盖率'],periods.map(([key,label])=>{const t=usage.token_usage?.[key];return[label,t?.effective_cache_calls||0,t?.effective_cache_hit_ratio==null?'暂无合资格观测':(t.effective_cache_hit_ratio*100).toFixed(1)+'%'];}));
  const cacheHealth=usage.cache_health||{};
  text('cache-alert',cacheHealth.prefix_busts_today?'⚠ 今日前缀异常击穿 '+cacheHealth.prefix_busts_today+' 次，请查看安全日志及截断、配置变更时间':'今日未记录前缀异常击穿；有效覆盖率稳态目标 ≥90%');
  $('cache-alert').className=cacheHealth.prefix_busts_today?'error':'muted';
  const h=usage.history||{};
  text('ledger-coverage','观测记录起点 '+(h.since_utc||'尚无记录')+(h.lifetime_call_count_complete===false?' · 旧累计调用明细不完整':'')+(h.legacy_unattributed_microdollars?' · 旧累计中 '+money(h.legacy_unattributed_microdollars)+' 无可追溯用量明细':''));
  for(const [k,id] of [['daily_usd','budget-daily'],['monthly_usd','budget-monthly'],['user_daily_usd','budget-user'],['patrol_daily_usd','budget-patrol'],['memory_daily_usd','budget-memory'],['proactive_daily_usd','budget-proactive'],['admin_daily_usd','budget-admin']])$(id).value=policy.config[k];
  clearDirty('budget-policy');
  text('budget-source','配置来自 '+policy.source+' · '+(d.budget_status?.hard_stop?'当前暂停付费工作':'当前允许按额度调用')+((d.budget_status?.daily_calendar_carry||d.budget_status?.monthly_calendar_carry)?' · 历史时区迁移保守占用（日/月）：'+money(d.budget_status.daily_calendar_carry||0)+' / '+money(d.budget_status.monthly_calendar_carry||0)+'，不代表新增真实花销':''));
  const b=d.budget_status||{},sb=b.sub_budgets||{};
  renderUnifiedBalance(balVal,gcpVal,b.gcp_billing);
  table('budget-periods',['范围','账单扣账额','上限'],[['今日 '+(b.day_local||b.day||b.day_utc||''),money(b.daily),money(b.daily_limit)],['本月 '+(b.month_local||b.month||b.month_utc||''),money(b.monthly),money(b.monthly_limit)],['累计总上限（.env）',money(b.total),Number(b.total_limit)===0?'未设累计上限':money(b.total_limit)],['单用户今日最高',money(b.user_top_daily||0),money(b.user_daily_limit??0)],['B 站巡逻今日',money(sb.patrol?.used||0),money(sb.patrol?.limit??0)],['记忆提炼今日',money(sb.memory?.used||0),money(sb.memory?.limit??0)],['主动私信今日',money(sb.proactive?.used||0),money(sb.proactive?.limit??0)],['管理沙盒今日',money(sb.admin?.used||0),money(sb.admin?.limit??0)]]);
  if(gcpVal)renderGcpBilling(gcpVal);
}
$('budget-policy').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{await api('budget','PUT',{daily_usd:$('budget-daily').value,monthly_usd:$('budget-monthly').value,user_daily_usd:$('budget-user').value,patrol_daily_usd:$('budget-patrol').value,memory_daily_usd:$('budget-memory').value,proactive_daily_usd:$('budget-proactive').value,admin_daily_usd:$('budget-admin').value});notice('全局与分项预算已保存并立即生效');await ledger();});});
$('reset-budget').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复 .env 中的全局与分项预算？',{danger:true,confirmLabel:'恢复 .env 预算'})))return;
  await api('budget','DELETE');
  notice('已恢复 .env 全局与分项预算');
  await ledger();
}));
$('balance-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-balance'),async()=>{
  const val=Number($('balance-input').value);
  if(!Number.isFinite(val)||val<0||val>100000)throw Error('请输入 0–100000 之间的余额数字');
  const res=await api('account-balance','PUT',{balance_usd:$('balance-input').value});
  accountBalanceState=res;
  const inputVal=res.mode==='merged'?(res.set_usd??res.manual_remaining_usd??res.remaining_usd):(res.manual_remaining_usd??res.remaining_usd);
  if(inputVal!=null)$('balance-input').value=Number(inputVal).toFixed(2);
  clearDirty('balance-form');
  notice(res.mode==='merged'?('手动补充余额已设为 '+usd(res.set_usd??val,2)+' · 当前统一可用余额 '+usd(res.remaining_usd,4)):('账户余额已校准为 '+usd(res.remaining_usd,4)));
  await ledger();
});});
$('clear-balance').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('清除当前手动账户余额记录？',{danger:true,confirmLabel:'清除记录'})))return;
  const res=await api('account-balance','DELETE');
  accountBalanceState=res;$('balance-input').value='';clearDirty('balance-form');
  notice('已清除手动账户余额记录'+(res.gcp_synced?'，当前由 GCP 赠金实时余额独立驱动':''));
  await ledger();
}));
$('refresh-ledger').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('ledger','刷新账本')))return;
  await ledger();
}));

trackDirty('balance-form','手动余额校准');
trackDirty('gcp-bq-form','BigQuery 账单配置');
trackDirty('gcp-mon-form','Cloud Monitoring 配置');
trackDirty('gcp-sa-form','独立 Service Account');
trackDirty('budget-policy','全局与分项预算');

onLogout(()=>{
  gcpBillingState=null;
  accountBalanceState=null;
  cachedProvidersOverview=null;
  cachedGcpOverview=null;
  for(const id of ['stat-calls','stat-injection','stat-noise','stat-uptime','budget-spend','budget-left','pipeline-state','image-cache'])text(id,'—');
  for(const id of ['overview-day','budget-limit','budget-label','budget-percent','rate-label','overview-gcp-note','unified-balance-note','balance-status','gcp-bq-status','gcp-bq-lag-info','gcp-sa-status','gcp-mon-status','budget-source','ledger-coverage','cache-alert'])text(id,'');
  $('overview-day').removeAttribute('datetime');
  $('budget').value=0;
  text('rest-overview-badge','作息读取中');
  $('overview-routing-gcp').hidden=true;
  $('overview-gcp-strip').hidden=true;
  text('gcp-bq-sql','配置 billing_table 后可预览查询 SQL。');
  for(const id of ['lane-stats','overview-routing-pills','overview-gcp-strip','unified-balance-pills','unified-balance-strip','gcp-bq-pills','gcp-kpi-strip','gcp-mon-pills','gcp-grants-list','token-ledger','work-ledger','provider-ledger','cost-ledger','budget-periods','model-ledger','thinking-ledger','effective-cache','gcp-credits-table','gcp-mon-compare-table','gcp-mon-models-table'])$(id).replaceChildren();
  for(const id of ['save-balance','clear-balance','gcp-sync-bq','gcp-sync-monitoring','gcp-save-config','gcp-reset-config','gcp-save-mon','gcp-save-sa','gcp-clear-sa'])$(id).disabled=true;
});
