let gcpBillingState=null;
let accountBalanceState=null;
let cachedProvidersOverview=null;
let cachedGcpOverview=null;
let cachedColabOverview=null;
let colabUnavailableReason='';
let lastLedgerDash=null;
let cloudTrendDays=14;

const SVG_NS='http://www.w3.org/2000/svg';
function svgEl(tag,attrs={},cls=''){
  const node=document.createElementNS(SVG_NS,tag);
  if(cls)node.setAttribute('class',cls);
  for(const [k,v] of Object.entries(attrs)){
    if(v!==undefined&&v!==null)node.setAttribute(k,String(v));
  }
  return node;
}
function signedUsd(value,digits=4){
  if(value===null||value===undefined||value==='')return '—';
  const n=Number(value);
  if(!Number.isFinite(n))return '—';
  return (n>0?'+$':n<0?'-$':'$')+Math.abs(n).toFixed(digits);
}
function pctText(ratio,digits=1){
  if(ratio===null||ratio===undefined||!Number.isFinite(Number(ratio)))return '—';
  return (Number(ratio)*100).toFixed(digits)+'%';
}
function formatTtlCompact(sec){
  const n=Math.max(0,Math.round(Number(sec)||0));
  if(n<=0)return '即将释放';
  if(n<60)return n+' 秒';
  const m=Math.floor(n/60),r=n%60;
  return r?(m+' 分 '+r+' 秒'):(m+' 分钟');
}
function gcpReconcilePill(status){
  if(status==='in_sync')return pill('账单对账已对齐','ok');
  if(status==='export_lag')return pill('导出延迟正常','warn');
  if(status==='drift_detected')return pill('账单与估算存在差额','err');
  return pill('账单尚未对账','muted');
}
function resolveGcpReconciliation(gcp,dashUsage){
  if(gcp?.reconciliation&&typeof gcp.reconciliation==='object')return gcp.reconciliation;
  const bq=gcp?.bigquery||{},mon=gcp?.monitoring||{};
  const lifeEst=dashUsage?.cost_estimates?.lifetime||null;
  const localCharged=Number(lifeEst?.charged_microdollars??0)/1e6;
  const localEst=lifeEst?.estimated_microdollars!=null?Number(lifeEst.estimated_microdollars)/1e6:null;
  const useEst=localEst!=null&&Boolean(lifeEst?.complete);
  const localRef=useEst?localEst:localCharged;
  const initial=Number(bq.initial_credits_usd??gcp?.config?.initial_credits_usd??0);
  const used=Number(bq.credits_used_usd??0);
  const util=initial>0?Math.min(10,Math.max(0,used/initial)):null;
  if(!bq.synced){
    return{
      status:'not_synced',
      billing_actual_usd:null,
      billing_net_usd:null,
      billing_plus_unsettled_usd:null,
      local_estimated_usd:localEst,
      local_charged_usd:localCharged,
      local_reference_usd:localRef,
      local_reference_basis:useEst?'estimated':'charged',
      cost_delta_usd:null,
      cost_delta_after_unsettled_usd:null,
      cost_delta_ratio:null,
      credit_utilization_ratio:util,
      monitoring_status:mon.status||'not_synced',
      last_sync_at:bq.last_sync_at??null
    };
  }
  const actual=Number(bq.gross_cost_usd??0);
  const netUsd=Number(bq.net_cost_usd??0);
  const unsettled=Number(bq.unsettled_local_usd??0);
  const plusUnsettled=actual+unsettled;
  const delta=actual-localRef;
  const deltaAfter=plusUnsettled-localRef;
  const denom=Math.max(Math.abs(actual),Math.abs(localRef));
  const ratio=denom===0?0:Math.abs(delta)/denom;
  const afterDenom=Math.max(Math.abs(plusUnsettled),Math.abs(localRef));
  let status='drift_detected';
  if(Math.abs(delta)<=0.05||ratio<=0.05)status='in_sync';
  else if(delta<0&&unsettled>0&&Math.abs(deltaAfter)<=Math.max(0.05,0.1*afterDenom))status='export_lag';
  return{
    status,
    billing_actual_usd:actual,
    billing_net_usd:netUsd,
    billing_plus_unsettled_usd:plusUnsettled,
    local_estimated_usd:localEst,
    local_charged_usd:localCharged,
    local_reference_usd:localRef,
    local_reference_basis:useEst?'estimated':'charged',
    cost_delta_usd:delta,
    cost_delta_after_unsettled_usd:deltaAfter,
    cost_delta_ratio:ratio,
    credit_utilization_ratio:util,
    monitoring_status:mon.status||'not_synced',
    last_sync_at:bq.last_sync_at??null
  };
}
function normalizeColabAnalytics(colab){
  if(!colab||typeof colab!=='object')return null;
  const jobs=Array.isArray(colab.jobs)?colab.jobs:[];
  const warmSessions=Array.isArray(colab.warm_sessions)?colab.warm_sessions:(Array.isArray(colab.worker?.warm_sessions)?colab.worker.warm_sessions:[]);
  const cuTrend=Array.isArray(colab.cu_trend)?colab.cu_trend:null;
  const gpuBreakdown=Array.isArray(colab.gpu_breakdown)?colab.gpu_breakdown:null;
  const accel=colab.acceleration_stats&&typeof colab.acceleration_stats==='object'?colab.acceleration_stats:null;
  let summary=colab.summary&&typeof colab.summary==='object'?colab.summary:null;
  if(!summary){
    let succ=0,fail=0,canc=0,run=0,q=0,cuSum=0;
    for(const j of jobs){
      if(j.status==='succeeded')succ++;
      else if(j.status==='failed')fail++;
      else if(j.status==='cancelled')canc++;
      else if(j.status==='running')run++;
      else if(j.status==='queued')q++;
      if(j.cu_used!=null&&Number.isFinite(Number(j.cu_used)))cuSum+=Number(j.cu_used);
    }
    const fin=succ+fail;
    summary={
      total_jobs:jobs.length,
      succeeded_jobs:succ,
      failed_jobs:fail,
      cancelled_jobs:canc,
      running_jobs:run,
      queued_jobs:q,
      success_rate:fin>0?succ/fin:null,
      total_cu_used:cuSum,
      last_14d_cu_used:null,
      last_30d_cu_used:null,
      last_14d_jobs:null,
      last_30d_jobs:null,
      derived_legacy:true
    };
  }
  return{summary,warmSessions,cuTrend,gpuBreakdown,accel};
}
function syncTrendButtons(){
  for(const [id,dVal] of [['cloud-trend-14d',14],['cloud-trend-30d',30],['colab-trend-14d',14],['colab-trend-30d',30]]){
    const btn=$(id);
    if(btn)btn.setAttribute('aria-pressed',String(cloudTrendDays===dVal));
  }
}
function renderWarmSessionsList(containerId,warmSessions,accel,isReachable,isConfigured){
  const box=$(containerId);
  if(!box)return;
  box.replaceChildren();
  if(!isConfigured){
    box.append(el('p','Colab 尚未配置 COLAB_WORKER_URL，热会话保活未启用。','warm-strip-idle'));
    return;
  }
  if(Array.isArray(warmSessions)&&warmSessions.length>0){
    for(const s of warmSessions){
      const card=el('div',undefined,'warm-card');
      const head=el('div',undefined,'warm-card-head');
      const gpuLabel=String(s.gpu||'GPU');
      const ready=Boolean(s.setup_ready);
      head.append(
        pill('热机 '+gpuLabel,'vertex'),
        pill(ready?'环境就绪':'初始化中',ready?'ok':'warn'),
        el('span',String(s.session_name||'session').slice(0,20),'code-mono')
      );
      const ttlTotal=Math.max(1,Number(s.idle_ttl_seconds||360));
      const ttlRem=Math.max(0,Number(s.remaining_ttl_seconds??0));
      const bar=el('progress');
      bar.max=100;
      bar.value=Math.min(100,Math.max(0,(ttlRem/ttlTotal)*100));
      bar.setAttribute('aria-label',gpuLabel+' 热会话剩余保活时间 '+formatTtlCompact(ttlRem));
      const meta=el('div',undefined,'warm-card-meta');
      meta.append(
        el('span','剩余 TTL '+formatTtlCompact(ttlRem)+' / '+formatTtlCompact(ttlTotal)),
        el('span','已复用 '+Number(s.reuse_count||0)+' 次')
      );
      card.append(head,bar,meta);
      box.append(card);
    }
    return;
  }
  let idleText='当前无保活中的热会话';
  if(accel&&typeof accel==='object'){
    if(accel.warm_pool_enabled){
      const allowed=Array.isArray(accel.warm_allowed_gpus)&&accel.warm_allowed_gpus.length?accel.warm_allowed_gpus.join(' / '):'L4 / T4';
      idleText+=' · 策略：'+allowed+' 完工保活 '+formatTtlCompact(accel.warm_idle_ttl_seconds||360)+'（上限 '+formatTtlCompact(accel.warm_max_ttl_seconds||900)+'，H100/A100/G4 不保活）';
    }else{
      idleText+=' · 短时热保活当前已关闭（任务结束后立即释放运行时）';
    }
  }else if(!isReachable){
    idleText+='（Worker 离线或暂不可达）';
  }else{
    idleText+='（旧版引擎暂未上报热会话池明细）';
  }
  box.append(el('p',idleText,'warm-strip-idle'));
}
function renderCuTrendChart(chartId,readoutId,noteId,trendRaw,days){
  const host=$(chartId);
  if(!host)return;
  host.replaceChildren();
  if(!Array.isArray(trendRaw)||trendRaw.length===0){
    host.append(el('p','当前引擎未返回每日 CU 消耗趋势数据（升级后端或等待作业执行后将自动绘制）。','empty-state'));
    text(readoutId,'暂无趋势数据');
    text(noteId,'已启用旧版引擎降级展示，不影响基础配额与作业队列操作。');
    return;
  }
  const points=trendRaw.slice(-days).map(raw=>({
    date:String(raw?.date||''),
    cu:Math.max(0,Number(raw?.cu_used)||0),
    jobs:Math.max(0,Number(raw?.jobs)||0),
    succeeded:Math.max(0,Number(raw?.succeeded)||0),
    failed:Math.max(0,Number(raw?.failed)||0),
    cancelled:Math.max(0,Number(raw?.cancelled)||0)
  }));
  const totalCu=points.reduce((s,p)=>s+p.cu,0);
  const totalJobs=points.reduce((s,p)=>s+p.jobs,0);
  const totalSucc=points.reduce((s,p)=>s+p.succeeded,0);
  const totalFail=points.reduce((s,p)=>s+p.failed,0);
  const maxCu=points.reduce((m,p)=>Math.max(m,p.cu),0);
  const maxJobs=points.reduce((m,p)=>Math.max(m,p.jobs),0);
  const summaryLine='近 '+points.length+' 天合计：'+totalCu.toFixed(3)+' CU · '+totalJobs+' 次作业（成功 '+totalSucc+' / 失败 '+totalFail+'）';
  text(readoutId,summaryLine);

  const W=640,H=214,padL=46,padR=38,padT=18,padB=34;
  const plotW=W-padL-padR,plotH=H-padT-padB;
  const cuTop=maxCu>0?maxCu*1.18:1;
  const jobsTop=maxJobs>0?Math.max(maxJobs,2)*1.18:4;

  const svg=svgEl('svg',{viewBox:'0 0 '+W+' '+H,role:'img','aria-label':'近 '+points.length+' 天 Colab 每日 CU 消耗与作业数趋势图'},'cloud-chart-svg');
  const gridSteps=3;
  for(let step=0;step<=gridSteps;step++){
    const ratio=step/gridSteps;
    const y=padT+plotH*(1-ratio);
    svg.append(svgEl('line',{x1:padL,y1:y.toFixed(1),x2:W-padR,y2:y.toFixed(1)},step===0?'cu-axis-line':'cu-grid'));
    const cuVal=cuTop*ratio;
    const leftLbl=svgEl('text',{x:padL-6,y:(y+3.5).toFixed(1),'text-anchor':'end'},'cu-axis-text');
    leftLbl.textContent=cuVal>=10?cuVal.toFixed(1):cuVal.toFixed(2);
    svg.append(leftLbl);
    const rightLbl=svgEl('text',{x:W-padR+6,y:(y+3.5).toFixed(1),'text-anchor':'start'},'cu-axis-right');
    rightLbl.textContent=String(Math.round(jobsTop*ratio));
    svg.append(rightLbl);
  }
  const leftUnit=svgEl('text',{x:padL-6,y:11,'text-anchor':'end'},'cu-axis-text');
  leftUnit.textContent='CU';
  const rightUnit=svgEl('text',{x:W-padR+6,y:11,'text-anchor':'start'},'cu-axis-right');
  rightUnit.textContent='作业';
  svg.append(leftUnit,rightUnit);

  const count=points.length;
  const slotW=plotW/count;
  const barW=Math.max(4,Math.min(22,slotW*0.54));
  const barNodes=[];
  const dotNodes=[];
  const lineCoords=[];
  const tickStep=count>16?5:2;

  for(let i=0;i<count;i++){
    const pt=points[i];
    const cx=padL+(i+0.5)*slotW;
    const barH=pt.cu>0?Math.max(3,(pt.cu/cuTop)*plotH):2;
    const barY=padT+plotH-barH;
    const rect=svgEl('rect',{
      x:(cx-barW/2).toFixed(1),
      y:barY.toFixed(1),
      width:barW.toFixed(1),
      height:barH.toFixed(1),
      rx:pt.cu>0?'3':'1'
    },pt.cu>0?'cu-bar':'cu-bar-zero');
    svg.append(rect);
    barNodes.push(rect);

    const jy=padT+plotH-(pt.jobs/jobsTop)*plotH;
    lineCoords.push(cx.toFixed(1)+','+jy.toFixed(1));
    if(i===0||i===count-1||i%tickStep===0){
      const tick=svgEl('text',{x:cx.toFixed(1),y:(H-12).toFixed(1),'text-anchor':'middle'},'cu-axis-text');
      tick.textContent=pt.date.length>=10?pt.date.slice(5):pt.date;
      svg.append(tick);
    }
  }
  if(maxJobs>0&&lineCoords.length>1){
    svg.append(svgEl('polyline',{points:lineCoords.join(' ')},'cu-line'));
  }
  for(let i=0;i<count;i++){
    const pt=points[i];
    const cx=padL+(i+0.5)*slotW;
    const jy=padT+plotH-(pt.jobs/jobsTop)*plotH;
    const dot=svgEl('circle',{cx:cx.toFixed(1),cy:jy.toFixed(1),r:pt.jobs>0?'3.4':'2'},'cu-dot');
    if(maxJobs===0)dot.setAttribute('opacity','0.35');
    svg.append(dot);
    dotNodes.push(dot);
  }
  const setHover=idx=>{
    for(let j=0;j<count;j++){
      const hasCu=points[j].cu>0;
      barNodes[j].setAttribute('class',j===idx?'cu-bar-active':(hasCu?'cu-bar':'cu-bar-zero'));
      dotNodes[j].setAttribute('class',j===idx?'cu-dot-active':'cu-dot');
    }
    if(idx>=0&&idx<count){
      const pt=points[idx];
      text(readoutId,pt.date+' · 消耗 '+pt.cu.toFixed(3)+' CU · 作业 '+pt.jobs+' 次（成功 '+pt.succeeded+' / 失败 '+pt.failed+(pt.cancelled?' / 取消 '+pt.cancelled:'')+'）');
    }else{
      text(readoutId,summaryLine);
    }
  };
  for(let i=0;i<count;i++){
    const pt=points[i];
    const hit=svgEl('rect',{
      x:(padL+i*slotW).toFixed(1),
      y:String(padT),
      width:slotW.toFixed(1),
      height:String(plotH),
      tabindex:'0',
      role:'button',
      'aria-label':pt.date+' 消耗 '+pt.cu.toFixed(3)+' CU，作业 '+pt.jobs+' 次'
    },'cu-hit');
    const tip=svgEl('title');
    tip.textContent=pt.date+' · '+pt.cu.toFixed(3)+' CU · '+pt.jobs+' 次作业';
    hit.append(tip);
    hit.addEventListener('mouseenter',()=>setHover(i));
    hit.addEventListener('focus',()=>setHover(i));
    hit.addEventListener('click',()=>setHover(i));
    hit.addEventListener('mouseleave',()=>setHover(-1));
    hit.addEventListener('blur',()=>setHover(-1));
    svg.append(hit);
  }
  host.append(svg);
  if(totalCu===0&&totalJobs===0){
    text(noteId,'近 '+points.length+' 天内暂无 Colab 作业与 CU 消耗记录；提交学歌或训练任务后将自动绘制趋势。');
  }else{
    text(noteId,'左轴为每日 CU 消耗（柱状），右轴为每日作业次数（折线）；周期按当前统计时区自然日聚合。');
  }
}
function renderGpuBreakdownList(containerId,breakdown){
  const box=$(containerId);
  if(!box)return;
  box.replaceChildren();
  if(!Array.isArray(breakdown)){
    box.append(el('p','当前引擎未返回按 GPU 类型的消耗分布（旧版引擎降级）。','empty-state'));
    return;
  }
  if(!breakdown.length){
    box.append(el('p','暂无按 GPU 类型归属的作业记录。','empty-state'));
    return;
  }
  const totalJobs=breakdown.reduce((s,r)=>s+Math.max(0,Number(r?.jobs)||0),0);
  const totalCu=breakdown.reduce((s,r)=>s+Math.max(0,Number(r?.cu_used)||0),0);
  for(const item of breakdown){
    const gpu=String(item?.gpu||'UNKNOWN');
    const jobs=Math.max(0,Number(item?.jobs)||0);
    const succ=Math.max(0,Number(item?.succeeded)||0);
    const fail=Math.max(0,Number(item?.failed)||0);
    const cu=Math.max(0,Number(item?.cu_used)||0);
    const share=item?.cu_share!=null&&totalCu>0?Math.min(1,Math.max(0,Number(item.cu_share)||0)):(totalJobs>0?jobs/totalJobs:0);
    const isWarmEligible=gpu==='L4'||gpu==='T4';
    const row=el('div',undefined,'cloud-meter-row');
    const head=el('div',undefined,'cloud-meter-head');
    const title=el('span',undefined,'cloud-meter-title');
    title.append(pill(gpu,isWarmEligible?'ok':'paid'),el('span',jobs+' 次作业','cloud-meter-meta'));
    head.append(title,el('span',cu.toFixed(3)+' CU · '+pctText(share),'cloud-meter-val'));
    const bar=el('progress');
    bar.max=100;
    bar.value=share*100;
    bar.dataset.tone=isWarmEligible?'ok':'vertex';
    bar.setAttribute('aria-label',gpu+' CU 占比 '+pctText(share));
    const meta=el('div','成功 '+succ+' / 失败 '+fail+(isWarmEligible?' · 支持短时热保活':' · 高价卡禁保活'),'cloud-meter-meta');
    row.append(head,bar,meta);
    box.append(row);
  }
}
function renderAccelStatsList(containerId,noteId,accel){
  const box=$(containerId);
  if(!box)return;
  box.replaceChildren();
  if(!accel||typeof accel!=='object'){
    box.append(el('p','当前引擎未上报三级加速效率指标（Drive 直传 / 热机复用 / 权重缓存）。','empty-state'));
    text(noteId,'升级 Colab Worker 与控制面后将自动汇聚方案 1+2+3 命中率。');
    return;
  }
  const metrics=[
    {label:'Drive 直传（方案 1）',hits:Number(accel.drive_direct_jobs)||0,misses:Number(accel.drive_direct_fallbacks)||0,hitText:'直传',missText:'WS 回退',tone:'ok'},
    {label:'热机复用（方案 2）',hits:Number(accel.warm_reuse_hits)||0,misses:Number(accel.warm_cold_starts)||0,hitText:'热复用',missText:'冷启动',tone:'vertex'},
    {label:'高频权重缓存（方案 3）',hits:Number(accel.cache_weight_hits)||0,misses:Number(accel.cache_weight_misses)||0,hitText:'命中',missText:'未命中',tone:'ok'},
    {label:'Venv 镜像缓存（方案 3）',hits:Number(accel.cache_venv_hits)||0,misses:Number(accel.cache_venv_misses)||0,hitText:'命中',missText:'未命中',tone:'vertex'}
  ];
  for(const m of metrics){
    const total=m.hits+m.misses;
    const rate=total>0?m.hits/total:null;
    const row=el('div',undefined,'cloud-meter-row');
    const head=el('div',undefined,'cloud-meter-head');
    head.append(el('span',m.label,'cloud-meter-title'),el('span',rate!=null?pctText(rate):'暂无样本','cloud-meter-val'));
    const bar=el('progress');
    bar.max=100;
    bar.value=rate!=null?rate*100:0;
    bar.dataset.tone=m.tone;
    bar.setAttribute('aria-label',m.label+' 命中率 '+(rate!=null?pctText(rate):'暂无样本'));
    const meta=el('div',m.hitText+' '+m.hits+' 次 · '+m.missText+' '+m.misses+' 次','cloud-meter-meta');
    row.append(head,bar,meta);
    box.append(row);
  }
  const allowed=Array.isArray(accel.warm_allowed_gpus)&&accel.warm_allowed_gpus.length?accel.warm_allowed_gpus.join('/'):'L4/T4';
  text(noteId,'已观测作业 '+Number(accel.measured_jobs||0)+' 次 · 热保活'+(accel.warm_pool_enabled?('开启（'+allowed+' · TTL '+formatTtlCompact(accel.warm_idle_ttl_seconds||360)+' · 当前活跃 '+Number(accel.warm_active_count||0)+'）'):'未开启'));
}
function renderColabAnalyticsOnPage(colab,prefix){
  const norm=normalizeColabAnalytics(colab);
  const w=colab?.worker||{};
  const isConfigured=Boolean(colab?.configured);
  const isReachable=Boolean(w.reachable);
  renderWarmSessionsList(prefix+'-warm-sessions',norm?.warmSessions||[],norm?.accel||null,isReachable,isConfigured);
  renderCuTrendChart(prefix+'-cu-chart',prefix+'-trend-readout',prefix+'-cu-chart-note',norm?.cuTrend||null,cloudTrendDays);
  renderGpuBreakdownList(prefix+'-gpu-breakdown',norm?.gpuBreakdown??null);
  renderAccelStatsList(prefix+'-accel-stats',prefix+'-accel-note',norm?.accel||null);
}
function renderUnifiedCloudGcp(gcp,bal,dashUsage){
  const c=gcp?.config||{},bq=gcp?.bigquery||{},mon=gcp?.monitoring||{};
  const rec=resolveGcpReconciliation(gcp,dashUsage);
  const bqSynced=Boolean(bq.synced);
  const initial=Number(bq.initial_credits_usd??c.initial_credits_usd??0);
  const used=Number(bq.credits_used_usd??0);
  const bqExhausted=Boolean(bq.exhausted||(c.enforce_credit_hard_stop&&bq.realtime_remaining_usd!=null&&Number(bq.realtime_remaining_usd)<=0));
  const rescuedByManual=Boolean(bqExhausted&&bal?.mode==='merged'&&!bal?.exhausted);
  const hardStopped=Boolean(bqExhausted&&!rescuedByManual);

  $('cloud-gcp-pills').replaceChildren(
    bqSynced?pill('BigQuery 已同步','ok'):(c.bigquery_enabled?pill('BigQuery 等待首次同步','warn'):pill('BigQuery 未启用','muted')),
    gcpReconcilePill(rec.status),
    monStatusPill(mon.status||rec.monitoring_status),
    hardStopped?pill('赠金硬熔断已触发','err'):rescuedByManual?pill('赠金耗尽 · 手动余额接管','warn'):c.enforce_credit_hard_stop?pill('硬熔断保护开启','warn'):pill('仅监测不硬熔断','muted')
  );
  const utilPct=initial>0?Math.min(100,Math.max(0,(used/initial)*100)):0;
  const bar=$('cloud-gcp-credit-bar');
  bar.value=bqSynced&&initial>0?utilPct:0;
  bar.dataset.level=hardStopped||utilPct>=95?'err':utilPct>=80?'warn':'normal';
  text('cloud-gcp-credit-label',initial>0?(bqSynced?(usd(used,4)+' / '+usd(initial,2)+'（已抵扣 '+utilPct.toFixed(1)+'%）'):('已配置赠金 '+usd(initial,2)+' · 尚未同步 BigQuery')):'尚未配置初始赠金总额');

  const basisLabel=rec.local_reference_basis==='estimated'?'本地已确认估算':'本地账单扣账';
  let deltaNote='等待 BigQuery 首次同步';
  let deltaVariant='';
  if(bqSynced&&rec.cost_delta_usd!=null){
    if(rec.status==='in_sync'){
      deltaNote='偏差 '+pctText(rec.cost_delta_ratio)+' · 账单实花与'+basisLabel+'一致';
      deltaVariant='ok';
    }else if(rec.status==='export_lag'){
      deltaNote='含未结算增量后差额 '+signedUsd(rec.cost_delta_after_unsettled_usd,4)+'（导出延迟正常）';
      deltaVariant='warn';
    }else{
      deltaNote='偏差 '+pctText(rec.cost_delta_ratio)+' · 请核对其他 GCP 服务或定价';
      deltaVariant='err';
    }
  }
  $('cloud-gcp-kpis').replaceChildren(
    kpiCard('GCP 赠金总额 / 已用',usd(initial,2)+' / '+usd(used,4),initial>0?('抵扣率 '+(bqSynced?pctText(used/initial):'待同步')):'在下方 BigQuery 表单配置初始赠金',''),
    kpiCard('GCP 赠金实时剩余',bqSynced?usd(bq.realtime_remaining_usd,4):'未同步',bqSynced?('已结算 '+usd(bq.settled_remaining_usd,4)+' − 本地未结算 '+usd(bq.unsettled_local_usd,6)):'点击「同步 BigQuery」拉取最新余额',hardStopped?'err':rescuedByManual?'warn':(bqSynced?'ok':'')),
    kpiCard('账单实花 vs 本地估算',(bqSynced?usd(rec.billing_actual_usd,4):'未同步')+' / '+usd(rec.local_reference_usd,4),'BigQuery 毛费用（净自付 '+(bqSynced?usd(rec.billing_net_usd,4):'—')+'）vs '+basisLabel,'accent'),
    kpiCard('账单对账差额（实花 − 估算）',bqSynced?signedUsd(rec.cost_delta_usd,4):'待同步',deltaNote,deltaVariant)
  );
  const lagSec=bq.latest_usage_end_ts?Math.max(0,Math.round(Date.now()/1000-bq.latest_usage_end_ts)):null;
  const metaParts=[
    'BigQuery 最近同步：'+dateTime(bq.last_sync_at||bq.synced_at),
    bq.latest_usage_end_ts?('账单水位：'+dateTime(bq.latest_usage_end_ts)+'（延迟约 '+Math.floor(lagSec/60)+' 分钟）'):'尚无账单水位',
    'Monitoring 最近对账：'+dateTime(mon.last_sync_at||mon.synced_at)+(mon.delta_tokens!=null?'（Token 差额 '+mon.delta_tokens+'）':'')
  ];
  if(bq.last_error)metaParts.push('BigQuery 错误：'+bq.last_error);
  if(mon.last_error)metaParts.push('Monitoring 错误：'+mon.last_error);
  text('cloud-gcp-sync-meta',metaParts.join(' · '));
  $('cloud-sync-bq').disabled=false;
  $('cloud-sync-mon').disabled=false;
}
function renderUnifiedCloudColab(colab,unavailReason=''){
  syncTrendButtons();
  const pillsBox=$('cloud-colab-pills'),kpiBox=$('cloud-colab-kpis');
  if(!pillsBox||!kpiBox)return;
  if(!colab){
    pillsBox.replaceChildren(...[
      pill('Colab 算力暂不可用','muted'),
      unavailReason?pill(unavailReason,'warn'):null
    ].filter(Boolean));
    kpiBox.replaceChildren(
      kpiCard('Colab CU 余额','—','Colab Worker 未配置或离线',''),
      kpiCard('今日作业','—','等待连接 Colab 控制面',''),
      kpiCard('近 '+cloudTrendDays+' 天 CU 消耗','—','暂无趋势数据',''),
      kpiCard('热会话保活','0 活跃','仅 L4/T4 支持短时热保活','')
    );
    renderColabAnalyticsOnPage(null,'cloud');
    return;
  }
  const w=colab.worker||{},c=colab.config||{},t=colab.today||{};
  const norm=normalizeColabAnalytics(colab);
  const sum=norm.summary;
  const warm=norm.warmSessions;
  const accel=norm.accel;
  const cuAvail=w.compute_units!=null?Number(w.compute_units):null;
  const jobsCount=Number(t.jobs_count??0);
  const todayCu=Number(t.cu_used??0);
  const windowCu=cloudTrendDays===14?(sum.last_14d_cu_used??null):(sum.last_30d_cu_used??null);
  const windowJobs=cloudTrendDays===14?(sum.last_14d_jobs??null):(sum.last_30d_jobs??null);
  const warmHead=warm[0]||null;

  pillsBox.replaceChildren(
    colab.configured?(w.reachable?pill('Worker 在线','ok'):pill('Worker 离线','err')):pill('未配置 COLAB_WORKER_URL','muted'),
    c.enabled?pill('调度开启 · 按模块自选 GPU','vertex'):pill('算力调度已暂停','warn'),
    warm.length>0?pill('热机保活中 · '+warm.length+' 会话（'+String(warmHead.gpu||'L4')+' 剩余 '+formatTtlCompact(warmHead.remaining_ttl_seconds)+'）','ok'):(accel?.warm_pool_enabled?pill('热保活待命（'+((accel.warm_allowed_gpus||['L4','T4']).join('/'))+'）','muted'):pill('热保活未启用','muted'))
  );
  const cuNote=w.reachable?'账号实时 Compute Units 余额':(w.error?('Worker 离线：'+(colabErrorLabels?.[w.error]||w.error)):'Worker 离线时暂不可查询');
  kpiBox.replaceChildren(
    kpiCard('Colab CU 余额',cuAvail!=null?cuAvail.toFixed(2)+' CU':'未知',cuNote,cuAvail!=null?(cuAvail<2?'warn':'ok'):''),
    kpiCard('今日作业',jobsCount+' 次','累计 '+Number(sum.total_jobs??0)+' 次 · 成功率 '+(sum.success_rate!=null?pctText(sum.success_rate):'—')+(sum.derived_legacy?'（近 30 条估算）':''),''),
    kpiCard('近 '+cloudTrendDays+' 天 CU 消耗',windowCu!=null?Number(windowCu).toFixed(3)+' CU':todayCu.toFixed(2)+' CU（今日）',windowCu!=null?('近 '+cloudTrendDays+' 天 '+Number(windowJobs??0)+' 次作业 · 累计 '+Number(sum.total_cu_used??0).toFixed(3)+' CU'):'旧版引擎暂未返回周期聚合','accent'),
    kpiCard('热会话保活',warm.length>0?(warm.length+' 活跃 · '+String(warmHead.gpu||'L4')):'0 活跃',warm.length>0?('剩余 TTL '+formatTtlCompact(warmHead.remaining_ttl_seconds)+' · 复用 '+Number(warmHead.reuse_count||0)+' 次'):(accel?.warm_pool_enabled?'L4/T4 完工后短时保活':'H100/A100/G4 不保活'),warm.length>0?'ok':'')
  );
  renderColabAnalyticsOnPage(colab,'cloud');
}
function setCloudTrendDays(days){
  const next=days===30?30:14;
  if(cloudTrendDays===next)return;
  cloudTrendDays=next;
  syncTrendButtons();
  renderUnifiedCloudColab(cachedColabOverview,colabUnavailableReason);
  renderColabAnalyticsOnPage(cachedColabOverview,'colab');
}
for(const [id,dVal] of [['cloud-trend-14d',14],['cloud-trend-30d',30],['colab-trend-14d',14],['colab-trend-30d',30]]){
  $(id).addEventListener('click',()=>setCloudTrendDays(dVal));
}

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

function renderOverviewRoutingGcp(d,prov,gcp,colab){
  const box=$('overview-routing-gcp'),pills=$('overview-routing-pills'),strip=$('overview-gcp-strip');
  pills.replaceChildren();strip.replaceChildren();strip.hidden=true;text('overview-gcp-note','');
  const pCfg=prov?.config||d.provider_routing||null,pCreds=prov?.credentials||null;
  const gSummary=d.budget_status?.gcp_billing||null;
  const gCfg=gcp?.config||null,bq=gcp?.bigquery||null,mon=gcp?.monitoring||null;
  const u=resolveUnifiedBalance(d.budget_status?.account_balance,gcp,gSummary);
  const colabConfigured=Boolean(colab?.configured);
  const routingActive=Boolean(pCfg&&(pCfg.vertex_enabled||pCreds?.gemini_free_configured||pCreds?.vertex_api_key_configured||pCreds?.gcp_service_account_configured));
  const gcpEnabled=Boolean(gSummary?.bigquery_enabled||gSummary?.monitoring_enabled||gSummary?.configured||gCfg?.bigquery_enabled||gCfg?.monitoring_enabled||bq?.synced);
  const balActive=u.mode!=='none';
  if(!routingActive&&!gcpEnabled&&!balActive&&!colabConfigured){box.hidden=true;return;}
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
  const colabNorm=colabConfigured?normalizeColabAnalytics(colab):null;
  const colabWorker=colab?.worker||{};
  const warmList=colabNorm?.warmSessions||[];
  if(colabConfigured){
    const cuVal=colabWorker.compute_units!=null?Number(colabWorker.compute_units):null;
    pills.append(colabWorker.reachable?pill('Colab 在线'+(cuVal!=null?' · '+cuVal.toFixed(2)+' CU':''),'ok'):pill('Colab Worker 离线','warn'));
    if(warmList.length>0)pills.append(pill('热机保活 '+warmList.length+' · '+String(warmList[0].gpu||'L4'),'vertex'));
  }
  if(balActive||(bqActive&&bqView)||colabConfigured){
    strip.hidden=false;
    if(balActive||(bqActive&&bqView)){
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
    if(colabConfigured){
      const cuVal=colabWorker.compute_units!=null?Number(colabWorker.compute_units):null;
      const tToday=colab.today||{};
      const warmNote=warmList.length>0?('热机 '+String(warmList[0].gpu||'L4')+' 剩余 '+formatTtlCompact(warmList[0].remaining_ttl_seconds)):('今日 '+Number(tToday.jobs_count??0)+' 次作业 · '+Number(tToday.cu_used??0).toFixed(2)+' CU');
      strip.append(kpiCard('Colab CU 余额',cuVal!=null?cuVal.toFixed(2)+' CU':(colabWorker.reachable?'未知':'离线'),warmNote,warmList.length>0?'ok':(cuVal!=null&&cuVal<2?'warn':'')));
    }
  }
}

const laneNames={chat:'主对话',patrol:'B 站巡逻',memory:'记忆',memory_flex:'记忆 Flex',admin:'管理测试',proactive:'世界引擎与主动生成',search:'联网检索'};
const categoryNames={chat:'主对话',world_plan:'世界引擎规划',minecraft:'Minecraft 游玩',qq_proactive:'主动社交（群/私信）',owner_digest:'Owner 晚间日报',screen_watch:'屏幕伴看',voice_companion:'语音伴随',proactive_other:'其他主动生成',patrol:'B 站巡逻',patrol_diary:'巡逻日记',memory:'记忆提炼',memory_flex:'记忆 Flex',admin:'管理测试',search:'联网检索'};
const percent=new Intl.NumberFormat('zh-CN',{style:'percent',maximumFractionDigits:1});
function renderLanes(lanes,total){
  const rows=[...lanes].sort((a,b)=>b.amount-a.amount||b.calls-a.calls);
  $('lanes-empty').hidden=rows.length>0;
  $('lane-stats').replaceChildren(...rows.map(item=>{
    const key=item.category||item.lane;
    const label=categoryNames[key]||laneNames[key]||key;
    const row=el('div',undefined,'lane'),track=el('dd'),bar=el('progress');
    bar.max=100;
    bar.value=total>0?100*item.amount/total:0;
    bar.setAttribute('aria-label',label+'占当日分项费用的比例');
    track.append(bar);
    const amount=el('dd',undefined,'lane-amount');
    amount.append(el('span',usd(item.amount/1e6)),el('small',item.calls+' 次'));
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
  let d,prov=cachedProvidersOverview,gcp=cachedGcpOverview,colab=cachedColabOverview;
  if(lightweight){
    d=initial??await fetchOverviewPayload();
    if(!token||signal.aborted)return;
  }else{
    const [dashRes,provRes,gcpRes,colabRes]=await Promise.allSettled([
      initial??fetchOverviewPayload(),
      api('global/providers'),
      api('gcp-billing'),
      api('colab')
    ]);
    if(dashRes.status!=='fulfilled')throw dashRes.reason;
    if(!token||signal.aborted)return;
    d=dashRes.value;
    if(provRes.status==='fulfilled'){prov=provRes.value;cachedProvidersOverview=prov;}
    if(gcpRes.status==='fulfilled'){gcp=gcpRes.value;cachedGcpOverview=gcp;}
    if(colabRes.status==='fulfilled'){colab=colabRes.value;cachedColabOverview=colab;colabUnavailableReason='';}
    else{colab=null;}
  }
  displayZone=d.usage.timezone||d.usage.budget_timezone||'Asia/Shanghai';
  text('sidebar-zone',zoneLabel(displayZone));
  stampDashboard();
  renderRestHours(d.rest_hours);
  showNotebookAvailability(Boolean(d.notebook?.enabled));
  const usage=d.usage,lanes=usage.lanes||[];
  const breakdown=Array.isArray(usage.categories)&&usage.categories.length?usage.categories:lanes;
  const laneTotal=lanes.reduce((sum,lane)=>sum+lane.amount,0);
  const breakdownTotal=breakdown.reduce((sum,item)=>sum+item.amount,0);
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
  renderLanes(breakdown,breakdownTotal);
  renderOverviewRoutingGcp(d,prov,gcp,colab);
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
  $('gcp-mon-pills').replaceChildren(...[
    c.monitoring_enabled?pill('Monitoring 定期对账开启','ok'):pill('Monitoring 定期对账关闭','muted'),
    monStatusPill(mon.status),
    mon.drift_ratio!=null?pill('偏差率 '+(Number(mon.drift_ratio)*100).toFixed(2)+'%',mon.status==='drift_detected'?'err':'muted'):null
  ].filter(Boolean));
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
  renderUnifiedCloudGcp(d,accountBalanceState,lastLedgerDash?.usage);
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
  $('unified-balance-pills').replaceChildren(...[
    modePills[u.mode]||pill('模式: '+u.mode,'muted'),
    u.gcpSynced?pill('BigQuery 已同步','ok'):pill('BigQuery 未同步','muted'),
    u.manualCfg?pill('手动余额已配置','paid'):pill('手动余额未设定','muted'),
    u.exhausted?pill('统一余额已耗尽 / 触发熔断','err'):(u.mode!=='none'?pill('当前可用余额正常','ok'):null)
  ].filter(Boolean));
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
$('cloud-sync-bq').addEventListener('click',e=>busy(e.target,async()=>{
  const g=await api('gcp-billing/sync-bigquery','POST');
  await refreshUnifiedAfterGcp(g);
  notice('BigQuery 账单同步完成');
}));
$('cloud-sync-mon').addEventListener('click',e=>busy(e.target,async()=>{
  const g=await api('gcp-billing/sync-monitoring','POST');
  renderGcpBilling(g);
  notice('Cloud Monitoring 对账完成');
}));
$('cloud-refresh').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('ledger','刷新统一云资源')))return;
  const [gcpRes,balRes,colabRes]=await Promise.allSettled([api('gcp-billing'),api('account-balance'),api('colab')]);
  if(!token)return;
  if(balRes.status==='fulfilled')accountBalanceState=balRes.value;
  if(gcpRes.status==='fulfilled'){
    gcpBillingState=gcpRes.value;
    renderGcpBilling(gcpRes.value);
    renderUnifiedBalance(accountBalanceState,gcpRes.value,null);
  }else{
    renderUnifiedCloudGcp(gcpBillingState,accountBalanceState,lastLedgerDash?.usage);
  }
  if(colabRes.status==='fulfilled'){
    cachedColabOverview=colabRes.value;
    colabUnavailableReason='';
    renderUnifiedCloudColab(cachedColabOverview,'');
  }else{
    cachedColabOverview=null;
    const rawMsg=colabRes.reason?.message||'colab_unconfigured';
    colabUnavailableReason=colabErrorLabels?.[rawMsg]||rawMsg;
    renderUnifiedCloudColab(null,colabUnavailableReason);
  }
  notice('已刷新统一云资源总览');
}));
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
  const [dashRes,polRes,gcpRes,balRes,colabRes]=await Promise.allSettled([api('global/dashboard'),api('budget'),api('gcp-billing'),api('account-balance'),api('colab')]);
  if(dashRes.status!=='fulfilled')throw dashRes.reason;
  if(polRes.status!=='fulfilled')throw polRes.reason;
  const d=dashRes.value,policy=polRes.value;
  if(!token)return;
  lastLedgerDash=d;
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
  table('budget-periods',['范围','账单扣账额','上限'],[['今日 '+(b.day_local||b.day||b.day_utc||''),money(b.daily),money(b.daily_limit)],['本月 '+(b.month_local||b.month||b.month_utc||''),money(b.monthly),money(b.monthly_limit)],['累计总上限（.env）',money(b.total),Number(b.total_limit)===0?'未设累计上限':money(b.total_limit)],['单用户今日最高',money(b.user_top_daily||0),money(b.user_daily_limit??0)],['B 站巡逻今日',money(sb.patrol?.used||0),money(sb.patrol?.limit??0)],['记忆提炼今日',money(sb.memory?.used||0),money(sb.memory?.limit??0)],['世界引擎与主动生成今日',money(sb.proactive?.used||0),money(sb.proactive?.limit??0)],['管理沙盒今日',money(sb.admin?.used||0),money(sb.admin?.limit??0)]]);
  if(gcpVal)renderGcpBilling(gcpVal);
  else renderUnifiedCloudGcp(null,balVal,usage);
  if(colabRes.status==='fulfilled'){
    cachedColabOverview=colabRes.value;
    colabUnavailableReason='';
    renderUnifiedCloudColab(cachedColabOverview,'');
  }else{
    cachedColabOverview=null;
    const rawReason=colabRes.reason?.message||'colab_unconfigured';
    colabUnavailableReason=colabErrorLabels?.[rawReason]||rawReason;
    renderUnifiedCloudColab(null,colabUnavailableReason);
  }
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
  cachedColabOverview=null;
  colabUnavailableReason='';
  lastLedgerDash=null;
  cloudTrendDays=14;
  syncTrendButtons();
  for(const id of ['stat-calls','stat-injection','stat-noise','stat-uptime','budget-spend','budget-left','pipeline-state','image-cache'])text(id,'—');
  for(const id of ['overview-day','budget-limit','budget-label','budget-percent','rate-label','overview-gcp-note','unified-balance-note','balance-status','gcp-bq-status','gcp-bq-lag-info','gcp-sa-status','gcp-mon-status','budget-source','ledger-coverage','cache-alert','cloud-gcp-sync-meta','cloud-cu-chart-note','cloud-accel-note','colab-cu-chart-note','colab-accel-note'])text(id,'');
  for(const id of ['cloud-trend-readout','colab-trend-readout'])text(id,'悬停或选中柱条查看单日明细');
  text('cloud-gcp-credit-label','等待同步');
  $('cloud-gcp-credit-bar').value=0;
  delete $('cloud-gcp-credit-bar').dataset.level;
  $('overview-day').removeAttribute('datetime');
  $('budget').value=0;
  text('rest-overview-badge','作息读取中');
  $('overview-routing-gcp').hidden=true;
  $('overview-gcp-strip').hidden=true;
  text('gcp-bq-sql','配置 billing_table 后可预览查询 SQL。');
  for(const id of ['lane-stats','overview-routing-pills','overview-gcp-strip','unified-balance-pills','unified-balance-strip','gcp-bq-pills','gcp-kpi-strip','gcp-mon-pills','gcp-grants-list','token-ledger','work-ledger','provider-ledger','cost-ledger','budget-periods','model-ledger','thinking-ledger','effective-cache','gcp-credits-table','gcp-mon-compare-table','gcp-mon-models-table','cloud-gcp-pills','cloud-gcp-kpis','cloud-colab-pills','cloud-colab-kpis','cloud-warm-sessions','cloud-cu-chart','cloud-gpu-breakdown','cloud-accel-stats','colab-warm-sessions','colab-cu-chart','colab-gpu-breakdown','colab-accel-stats'])$(id).replaceChildren();
  for(const id of ['save-balance','clear-balance','gcp-sync-bq','gcp-sync-monitoring','gcp-save-config','gcp-reset-config','gcp-save-mon','gcp-save-sa','gcp-clear-sa','cloud-sync-bq','cloud-sync-mon'])$(id).disabled=true;
});
