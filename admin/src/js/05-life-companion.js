const dmStatus={pending:'等待投递',sent:'已发送并处理记忆',delivered:'已确认发送，等待记忆写入',uncertain:'发送结果不确定，不重发',expired:'已过期',skipped:'已跳过',failed:'生成未完成',queued:'已排队',generating:'生成中',cancelled:'已取消',not_queued:'未入队'};
const deliveryStages={generating:'生成中',generated:'已生成',queued:'已入队',awaiting_qq:'等待 QQ 上线',awaiting_channel:'等待投递通道上线',submitting:'等待平台回执',confirmed:'平台已确认',expired:'已过期',generation_uncertain:'生成结果待核实',uncertain:'回执待核实',failed:'生成失败',cancelled:'已取消'};
const dmKindLabels={companion:'日常陪伴',digest:'每日汇报',alert:'异常告警',test:'通路验证'};

let activeProactiveBase='proactive';
async function proactive(){
  let d=await api('proactive');
  if(!token)return;
  let base='proactive';
  if(d&&d.assigned===false){
    try{
      const mods=await api('life/modules');
      if(!token)return;
      const ownerMod=Array.isArray(mods)?mods.find(m=>m&&m.owner_delivery===true):null;
      if(ownerMod&&ownerMod.id){
        const modBase='life/modules/'+encodeURIComponent(ownerMod.id)+'/owner-delivery';
        const alt=await api(modBase);
        if(!token)return;
        if(alt&&alt.assigned!==false&&(alt.platform||alt.budget)){
          d=alt;
          base=modBase;
        }
      }
    }catch(err){
      if(err?.name==='AbortError')throw err;
    }
  }
  const assigned=Boolean(d&&d.assigned!==false);
  activeProactiveBase=assigned?base:'proactive';
  for(const id of ['dm-test','dm-now'])$(id).disabled=!assigned;
  if(!assigned){
    text('proactive-state','Owner 投递已分配给其他社交模块。');
    for(const id of ['proactive-stats','proactive-budget','proactive-queue','proactive-alerts'])$(id).replaceChildren();
    return;
  }
  const fmt=t=>t?lifeFmt(t,d.timezone):'—';
  const chanLabel=d.platform==='onebot'?'NapCat (QQ)':d.platform==='telegram'?'Telegram (telegram)':(d.platform||'投递通道');
  const stats=[[chanLabel,d.connected?'在线':'等待连接'],['下次日报',fmt(d.next_digest_at)]];
  $('proactive-stats').replaceChildren(...stats.map(([k,v])=>{const n=el('div',undefined,'stat');n.append(el('small',k),el('strong',v));return n;}));
  text('proactive-state',d.day+' · '+zoneLabel(d.timezone)+' · '+(!d.configured?'尚未配置 Owner 投递目标':d.delivery_enabled?'允许投递':'已停用投递'));
  const budget=d.budget||{total:0,hard_stop:false};
  text('proactive-budget','主动生成日预算 '+usd(d.proactive_daily_limit_usd,2)+' · 本地累计 '+usd(budget.total/1e6,4)+(budget.hard_stop?' · 付费生成已熔断':'')+'（含预留，不代表 Google 赠金余额）');
  table('proactive-queue',['类型','状态','数量'],(d.queue||[]).map(v=>[dmKindLabels[v.kind]||v.kind,dmStatus[v.status]||v.status,v.n]));
  table('proactive-alerts',['告警','入队时间','状态','冷却截止'],(d.alerts||[]).map(v=>[v.alert_key,fmt(v.queued_at),dmStatus[v.status]||v.status,fmt(v.cooldown_until)]));
}
$('refresh-proactive').addEventListener('click',e=>busy(e.target,proactive));
for(const [id,action] of [['dm-test','test'],['dm-now','digest']]){
  $(id).addEventListener('click',e=>busy(e.target,async()=>{
    notice(action==='test'?'正在验证私信通路…':'正在准备今日汇报…');
    const result=await api(activeProactiveBase+'/'+action,'POST');
    const state=result.delivery&&result.delivery!=='not_queued'?result.delivery:result.status;
    const label=deliveryStages[result.receipt?.stage]||dmStatus[state]||state;
    notice(label+(result.reason?' · '+result.reason:''));
    await proactive();
  }));
}

const lifeIntentStatus={pending:'待触发',executing:'执行中',done:'已完成',gated_skip:'门控跳过',expired:'已过期',uncertain:'结果不确定'};
const lifeIntentVariant={pending:'warn',executing:'paid',done:'ok',gated_skip:'muted',expired:'muted',uncertain:'err'};
function lifeFmt(t,tz){
  if(!t)return '—';
  try{return new Intl.DateTimeFormat('zh-CN',{timeZone:tz||'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(new Date(t*1000));}
  catch{return dateTime(t);}
}
function lifeSummaryJson(obj){
  if(obj===null||obj===undefined)return '—';
  if(typeof obj==='string')return obj.length>72?obj.slice(0,72)+'…':(obj||'—');
  if(typeof obj==='object'){
    if(typeof obj.note==='string'&&Object.keys(obj).length===1)return obj.note.length>72?obj.note.slice(0,72)+'…':(obj.note||'—');
    const raw=JSON.stringify(obj);
    return raw==='{}'?'—':(raw.length>88?raw.slice(0,88)+'…':raw);
  }
  return String(obj);
}
const planLabels={
  morning:'晨间行程',planned:'已规划',scheduled:'等待下次规划',not_due:'尚未到点',resting:'本轮留白',agenda_revised:'已调整行程',agenda_kept:'沿用已有行程',agenda_moved:'已改期',agenda_cancelled:'已取消',
  agenda_item_unavailable:'事项已开始或不属于本轮待办',invalid_agenda_time:'改期超出可用时段',
  agenda_duplicate:'同一时刻已安排该动作',
  accepted:'已采纳',all_filtered:'所有建议均被过滤',day_rest:'白天小憩',day_rest_unavailable:'当前不适合小憩',model_chose_idle:'选择清醒空闲',model_chose_rest:'模型未安排行动（旧记录）',local_rest:'本地未安排行动（旧记录）',
  unchanged_state:'状态与可用动作未变，延后付费规划',no_available_actions:'当前没有可用动作',
  sleeping:'休眠时段',disabled:'引擎关闭',running:'规划进行中',prepared:'规划已保存，待登记动作',
  interrupted:'规划中断',local_idle:'本地延后',local_fallback:'本地规划',llm:'模型规划',pending:'处理中',
  manual:'手动',timer:'定时',milestone:'新事件',exploration:'自主探索',opportunities_changed:'可用动作变化',unchanged:'状态未变',
  model_unavailable_or_invalid:'模型不可用或返回无效结果',planner_budget_closed:'规划预算已用尽',
  process_interrupted:'运行时中断',empty_note:'缺少动机',prewritten_speech:'建议含预写发言',
  recipient_unavailable:'接收人不可用',invalid_action:'动作无效',unknown_action:'动作未注册',unavailable_action:'动作当前不可用',
  window_expired:'执行窗口已过',invalid_ticket:'任务数据无效',round_cap:'超过单轮名额'
};
const planLabel=value=>planLabels[value]||value||'—';
function planCost(c){
  const parts=[];
  if(c.estimated_microdollars!=null)parts.push('已知估算 '+money(c.estimated_microdollars));
  if(c.unknown_calls)parts.push(c.unknown_calls+' 笔费用未知，账本暂计 '+money(c.unknown_budget_microdollars));
  if(c.open_reservations)parts.push(c.open_reservations+' 笔预留 '+money(c.reserved_microdollars));
  if(c.missing)parts.push('账本明细缺失');
  return parts.join(' · ')||'暂无明细';
}
function planReasons(r){
  return Object.entries(r.reasons||{}).filter(([k])=>k!==r.outcome).map(([k,n])=>planLabel(k)+' ×'+n).join('、');
}
function renderWorldObservation(o,eng,tz){
  $('world-observation-card').hidden=!o;
  if(!o)return;
  const day=o.today,latest=o.recent_plans[0],job=o.next_action;
  const morning=o.morning_plan;
  text('world-morning',morning?'晨间规划 '+lifeFmt(morning.started,tz)+' · '+planLabel(morning.source)+(morning.source==='llm'?' · '+morning.thinking_level:'')+' · '+planLabel(morning.outcome):'今天尚无晨间规划');
  table('world-agenda',['计划时间','活动','意图','进度'],(o.agenda||[]).map(a=>[
    lifeFmt(a.execute_after,tz),a.action_name,a.note||'—',
    ['agenda_cancelled','agenda_moved'].includes(a.result_reason)?planLabel(a.result_reason):(lifeIntentStatus[a.status]||a.status)
  ]));
  const kpis=[['今日规划',day.runs+' 轮'],['候选 / 采纳',day.candidates+' / '+day.accepted],['本地省下的付费规划',day.avoided+' 轮'],['今日规划费用',planCost(day.cost)]];
  $('world-day-stats').replaceChildren(...kpis.map(([k,v])=>{const stat=el('div',undefined,'stat');stat.append(el('small',k),el('strong',v));return stat;}));
  text('world-next','下次规划 '+lifeFmt(eng.next_plan_at,tz)+' · '+(job?(job.origin==='timer'?'下个定时动作 ':'下个待执行动作 ')+(job.label||job.action)+'，可执行时间 '+lifeFmt(job.execute_after,tz):'当前没有待执行动作'));
  text('world-quiet',latest?'上轮规划 '+lifeFmt(latest.started,tz)+' · '+planLabel(latest.outcome)+(planReasons(latest)?' · '+planReasons(latest):''):'尚未记录规划');
  $('world-daily-actions').replaceChildren(...(o.today_actions.length?o.today_actions.map(a=>pill(a.action+' · '+(lifeIntentStatus[a.status]||a.status)+' '+a.count,lifeIntentVariant[a.status]||'muted')):[el('span','今天尚无动作状态更新','muted')]));
  table('world-plans',['时间','触发 / 来源','候选 / 采纳','结果及原因','规划费用'],o.recent_plans.map(r=>[
    lifeFmt(r.started,tz),planLabel(r.trigger)+' / '+planLabel(r.source),r.candidates+' / '+r.accepted,
    [planLabel(r.outcome),planReasons(r)].filter(Boolean).join(' · '),planCost(r.cost)
  ]));
  text('world-day-coverage','已结束的观测日 '+o.baseline_days.length+' / 7'+(o.unobserved_recent_days.length?' · 最近七个自然日无采样 '+o.unobserved_recent_days.join('、'):''));
  table('world-days',['观测日','首次 / 最近采样','规划轮数','省下付费规划','候选 / 采纳','规划费用'],o.baseline_days.map(r=>[
    r.day,lifeFmt(r.first_tick,tz)+' / '+lifeFmt(r.last_tick,tz),r.runs,r.avoided,r.candidates+' / '+r.accepted,planCost(r.cost)
  ]));
  const progress=Object.values(o.delivery_progress);
  text('world-evening-coverage',progress.length?progress.map(r=>r.label+' · 已结束 '+r.closed+' / 7 晚 · 90 秒内启动 '+r.timely+' 晚 · 有送达回执 '+r.confirmed+' 晚 · 两项均达标 '+r.successful+' 晚'+(r.unobserved_recent_days.length?' · 近七晚无时段采样 '+r.unobserved_recent_days.join('、'):'')).join('；'):'尚无已结束且实际观测到的投递时段');
  table('world-evenings',['日期 / 安排','到期时间','生成或投递启动','送达确认','观察结果'],[...o.open_windows,...o.delivery_windows].map(w=>[
    w.day+' · '+w.label,lifeFmt(w.due,tz),w.started==null?'尚无开始记录':lifeFmt(w.started,tz)+(w.started<w.due?'（提前启动）':'（延迟 '+Math.ceil(w.start_delay_seconds)+' 秒）'),
    w.delivery_confirmed?lifeFmt(w.confirmed,tz):deliveryStages[w.stage]&&w.stage!=='confirmed'?deliveryStages[w.stage]:'尚无带时间的回执',
    !w.closed?'时段进行中':w.successful?'已达标':w.within_90s===false?'启动超过 90 秒':w.delivery_confirmed?'启动或窗口待核实':'尚无达标回执'
  ]));
}
function renderSocialSchedule(values,spMeta){
  const v=values||{};
  const meta=spMeta||{};
  $('social-owner-delivery').checked=v.owner_delivery_enabled!==undefined?Boolean(v.owner_delivery_enabled):true;
  $('social-owner-unlimited').checked=Boolean(v.owner_dms_unlimited);
  $('social-group-cooldown').value=v.group_cooldown_seconds??1500;
  $('social-group-window').value=v.group_active_window_seconds??1800;
  $('social-group-min-msgs').value=v.group_min_recent_messages??2;
  $('social-desktop-suppress').value=v.desktop_active_suppress_seconds??7200;
  const parts=['托管模块：social_presence（社交主动）'];
  parts.push(meta.enabled===false?'模块已停用':'模块已启用');
  if(meta.proactive_paused)parts.push('高专注已暂停主动');
  const src=meta.source||meta.scheduling?.source;
  if(src){
    const srcLabel=(src==='admin'||src==='sqlite')?'后台 SQLite（重启保留）':'.env / 默认值';
    parts.push('配置来源：'+srcLabel);
  }
  text('social-schedule-status',parts.join(' · '));
  for(const id of ['social-schedule-save','social-schedule-reset'])$(id).disabled=false;
  clearDirty('social-schedule-form');
}
function renderLife(d){
  const c=d.config||{},ws=d.world_state||{},nodes=d.nodes||{},dm=d.desktop_memory||{},eng=d.engine||{},tz=d.timezone||'Asia/Shanghai';
  renderWorldObservation(d.observation,eng,tz);
  const exp=ws.experience||{},plan=exp.plan,actual=exp.actual||{};
  const labels={bilibili_surf:'B站',minecraft:'Minecraft',qq_social:'QQ',social_presence:'社交主动',idle:'清醒空闲',rest:'休息（旧记录）',sleep:'定时睡眠',day_rest:'白天小憩',resting:'小憩',bili_patrol:'B站巡逻中',bili_read:'正在学习视频',mc_work:'MC 任务',mc_play:'MC 游玩',online:'在线游玩',video_learned:'完成视频学习',mc_goal_completed:'完成 MC 任务',mc_rare_find:'MC 发现稀有资源',qq_delivered:'QQ 消息已送达',chatting:'正在聊天',chat_delivered:'聊天回复已送达',baseline:'沿用升级前数值',manual:'后台手动调整',sleeping:'休息',calm:'平静',curious:'好奇',pleased:'得意',focused:'专注',sleepy:'困倦',playful:'活泼',cozy:'惬意',mine:'采矿',gather:'采集',explore:'探索',goto:'赶路',follow:'跟随'};
  const circadianLabels={just_woke_up:'刚醒迷糊',bedtime_drowsy:'睡前犯困',late_night_catch:'深夜值守'};
  const name=k=>labels[k]||k||'—';
  const battery=exp.social_battery;
  const restingPlan=plan&&['model_chose_rest','model_chose_idle','sleeping','local_rest','local_idle','unchanged_state','no_available_actions'].includes(plan.outcome);
  let planned='等待首次规划';
  if(plan){
    if(['day_rest','agenda_kept','agenda_revised'].includes(plan.outcome))planned=planLabel(plan.outcome);
    else if(restingPlan)planned='本轮未安排动作';
    else if(plan.accepted>0&&plan.plan_module==='idle')planned='已安排 '+plan.accepted+' 项活动';
    else planned=plan.plan_module?name(plan.plan_module)+' · '+(plan.accepted||0)+' 个动作':'本轮已结束';
  }
  const stats=[
    ['本时段计划',planned],
    ['实际活动',name(actual.state)+(actual.day_rest_until?' · '+(actual.day_rest_source==='low_energy'?'精力偏低':'自主选择')+' · '+lifeFmt(actual.day_rest_until,tz)+' 醒来':'')+(actual.mc_task?' · '+name(actual.mc_task):'')],
    ['最近确认完成',actual.last?name(actual.last.reason)+' · '+lifeFmt(actual.last.at,tz):'等待活动回执'],
    ['心情 / 精力 / 无聊',name(ws.mood)+' · '+(ws.energy??80)+' / '+(ws.boredom??20)],
    ['挂念值 / 节律',(exp.owner_longing??0)+' / 100 · '+(circadianLabels[exp.circadian_tag]||'日常清醒')],
    ['当前上头余韵',exp.interest_lingering||'暂无特定上头话题'],
    ['共享社交电量',battery?battery.charge+' / 100':'—'],
    ['待分享见闻',(exp.news_count??0)+' 条'],
    ['跨日待办账本',((d.ledger_events||[]).filter(x=>x.status==='pending').length)+' 条待办'],
    ['Attention Lock',ws.proactive_suspended?'HIGH 锁定中':'正常开放'],
    ['今日自主活动',eng.proactive_sent_today??0],
    ['已用共用次数',(eng.daily_activity_usage?.counted??eng.proactive_sent_today??0)+' / '+(c.max_proactive_per_day??6)]
  ];
  $('life-stats').replaceChildren(...stats.map(([k,v])=>{const n=el('div',undefined,'stat');n.append(el('small',k),el('strong',v));return n;}));
  const points=n=>Number(n.toFixed(2));
  const delta=(a,b)=>a==null||b==null||points(a)===points(b)?'—':points(a)+' → '+points(b);
  table('life-changes',['时间','变化原因','精力','无聊','社交电量'],(exp.changes||[]).map(r=>[
    lifeFmt(r.at,tz),name(r.reason)+(r.reason==='idle'?' · 清醒累计':r.reason==='rest'?' · 休息累计':'')+(r.reason==='video_learned'&&r.seconds>0?' · '+Math.ceil(r.seconds)+' 秒':'')+(r.audience_label?' · '+r.audience_label:''),
    r.reason==='baseline'?r.energy_after:delta(r.energy_before,r.energy_after),r.reason==='baseline'?r.boredom_after:delta(r.boredom_before,r.boredom_after),delta(r.social_before,r.social_after)
  ]));
  text('life-battery-status',battery?
    (battery.chatting?'聊天中，恢复暂停':battery.charge===100?'电量已满':'电量恢复中')+
    (battery.last_reply_at?' · 最近一次成功回复 '+lifeFmt(battery.last_reply_at,tz):' · 尚无成功回复'):'');
  table('life-news',['来源','待分享见闻','发生时间'],(exp.news||[]).map(r=>[name(r.source),r.label,lifeFmt(r.at,tz)]));
  const nap=nodes.onebot_cn_napcat||{},sh=nodes.shanghai_node_agent||{},mc=nodes.shanghai_mc_agent||{};
  const nodePills=[
    nap.connected?pill('NapCat CN 在线 ('+(nap.mode||'http')+')','ok'):pill('NapCat CN 未连接 ('+(nap.mode||'http')+')','warn'),
    sh.connected?pill('境内躯干节点在线 ('+(sh.node_id||'CN')+')','ok'):sh.configured?pill('境内躯干节点等待接入 ('+(sh.node_id||'CN')+')','warn'):pill('境内躯干节点未启用','muted'),
    mc.connected?pill('MC躯干在线'+(mc.paused?' (已暂停)':'')+(mc.dimension?' · '+mc.dimension:''),mc.paused?'warn':'ok'):mc.bridge_ready?pill('MC躯干就绪(待机)'+(mc.paused?' (已暂停)':''),mc.paused?'warn':'ok'):mc.mc_enabled?pill('MC躯干等待接入','warn'):pill('MC躯干未就绪','muted')
  ];
  if(sh.bili_delegate_active)nodePills.push(pill('B站 CN 专线托管中','ok'));
  nodePills.push(
    dm.enabled?(dm.latest_timestamp?pill('桌面记忆活跃 '+lifeFmt(dm.latest_timestamp,tz),'ok'):pill('桌面记忆已启用（暂无活跃时间）','muted')):pill('桌面记忆未启用','muted'),
    ws.proactive_suspended?pill('Attention Lock 已触发：非核心主动已暂停','warn'):pill('Attention Lock 未锁定','ok')
  );
  $('life-node-pills').replaceChildren(...nodePills);
  text('life-world-status','世界状态更新于：'+lifeFmt(ws.updated_at,tz)+' · 最近主动触发：'+lifeFmt(ws.last_proactive_at,tz)+' · 境内节点说明：'+(sh.phase_note||'CN 反向 WSS 躯干专线'));
  const caps=Array.isArray(sh.capabilities)&&sh.capabilities.length?sh.capabilities.join(', '):'—';
  const nodeHead='境内节点'+(sh.node_id?' '+sh.node_id:'')+'（'+(sh.region||'CN')+'）：'+(sh.connected?'在线':sh.configured?'等待连接':'未配置');
  text('life-node-status',!sh.connected&&!sh.connected_at&&!sh.last_heartbeat_at
    ?nodeHead+' · 尚未建立过连接'
    :[nodeHead,
      sh.source_ip&&'来源 IP '+sh.source_ip,
      sh.connected_at&&'建连 '+lifeFmt(sh.connected_at,tz),
      sh.last_heartbeat_at&&'心跳 '+lifeFmt(sh.last_heartbeat_at,tz),
      'RPC 成功/失败 '+(sh.rpc_calls??0)+'/'+(sh.rpc_errors??0),
      'B站托管 '+(sh.bili_delegate_active?'开启':'未激活'),
      caps!=='—'&&'能力 '+caps
    ].filter(Boolean).join(' · '));
  text('life-desktop-status',dm.enabled?('桌面端 Antigravity_Memory 最近活跃：'+(dm.latest_iso||lifeFmt(dm.latest_timestamp,tz))+(dm.latest_tag?' · 标签 '+dm.latest_tag:'')):'桌面端 Antigravity_Memory 协同当前未启用。');
  $('life-world-module').value=ws.current_module||'idle';
  const moduleIds=new Set(['idle',...(ws.modules||[]).map(m=>m.module_id)]);
  $('life-module-options').replaceChildren(...Array.from(moduleIds,id=>{
    const option=el('option');option.value=id;return option;
  }));
  $('life-world-focus').value=ws.focus_level||'LOW';
  $('life-world-mood').value=ws.mood||'calm';
  $('life-world-energy').value=ws.energy??80;
  $('life-world-boredom').value=ws.boredom??20;
  $('life-enabled').checked=Boolean(c.enabled);
  $('life-max-daily').value=c.max_proactive_per_day??6;
  $('life-planner-min').value=c.planner_min_minutes??120;
  $('life-planner-max').value=c.planner_max_minutes??240;
  $('life-sleep-min').value=c.sleep_min_minutes??360;
  $('life-sleep-max').value=c.sleep_max_minutes??480;
  clearDirty('life-world-form');
  clearDirty('life-config-form');
  const srcLabel=(d.source==='admin'||d.source==='sqlite')?'后台 SQLite（重启保留）':'.env / 默认值';
  const planSrcLabels={llm:'模型规划',sleeping:'夜间休眠',local_fallback:'本地兜底',milestone:'里程碑重规划',none:'尚未规划'};
  const planSrcText=eng.last_plan_source?('（'+(planSrcLabels[eng.last_plan_source]||eng.last_plan_source)+'）'):'';
  text('life-source','配置来源：'+srcLabel+' · '+(eng.running?'引擎运行中':'引擎待机')+' · 时区 '+zoneLabel(tz)+(eng.next_plan_at?' · 下次行程规划 '+lifeFmt(eng.next_plan_at,tz):'')+(eng.last_plan_at?' · 上次规划 '+lifeFmt(eng.last_plan_at,tz)+planSrcText:'')+(eng.last_error?' · 最近提示：'+eng.last_error:''));
  for(const id of ['life-world-save','life-save-config','life-reset-config','life-plan-now','life-gatekeeper-now'])$(id).disabled=false;
  const spMod=(ws.modules||[]).find(m=>m.module_id==='social_presence');
  renderSocialSchedule({...c,...(c.module_settings?.social_presence||{})},spMod?{...spMod,source:d.source}:{source:d.source});
  table('life-digests',['日期','进度','已生成','已排队','平台已确认'],(d.digest_history||[]).map(r=>[
    r.day,deliveryStages[r.stage]||r.stage,lifeFmt(r.generated_at,tz),lifeFmt(r.queued_at,tz),lifeFmt(r.confirmed_at,tz)
  ]));
  table('life-intents-table',['ID','动作 / 接收人','来源 / 事件','执行窗口','门控与模糊动机','状态 / 结果','操作'],(d.intents||[]).map(it=>[
    el('span',String(it.id||'').slice(0,10),'code-mono'),
    (it.action_name||it.action_type)+(it.target_scope?' · '+it.target_scope:''),
    [it.origin,it.source_module,it.source_event_id?.slice(0,10)].filter(Boolean).join(' · '),
    lifeFmt(it.execute_after,tz)+' ~ '+lifeFmt(it.expire_at,tz),
    '门控 '+lifeSummaryJson(it.gate_condition)+' | 动机 '+lifeSummaryJson(it.intent_note),
    [lifeIntentStatus[it.status]||it.status,it.result_reason,it.shadow_result].filter(Boolean).join(' · '),
    it.status==='pending'?button('取消',async()=>{const next=await api('life/intents/'+encodeURIComponent(it.id),'DELETE');renderLife(next);notice('已取消待执行意图 '+String(it.id).slice(0,8));},'danger'):el('span','—','muted')
  ]));
  table('life-rules-table',['处理时间','规则','事件 ID','结果','意图 ID'],(d.rule_receipts||[]).map(row=>[
    lifeFmt(row.created_at,tz),row.rule_id,row.event_id.slice(0,10),row.outcome,row.intent_id?.slice(0,10)||'—'
  ]));
  table('life-events-table',['时间','来源模块','事件类型','重要度','规划器已读','脱敏载荷'],(d.events||[]).map(ev=>[
    lifeFmt(ev.created_at,tz),
    ev.source_module||'—',
    ev.event_type||'—',
    pill(ev.importance||'normal',ev.importance==='high'?'warn':'muted'),
    ev.consumed?pill('已消费','muted'):pill('未消费','ok'),
    lifeSummaryJson(ev.payload)
  ]));
}
async function life(){
  const [d,rest]=await Promise.all([api('life/overview'),api('life/rest-hours')]);
  if(!token)return;
  renderRestHours(rest);renderLife(d);
}
for(const id of ['refresh-life','refresh-rest-hours']){
  $(id).addEventListener('click',e=>busy(e.target,async()=>{
    if(!(await confirmDiscardTabChanges('life','刷新世界状态')))return;
    await life();
  }));
}
$('life-world-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('life-world-save'),async()=>{
    const mod=$('life-world-module').value.trim();
    const mood=$('life-world-mood').value.trim();
    const energy=Number($('life-world-energy').value);
    const boredom=Number($('life-world-boredom').value);
    if(!mod)throw Error('current_module 不能为空');
    if(!mood)throw Error('mood 不能为空');
    if(!Number.isFinite(energy)||energy<0||energy>100||!Number.isInteger(energy*10))throw Error('精力范围为 0～100，最多一位小数');
    if(!Number.isInteger(boredom)||boredom<0||boredom>100)throw Error('无聊度须为 0～100 的整数');
    const next=await api('life/world','PUT',{current_module:mod,focus_level:$('life-world-focus').value,mood,energy,boredom});
    renderLife(next);
    notice('世界状态已更新（Attention Lock：'+((next.world_state&&next.world_state.proactive_suspended)?'已锁定':'已释放')+'）');
  });
});
$('life-config-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('life-save-config'),async()=>{
    const intVal=(id,min,max,label)=>{const v=Number($(id).value);if(!Number.isInteger(v)||v<min||v>max)throw Error(label+'须为 '+min+'～'+max+' 的整数');return v;};
    const pMin=intVal('life-planner-min',30,720,'活跃规划最小间隔');
    const pMax=intVal('life-planner-max',60,720,'活跃规划最大间隔');
    if(pMin>pMax)throw Error('活跃规划最小间隔不能大于最大间隔');
    const sMin=intVal('life-sleep-min',120,960,'夜间休眠最小间隔');
    const sMax=intVal('life-sleep-max',180,960,'夜间休眠最大间隔');
    if(sMin>sMax)throw Error('夜间休眠最小间隔不能大于最大间隔');
    if($('rest-start').value===$('rest-end').value)throw Error('起床与入睡时间须不同');
    const restHours=$('rest-start').value+'-'+$('rest-end').value;
    const payload={
      enabled:$('life-enabled').checked,
      quiet_hours:restHours,
      planner_min_minutes:pMin,
      planner_max_minutes:pMax,
      sleep_min_minutes:sMin,
      sleep_max_minutes:sMax,
      max_proactive_per_day:intVal('life-max-daily',0,30,'自主活动共用每日上限'),
    };
    const rest={enabled:$('rest-enabled').checked,day_rest_enabled:$('rest-day-enabled').checked,allow_owner_private:$('rest-owner-private').checked,rest_hours:restHours};
    const next=await api('life/rhythm','PUT',{schedule:payload,rest});
    renderRestHours(next.rest);renderLife(next.world);notice('作息与调度已保存');
  });
});
$('life-reset-config').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复默认作息与规划间隔？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('life/rhythm','DELETE');
  renderRestHours(next.rest);renderLife(next.world);notice('已恢复默认作息与调度');
}));
$('life-plan-now').addEventListener('click',e=>busy(e.target,async()=>{
  notice('正在规划今日行程…');
  const r=await api('life/plan','POST');
  await life();
  notice('行程规划结果：'+planLabel(r.status||'planned'));
}));
$('life-gatekeeper-now').addEventListener('click',e=>busy(e.target,async()=>{
  notice('正在执行零 Token 门控扫描…');
  const r=await api('life/gatekeeper','POST');
  await life();
  notice('门控扫描完成：'+(r.status||'ok'));
}));
$('refresh-social-schedule').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('life','刷新共享社交排程')))return;
  const detail=await api('life/modules/social_presence');
  renderSocialSchedule(detail.scheduling?.values||{},detail);
}));
$('social-schedule-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('social-schedule-save'),async()=>{
    const intVal=(id,min,max,label)=>{
      const raw=String($(id).value??'').trim();
      if(!raw)throw Error(label+'须为 '+min+'～'+max+' 的整数');
      const v=Number(raw);
      if(!Number.isInteger(v)||v<min||v>max)throw Error(label+'须为 '+min+'～'+max+' 的整数');
      return v;
    };
    const payload={
      owner_delivery_enabled:$('social-owner-delivery').checked,
      owner_dms_unlimited:$('social-owner-unlimited').checked,
      group_cooldown_seconds:intVal('social-group-cooldown',300,86400,'群聊主动冷却（秒）'),
      group_active_window_seconds:intVal('social-group-window',60,7200,'群聊活跃观察窗口（秒）'),
      group_min_recent_messages:intVal('social-group-min-msgs',1,50,'群聊最少有效消息数'),
      desktop_active_suppress_seconds:intVal('social-desktop-suppress',300,86400,'桌面活跃抑制窗口（秒）'),
    };
    const next=await api('life/modules/social_presence/schedule','PUT',payload);
    renderSocialSchedule(next.scheduling?.values||{},next);
    notice('共享社交主动排程已保存');
  });
});
$('social-schedule-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复默认共享社交主动排程？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('life/modules/social_presence/schedule','DELETE');
  renderSocialSchedule(next.scheduling?.values||{},next);
  notice('已恢复默认共享社交主动排程');
}));

const mcControls=['life-mc-build','life-mc-connect','life-mc-disconnect','life-mc-chat-btn','life-mc-status-btn','life-mc-idle','life-mc-pause','life-mc-resume'];
const mcTodLabels={day:'白天',sunset:'黄昏',night:'夜晚',sunrise:'黎明'};
function renderMinecraft(mc){
  const eq=mc.equipment||{};
  const gear=[eq.main_hand,eq.off_hand,eq.head,eq.chest,eq.legs,eq.feet].filter(Boolean).join(', ')||'无装备';
  const modeText=mc.combat_mode==='guard'?'护卫'+(mc.guard_target?' ('+mc.guard_target+')':''):'躲避';
  const todText=mcTodLabels[mc.time_of_day]||'—';
  const fields=[
    ['境内连接',mc.bridge_ready?'桥接就绪':'未就绪'],['游戏连接',mc.connected?'游戏在线':'未进入游戏'],
    ['游戏配置',(mc.mc_enabled?'已启用':'未启用')+' / '+(mc.mc_configured?'已配置':'未配置')],
    ['角色活动',mc.paused?'已暂停':mc.activity||'—'],
    ['昼夜 / 模式',todText+' · '+modeText],
    ['装备 / 空槽',gear+(mc.free_slots!=null?'（空槽 '+mc.free_slots+'）':'')]
  ];
  $('mc-overview').replaceChildren(...fields.map(([name,value])=>{
    const row=el('div');row.append(el('dt',name),el('dd',value));return row;
  }));
  const position=Array.isArray(mc.pos)?mc.pos.join(', '):mc.pos;
  text('life-mc-status','维度 '+(mc.dimension||'—')+' · 坐标 '+(position||'—')+' · 生命 / 饱食 '+(mc.health??'—')+' / '+(mc.food??'—')+' · 更新 '+lifeFmt(mc.updated_at,displayZone));
  const planParts=[];
  if(mc.session_intent)planParts.push('会话意图：'+mc.session_intent);
  if(mc.active_plan)planParts.push('连贯计划：'+mc.active_plan);
  if(mc.brain_last_reason)planParts.push('最近决策：'+mc.brain_last_reason);
  text('mc-plan-summary',planParts.length?planParts.join(' · '):'暂无连贯计划或决策记录。');
  const wps=Array.isArray(mc.waypoints)?mc.waypoints:[];
  $('mc-waypoints-list').replaceChildren(...(wps.length?wps.map(w=>{
    const posStr=Array.isArray(w.pos)?w.pos.join(','):'';
    return pill((w.name||'wp')+(posStr?' ('+posStr+')':''),'muted');
  }):[pill('暂无已记录路标','muted')]));
  const logs=Array.isArray(mc.action_log)?mc.action_log.slice().reverse():[];
  table('mc-action-log',['时间','触发','动作','执行结果','理由 / 公屏'],logs.map(item=>[
    lifeFmt(item.ts,displayZone),
    item.trigger||'—',
    item.action||'—',
    item.outcome||'—',
    [item.reason,item.say?'「'+item.say+'」':''].filter(Boolean).join(' · ')||'—'
  ]),'暂无 MC 动作流水（上线执行动作后会在此展示最近 30 步）。');
  mcControls.forEach(id=>$(id).disabled=false);
}
async function sendMcGoal(actionType,topic='',message=''){
  const body={action_type:actionType,view:'module'};
  if(topic)body.topic=topic;
  if(message)body.message=message;
  const res=await api('life/mc/goal','POST',body);
  if(res.mc_status)renderMinecraft(res.mc_status);
  const summary=res?.result?.summary||res?.result?.reason||res?.status||'ok';
  notice('MC 指令 ['+actionType+']：'+(res?.status==='ok'?'成功':'不可用')+' · '+summary,res?.status!=='ok');
}
$('life-mc-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('life-mc-build'),async()=>{
    const topic=$('life-mc-topic').value.trim();
    if(!topic)throw Error('请输入建造或活动目标');
    await sendMcGoal('build_plan',topic);
    $('life-mc-topic').value='';
  });
});
$('life-mc-chat-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('life-mc-chat-btn'),async()=>{
    const msg=$('life-mc-chat-msg').value.trim();
    if(!msg)throw Error('请输入公屏发言内容');
    if(msg.startsWith('/'))throw Error('禁止在公屏发送以 / 开头的斜杠指令');
    await sendMcGoal('chat','',msg);
    $('life-mc-chat-msg').value='';
  });
});
$('life-mc-connect').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('connect')));
$('life-mc-disconnect').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('disconnect')));
$('life-mc-status-btn').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('status')));
$('life-mc-idle').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('idle')));
$('life-mc-pause').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('pause')));
$('life-mc-resume').addEventListener('click',e=>busy(e.target,()=>sendMcGoal('resume')));

const ledgerStatusLabels={pending:'待处理',consumed:'已核销',expired:'已过期'};
function renderEventLedger(d){
  const tz=d.timezone||'Asia/Shanghai', c=d.counts||{};
  text('event-ledger-state','待处理 '+(c.pending??0)+' 条（已到期 '+(c.due??0)+' 条） · 最近已核销 '+(c.consumed??0)+' 条 · 已过期 '+(c.expired??0)+' 条 · 时区 '+zoneLabel(tz));
  $('ledger-module-options').replaceChildren(...(d.modules||[]).map(id=>{
    const option=el('option');option.value=id;return option;
  }));
  table('life-ledger-table',['来源 → 目标','类型 / 可见域','待办摘要','生效 / 过期时间','状态','操作'],(d.ledger_events||[]).map(item=>{
    const route=(item.source_module||'—')+(item.target_module?' → '+item.target_module:'');
    const visPill=pill(item.visibility==='owner_private'?'私密排程':'公开可见',item.visibility==='owner_private'?'paid':'muted');
    const typeBox=el('div',undefined,'row');
    typeBox.append(el('span',item.event_type||'—'),visPill);
    const statusPill=pill(ledgerStatusLabels[item.status]||item.status,item.status==='pending'?'ok':'muted');
    return[
      route,typeBox,item.summary||'—',
      lifeFmt(item.due_at,tz)+' ~ '+lifeFmt(item.expires_at,tz),
      statusPill,
      item.status==='pending'?button('核销',async()=>{const next=await api('life/ledger/'+encodeURIComponent(item.id),'DELETE');renderEventLedger(next);notice('已核销待办事件');},'secondary'):el('span',item.consumed_by||'—','muted')
    ];
  }));
}
async function eventLedger(){
  const d=await api('life/ledger');
  if(!token)return;
  renderEventLedger(d);
}
$('refresh-event-ledger').addEventListener('click',e=>busy(e.target,eventLedger));
$('life-ledger-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('life-ledger-submit'),async()=>{
    const summary=$('life-ledger-summary').value.trim();
    const target=$('life-ledger-target').value.trim();
    const dueMins=Number($('life-ledger-due').value);
    if(!summary)throw Error('请填写待办摘要');
    if(!Number.isInteger(dueMins)||dueMins<0||dueMins>43200)throw Error('延后生效分钟须为 0～43200 的整数');
    const next=await api('life/ledger','POST',{summary,target_module:target,due_in_minutes:dueMins,visibility:$('life-ledger-vis').value});
    $('life-ledger-summary').value='';
    $('life-ledger-target').value='';
    $('life-ledger-due').value='0';
    renderEventLedger(next);
    notice('待办事件已加密送入账本');
  });
});

let ttsPreviewUrl='';
function clearTtsPreview(){
  if(ttsPreviewUrl){URL.revokeObjectURL(ttsPreviewUrl);ttsPreviewUrl='';}
  const box=$('vc-gw-tts-preview');
  box.replaceChildren();
  box.hidden=true;
}
function renderTtsPreview(r){
  clearTtsPreview();
  if(!r||!r.audio_b64)return;
  const bin=atob(r.audio_b64);
  const bytes=new Uint8Array(bin.length);
  for(let i=0;i<bin.length;i++)bytes[i]=bin.charCodeAt(i);
  const blob=new Blob([bytes],{type:r.mime_type||'audio/mpeg'});
  ttsPreviewUrl=URL.createObjectURL(blob);
  const head=el('div',undefined,'tts-preview-head');
  const meta='TTS 试听（'+(r.fmt||'mp3')+' · '+(r.duration_ms??0)+' ms · '+(r.bytes??bytes.length)+' 字节 · 耗时 '+(r.latency_ms??0)+' ms）';
  const closeBtn=el('button','关闭试听','secondary');
  closeBtn.type='button';
  closeBtn.addEventListener('click',clearTtsPreview);
  head.append(el('span',meta),closeBtn);
  const audio=document.createElement('audio');
  audio.controls=true;
  audio.autoplay=true;
  audio.src=ttsPreviewUrl;
  const box=$('vc-gw-tts-preview');
  box.replaceChildren(head,audio);
  box.hidden=false;
}

function syncVoiceGatewayFolds(){
  const stt=$('vc-gw-stt-provider').value;
  const tts=$('vc-gw-tts-provider').value;
  for(const fold of $('vc-gw-form').querySelectorAll('details.provider-fold')){
    const groups=(fold.dataset.providerGroup||'').split(',').map(s=>s.trim()).filter(Boolean);
    const active=groups.includes(stt)||groups.includes(tts);
    fold.open=active;
    fold.classList.toggle('provider-fold-active',active);
  }
}
$('vc-gw-stt-provider').addEventListener('change',syncVoiceGatewayFolds);
$('vc-gw-tts-provider').addEventListener('change',syncVoiceGatewayFolds);

function renderVoiceCompanion(d){
  const tz=d.timezone||'Asia/Shanghai', c=d.config||{}, active=d.active_session;
  const modeLabel=m=>m==='owner_private'?'Owner 私密陪伴':'公开语音房隔离';
  const vcPills=[
    d.online?pill('伴随端在线 · '+(d.adapter_id||'adapter'),'ok'):d.ws_configured?pill('等待伴随端接入','warn'):pill('WS 接入未启用','muted'),
    active?pill(modeLabel(active.mode),'paid'):pill('当前无活跃会话','muted')
  ];
  $('vc-pills').replaceChildren(...vcPills);
  text('vc-state',
    '配置来源：'+(d.source==='sqlite'?'后台 SQLite':'默认配置')+
    (d.client_id||d.source_ip?(d.client_id?' · 客户端 '+d.client_id:'')+(d.source_ip?' · 来源 IP '+d.source_ip:''):' · 暂无客户端连接')+
    (d.connected_at?' · 建连 '+lifeFmt(d.connected_at,tz):'')+
    (active?' · 正在游玩 '+(active.game_title||'未命名游戏')+'（已对话 '+active.spoken_turns+'/'+active.turns+' 轮 · 看图 '+active.frames_analyzed+' 帧）':'')
  );
  $('vc-disconnect').disabled=!d.online;
  $('vc-enabled').checked=Boolean(c.enabled);
  $('vc-use-screen-watch').checked=Boolean(c.use_screen_watch);
  $('vc-save-recap').checked=Boolean(c.save_recap_to_ledger);
  $('vc-style-hint').value=c.tts_style_hint||'';
  for(const id of ['vc-save','vc-reset'])$(id).disabled=false;
  clearDirty('vc-config-form');
  const stLabels={active:'进行中',ended:'正常下播',disconnected:'连接断开',restarted:'切换重开',superseded:'被新连接接管',admin_disconnect:'后台断开',service_stop:'服务停机'};
  table('vc-sessions-table',['时间','适配器 / 模式','游戏','对话 / 看图','状态','摘要'],(d.sessions||[]).map(s=>[
    lifeFmt(s.started_at,tz),
    (s.adapter_id||'—')+' · '+modeLabel(s.mode),
    s.game_title||'—',
    (s.spoken_turns??0)+'/'+(s.turns??0)+' 轮 · '+(s.frames_analyzed??0)+' 帧',
    stLabels[s.status]||s.status,
    s.summary||'—'
  ]));
}
function _normSttProv(v){const s=String(v||'fish').toLowerCase();return s==='fish_audio'?'fish':['openai_compatible','groq'].includes(s)?'openai':s;}
function _normTtsProv(v){const s=String(v||'breezeblue').toLowerCase();return s==='fish_audio'?'fish':['breeze_blue','breeze'].includes(s)?'breezeblue':s==='edge_tts'?'edge':s;}
function renderVoiceGatewayConfig(d,opts={}){
  const g=d?.gateway||null,saved=d?.saved_keys||{};
  const preserveForm=Boolean(opts.preserveForm);
  const pills=[];
  if(!d?.configured){pills.push(pill('未配置 VOICE_GATEWAY_URL','muted'));}
  else if(d.reachable&&g){
    pills.push(pill('网关在线 · STT '+g.stt_provider,'ok'));
    pills.push(pill('TTS '+g.tts_provider+(g.tts_model?' ('+g.tts_model+')':''),'paid'));
    pills.push(pill(d.source==='sqlite'?'SQLite 加密热覆盖':'.env 启动默认',d.source==='sqlite'?'warn':'muted'));
  }else{pills.push(pill('网关暂不可达','err'));}
  $('vc-gw-pills').replaceChildren(...pills);
  text('vc-gw-state',d?.configured?(d.reachable?'热切换就绪：修改保存后立即写入 SQLite 保险库并同步网关，网关重启也会自动重放覆盖。':('无法连接语音网关：'+(d.error||'unavailable'))):'主脑尚未配置 VOICE_GATEWAY_URL / VOICE_GATEWAY_TOKEN。');
  const usable=Boolean(d?.configured);
  ['vc-gw-save','vc-gw-reset'].forEach(id=>{$(id).disabled=!usable;});
  $('vc-gw-test').disabled=!Boolean(d?.configured&&d?.reachable);
  if(g){
    const brz=g.breezeblue||{},fish=g.fish||{},sf=g.siliconflow||{},oai=g.openai_stt||{},edge=g.edge_tts||{};
    if(!preserveForm){
      $('vc-gw-stt-provider').value=_normSttProv(g.stt_provider);
      $('vc-gw-tts-provider').value=_normTtsProv(g.tts_provider);
      $('vc-gw-tts-speed').value=String(g.tts_speed??1.05);
      $('vc-gw-tts-buffer-mode').value=g.tts_buffer_mode||'turn';
      $('vc-gw-tts-streaming').checked=Boolean(g.tts_streaming);
      $('vc-gw-brz-url').value=brz.base_url||'https://api.breeze.blue';
      $('vc-gw-brz-voice').value=brz.voice_id||'';
      $('vc-gw-brz-model').value=brz.model_id||'breeze-tts-2';
      $('vc-gw-brz-lang').value=brz.language_code??'zh';
      $('vc-gw-brz-guidance').value=brz.guidance_scale??'';
      $('vc-gw-brz-instructions').value=brz.instructions||'';
      $('vc-gw-fish-url').value=fish.base_url||'https://api.fish.audio';
      $('vc-gw-fish-ref').value=fish.reference_id||'';
      $('vc-gw-fish-model').value=fish.model||'';
      $('vc-gw-fish-latency').value=fish.latency||'balanced';
      $('vc-gw-fish-asr-lang').value=fish.asr_language??'zh';
      $('vc-gw-sf-url').value=sf.base_url||'https://api.siliconflow.cn/v1';
      $('vc-gw-sf-stt-model').value=sf.stt_model||'FunAudioLLM/SenseVoiceSmall';
      $('vc-gw-sf-tts-model').value=sf.tts_model||'FunAudioLLM/CosyVoice2-0.5B';
      $('vc-gw-sf-tts-voice').value=sf.tts_voice||'FunAudioLLM/CosyVoice2-0.5B:diana';
      $('vc-gw-oai-url').value=oai.base_url||'https://api.groq.com/openai/v1';
      $('vc-gw-oai-model').value=oai.model||'whisper-large-v3-turbo';
      $('vc-gw-edge-voice').value=edge.voice||'zh-CN-XiaoyiNeural';
    }
    const brzOk=Boolean(brz.configured||saved.breezeblue);
    text('vc-gw-brz-key-state',brz.configured?(saved.breezeblue?'已存(SQLite)':'已配置(.env)'):saved.breezeblue?'已存(SQLite)':'未配置');
    text('vc-gw-fish-key-state',fish.configured?(saved.fish?'已存(SQLite)':'已配置(.env)'):saved.fish?'已存(SQLite)':'未配置');
    text('vc-gw-sf-key-state',sf.configured?(saved.siliconflow?'已存(SQLite)':'已配置(.env)'):saved.siliconflow?'已存(SQLite)':'未配置');
    text('vc-gw-oai-key-state',oai.configured?(saved.openai_stt?'已存(SQLite)':'已配置(.env)'):saved.openai_stt?'已存(SQLite)':'未配置');
    $('vc-gw-brz-fetch-voices').disabled=!Boolean(d.reachable&&brzOk);
  }else{
    text('vc-gw-brz-key-state',saved.breezeblue?'已存(SQLite)':'未读取');
    text('vc-gw-fish-key-state',saved.fish?'已存(SQLite)':'未读取');
    text('vc-gw-sf-key-state',saved.siliconflow?'已存(SQLite)':'未读取');
    text('vc-gw-oai-key-state',saved.openai_stt?'已存(SQLite)':'未读取');
    $('vc-gw-brz-fetch-voices').disabled=true;
  }
  if(!preserveForm){
    syncVoiceGatewayFolds();
    clearDirty('vc-gw-form');
  }
}
let vcFetchGen=0,vcGwFetchGen=0;
async function voiceGatewayConfig(){
  const gwGen=++vcGwFetchGen;
  const d=await api('life/voice-companion/gateway');
  if(!token||gwGen!==vcGwFetchGen)return;
  renderVoiceGatewayConfig(d);
}
$('vc-gw-refresh').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('voice-companion','刷新网关配置')))return;
  await voiceGatewayConfig();
}));
$('vc-gw-brz-fetch-voices').addEventListener('click',e=>busy(e.target,async()=>{
  const r=await api('life/voice-companion/gateway/voices');
  const list=r.voices||[],sel=$('vc-gw-brz-voice-select');
  sel.replaceChildren(el('option','请选择音色（'+list.length+' 个）',''));
  sel.firstChild.value='';
  list.forEach(v=>{
    const opt=document.createElement('option');
    opt.value=v.voice_id;
    opt.textContent=(v.voice_type==='personal'?'[我的] ':'[官方] ')+(v.name||v.voice_id)+' ('+v.voice_id+')';
    sel.append(opt);
  });
  sel.hidden=list.length===0;
  if(list.length){sel.value=$('vc-gw-brz-voice').value||'';notice('已加载 '+list.length+' 个 BreezeBlue 音色');}
  else notice('账号下暂无可用音色',true);
}));
$('vc-gw-brz-voice-select').addEventListener('change',e=>{
  if(e.target.value){$('vc-gw-brz-voice').value=e.target.value;markDirty('vc-gw-form');}
});
$('vc-gw-test').addEventListener('click',e=>busy(e.target,async()=>{
  const r=await api('life/voice-companion/gateway/test','POST');
  renderTtsPreview(r);
  notice('TTS 实测成功：'+r.bytes+' 字节 · '+r.duration_ms+' ms（耗时 '+r.latency_ms+' ms）');
}));
$('vc-gw-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('vc-gw-save'),async()=>{
    const rawGs=$('vc-gw-brz-guidance').value.trim();
    const payload={
      stt_provider:$('vc-gw-stt-provider').value,
      tts_provider:$('vc-gw-tts-provider').value,
      tts_speed:Number($('vc-gw-tts-speed').value||'1.05'),
      tts_buffer_mode:$('vc-gw-tts-buffer-mode').value,
      tts_streaming:$('vc-gw-tts-streaming').checked,
      breezeblue_api_key:$('vc-gw-brz-key').value.trim(),
      clear_breezeblue_api_key:$('vc-gw-brz-clear').checked,
      breezeblue_base_url:$('vc-gw-brz-url').value.trim()||'https://api.breeze.blue',
      breezeblue_voice_id:$('vc-gw-brz-voice').value.trim(),
      breezeblue_model_id:$('vc-gw-brz-model').value.trim()||'breeze-tts-2',
      breezeblue_language_code:$('vc-gw-brz-lang').value.trim(),
      breezeblue_guidance_scale:rawGs?Number(rawGs):null,
      breezeblue_instructions:$('vc-gw-brz-instructions').value.trim(),
      fish_api_key:$('vc-gw-fish-key').value.trim(),
      clear_fish_api_key:$('vc-gw-fish-clear').checked,
      fish_base_url:$('vc-gw-fish-url').value.trim()||'https://api.fish.audio',
      fish_reference_id:$('vc-gw-fish-ref').value.trim(),
      fish_model:$('vc-gw-fish-model').value.trim(),
      fish_latency:$('vc-gw-fish-latency').value,
      fish_asr_language:$('vc-gw-fish-asr-lang').value.trim(),
      siliconflow_api_key:$('vc-gw-sf-key').value.trim(),
      clear_siliconflow_api_key:$('vc-gw-sf-clear').checked,
      siliconflow_base_url:$('vc-gw-sf-url').value.trim()||'https://api.siliconflow.cn/v1',
      siliconflow_stt_model:$('vc-gw-sf-stt-model').value.trim(),
      siliconflow_tts_model:$('vc-gw-sf-tts-model').value.trim(),
      siliconflow_tts_voice:$('vc-gw-sf-tts-voice').value.trim(),
      openai_stt_api_key:$('vc-gw-oai-key').value.trim(),
      clear_openai_stt_api_key:$('vc-gw-oai-clear').checked,
      openai_stt_base_url:$('vc-gw-oai-url').value.trim()||'https://api.groq.com/openai/v1',
      openai_stt_model:$('vc-gw-oai-model').value.trim(),
      edge_tts_voice:$('vc-gw-edge-voice').value.trim()||'zh-CN-XiaoyiNeural',
    };
    vcGwFetchGen++;
    const next=await api('life/voice-companion/gateway','PUT',payload);
    if(!token)return;
    ['vc-gw-brz-key','vc-gw-fish-key','vc-gw-sf-key','vc-gw-oai-key'].forEach(id=>{$(id).value='';});
    ['vc-gw-brz-clear','vc-gw-fish-clear','vc-gw-sf-clear','vc-gw-oai-clear'].forEach(id=>{$(id).checked=false;});
    renderVoiceGatewayConfig(next);
    notice('语音网关 API 配置已保存并实时热切换');
  });
});
$('vc-gw-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('清除后台语音网关 API 覆盖，恢复网关 .env 默认配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  vcGwFetchGen++;
  const next=await api('life/voice-companion/gateway','DELETE');
  if(!token)return;
  ['vc-gw-brz-key','vc-gw-fish-key','vc-gw-sf-key','vc-gw-oai-key'].forEach(id=>{$(id).value='';});
  ['vc-gw-brz-clear','vc-gw-fish-clear','vc-gw-sf-clear','vc-gw-oai-clear'].forEach(id=>{$(id).checked=false;});
  renderVoiceGatewayConfig(next);
  notice('已恢复网关 .env 默认配置');
}));
async function voiceCompanion(){
  const vcGen=++vcFetchGen,gwGen=++vcGwFetchGen;
  if($('vc-gw-pills')&&!$('vc-gw-pills').children.length){
    $('vc-gw-pills').replaceChildren(pill('正在读取语音网关配置…','muted'));
    text('vc-gw-state','正在异步读取语音网关状态…');
  }
  api('life/voice-companion/gateway').then(gw=>{
    if(!token||gwGen!==vcGwFetchGen)return;
    renderVoiceGatewayConfig(gw,{preserveForm:Boolean(dirtyRegistry.get($('vc-gw-form'))?.dirty)});
  }).catch(err=>{
    if(!token||gwGen!==vcGwFetchGen||err?.name==='AbortError')return;
    renderVoiceGatewayConfig(
      {configured:false,reachable:false,error:err?.message||'unavailable'},
      {preserveForm:Boolean(dirtyRegistry.get($('vc-gw-form'))?.dirty)}
    );
  });
  const vc=await api('life/voice-companion');
  if(!token||vcGen!==vcFetchGen)return;
  renderVoiceCompanion(vc);
}
$('refresh-vc').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('voice-companion','刷新语音伴随状态')))return;
  await voiceCompanion();
}));
$('vc-disconnect').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('确认主动断开当前的语音伴随连接？',{danger:true,confirmLabel:'断开连接'})))return;
  const next=await api('life/voice-companion/disconnect','POST');
  renderVoiceCompanion(next);notice('已断开当前语音伴随连接');
}));
$('vc-config-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('vc-save'),async()=>{
    const payload={
      enabled:$('vc-enabled').checked,
      use_screen_watch:$('vc-use-screen-watch').checked,
      save_recap_to_ledger:$('vc-save-recap').checked,
      tts_style_hint:$('vc-style-hint').value.trim()
    };
    const next=await api('life/voice-companion','PUT',payload);
    renderVoiceCompanion(next);notice('语音伴随配置已保存');
  });
});
$('vc-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复语音伴随默认配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('life/voice-companion','DELETE');
  renderVoiceCompanion(next);notice('已恢复语音伴随默认配置');
}));

function renderScreenWatch(d){
  const tz=d.timezone||'Asia/Shanghai', c=d.config||{};
  const swPills=[
    d.online?(d.paused?pill('看屏端在线 · 已暂停采集','warn'):pill('看屏端在线 · '+(d.adapter_id||'adapter'),'ok')):d.ws_configured?pill('等待看屏端接入','warn'):pill('WS 接入未启用','muted'),
    c.enabled?pill('内存缓存 '+d.buffered_frames+' 帧','ok'):pill('隐私开关已关闭','err')
  ];
  $('sw-pills').replaceChildren(...swPills);
  text('sw-state',
    '配置来源：'+(d.source==='sqlite'?'后台 SQLite':'默认配置')+
    (d.client_id||d.source_ip?(d.client_id?' · 客户端 '+d.client_id:'')+(d.source_ip?' · 来源 IP '+d.source_ip:''):' · 暂无客户端连接')+
    (d.connected_at?' · 建连 '+lifeFmt(d.connected_at,tz):'')+
    (d.game_title?' · 当前窗口 '+(d.game_title+(d.window_title?' / '+d.window_title:'')):'')+
    ' · 累计收帧 '+d.frames_received+' · 语音口播 '+d.routed_voice_count+' · 独立吐槽 '+d.standalone_commentary_count
  );
  $('sw-clear').disabled=false;
  $('sw-disconnect').disabled=!d.online;
  $('sw-enabled').checked=Boolean(c.enabled);
  $('sw-proactive').checked=Boolean(c.proactive_commentary_enabled);
  $('sw-qq-fallback').checked=Boolean(c.deliver_to_qq_when_no_voice);
  $('sw-pulse-interval').value=c.min_pulse_interval_seconds??25;
  $('sw-max-frames').value=c.max_frames_per_query??2;
  $('sw-frame-ttl').value=c.frame_ttl_seconds??15;
  for(const id of ['sw-save','sw-reset'])$(id).disabled=false;
  clearDirty('sw-config-form');
  const chLabels={voice_companion:'语音伴随口播',qq_dm:'Owner 私聊 + 气泡',overlay:'桌面悬浮气泡'};
  table('sw-commentary-table',['时间','投递渠道','画面主题','内容摘要'],(d.recent_commentaries||[]).map(item=>[
    lifeFmt(item.at,tz),
    chLabels[item.channel]||item.channel,
    item.game_title||'—',
    item.text||'—'
  ]));
}
async function screenWatch(){
  const d=await api('life/screen-watch');
  if(!token)return;
  renderScreenWatch(d);
}
$('refresh-sw').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('screen-watch','刷新屏幕伴看状态')))return;
  await screenWatch();
}));
$('sw-clear').addEventListener('click',e=>busy(e.target,async()=>{
  const next=await api('life/screen-watch/clear','POST');
  renderScreenWatch(next);notice('已立即销毁内存画面缓存');
}));
$('sw-disconnect').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('确认断开当前屏幕伴看连接并立即销毁内存画面缓存？',{danger:true,confirmLabel:'断开连接'})))return;
  const next=await api('life/screen-watch/disconnect','POST');
  renderScreenWatch(next);notice('已断开当前看屏连接并销毁缓存');
}));
$('sw-config-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('sw-save'),async()=>{
    const pulse=Number($('sw-pulse-interval').value);
    const frames=Number($('sw-max-frames').value);
    const ttl=Number($('sw-frame-ttl').value);
    if(!Number.isInteger(pulse)||pulse<10||pulse>300)throw Error('主动吐槽最小间隔须为 10～300 秒的整数');
    if(!Number.isInteger(frames)||frames<1||frames>2)throw Error('单次最多提供截图须为 1 或 2');
    if(!Number.isInteger(ttl)||ttl<5||ttl>60)throw Error('内存帧保质期 TTL 须为 5～60 秒的整数');
    const payload={
      enabled:$('sw-enabled').checked,
      proactive_commentary_enabled:$('sw-proactive').checked,
      min_pulse_interval_seconds:pulse,
      max_frames_per_query:frames,
      frame_ttl_seconds:ttl,
      deliver_to_qq_when_no_voice:$('sw-qq-fallback').checked
    };
    const next=await api('life/screen-watch','PUT',payload);
    renderScreenWatch(next);notice('屏幕伴看配置已保存');
  });
});
$('sw-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复屏幕伴看默认配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('life/screen-watch','DELETE');
  renderScreenWatch(next);notice('已恢复屏幕伴看默认配置');
}));

trackDirty('life-world-form','世界状态');
trackDirty('life-config-form','世界作息与调度');
trackDirty('social-schedule-form','共享社交主动排程');
trackDirty('vc-config-form','语音伴随设置');
trackDirty('vc-gw-form','语音网关 API 配置');
trackDirty('sw-config-form','屏幕伴看策略');

onLogout(()=>{
  activeProactiveBase='proactive';
  vcFetchGen++;
  vcGwFetchGen++;
  clearTtsPreview();
  $('world-observation-card').hidden=true;
  for(const id of ['proactive-stats','proactive-queue','proactive-alerts','world-day-stats','world-agenda','world-daily-actions','world-plans','world-days','world-evenings','life-digests','life-stats','life-changes','life-news','life-node-pills','life-module-options','life-intents-table','life-events-table','life-rules-table','mc-overview','mc-waypoints-list','mc-action-log','life-ledger-table','ledger-module-options','vc-pills','vc-sessions-table','vc-gw-pills','vc-gw-brz-voice-select','sw-pills','sw-commentary-table'])$(id).replaceChildren();
  $('vc-gw-brz-voice-select').hidden=true;
  for(const id of ['proactive-state','proactive-budget','world-morning','world-next','world-quiet','world-day-coverage','world-evening-coverage','rest-hours-status','wake-window','life-battery-status','life-world-status','life-node-status','life-desktop-status','life-source','social-schedule-status','life-mc-status','event-ledger-state','vc-state','vc-gw-state','sw-state'])text(id,'');
  text('mc-plan-summary','暂无连贯计划或路标记录。');
  ['vc-gw-brz-key-state','vc-gw-fish-key-state','vc-gw-sf-key-state','vc-gw-oai-key-state'].forEach(id=>text(id,'未读取'));
  for(const id of ['life-world-save','life-save-config','life-reset-config','life-plan-now','life-gatekeeper-now','social-schedule-save','social-schedule-reset','life-mc-build','life-mc-connect','life-mc-disconnect','life-mc-chat-btn','life-mc-status-btn','life-mc-idle','life-mc-pause','life-mc-resume','vc-disconnect','vc-save','vc-reset','vc-gw-save','vc-gw-reset','vc-gw-test','vc-gw-brz-fetch-voices','sw-clear','sw-disconnect','sw-save','sw-reset'])$(id).disabled=true;
});
