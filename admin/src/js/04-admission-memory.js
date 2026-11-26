async function groups(){
  const [items,filterData]=await Promise.all([api('groups'),api('filter')]);
  if(!token)return;
  table('groups',['平台 / 群','触发策略','自由插话活跃度'],items.map(g=>{
    const select=el('select',undefined,'table-select');
    for(const [value,label] of Object.entries({at_only:'点名 / 回复 / 视频',smart_reply:'智能插话',muted:'静默'})){const o=el('option',label);o.value=value;select.append(o);}
    select.value=g.mode;
    select.addEventListener('change',()=>busy(select,async()=>{await api('groups','PUT',{platform:g.platform,group_id:g.group_id,mode:select.value});notice('群策略已保存');}));
    const activity=el('select',undefined,'table-select');
    for(const [value,label] of Object.entries({'':'跟随全局 '+filterData.config.reply_threshold,quiet:'安静 '+filterData.presets.quiet,natural:'自然 '+filterData.presets.natural,active:'活跃 '+filterData.presets.active})){const o=el('option',label);o.value=value;activity.append(o);}
    activity.value=filterData.config.group_activity[g.platform+':'+g.group_id]||'';
    activity.addEventListener('change',()=>busy(activity,async()=>{await api('filter/group-activity','POST',{platform:g.platform,group_id:g.group_id,preset:activity.value||null});notice('群活跃度已保存并立即生效');}));
    return[g.platform+':'+g.group_id,select,activity];
  }));
  $('sim-group').replaceChildren(el('option','默认群策略'));
  $('sim-group').firstChild.value='';
  items.filter(g=>g.platform==='onebot').forEach(g=>{const o=el('option',g.group_id+' · '+g.mode);o.value=g.group_id;$('sim-group').append(o);});
}

function parseBotThreshold(raw,optional=false){const s=String(raw??'').trim();if(!s&&optional)return null;const n=Number(s);if(!Number.isFinite(n)||n<.01||n>1)throw Error('Bot 回复阈值须为 0.01～1.00');return n;}
function botThresholdLabel(custom,effective){const val=Number(effective??0.85);return (val>=1?'1.00（完全屏蔽）':val.toFixed(2))+(custom!=null?' · 专属覆盖':' · 跟随全局');}
function botThresholdControl(uid,custom,effective,after){
  const wrap=el('div',undefined,'row'),inp=el('input');
  inp.type='number';inp.min='0.01';inp.max='1';inp.step='0.01';inp.value=String(custom??effective??0.85);
  inp.className='bot-th-input';inp.setAttribute('aria-label','Bot '+uid+' 阈值');
  wrap.append(inp,button('保存阈值',async()=>{const v=parseBotThreshold(inp.value);await api('bots','PUT',{user_id:uid,action:'threshold',threshold:v});await after();notice('Bot '+uid+' 回复阈值已更新为 '+v.toFixed(2));}));
  if(custom!=null)wrap.append(button('跟随全局',async()=>{await api('bots','PUT',{user_id:uid,action:'threshold',threshold:null});await after();notice('Bot '+uid+' 已恢复跟随全局阈值');}));
  else if(Number(effective)<1)wrap.append(button('设为 1.00',async()=>{await api('bots','PUT',{user_id:uid,action:'threshold',threshold:1});await after();notice('Bot '+uid+' 已设为 1.00 完全屏蔽');}));
  return wrap;
}
async function syncActiveAdmissionOrBots(){
  if(tab==='admission')await admission();
  else if(tab==='group-policy')await bots();
  else await admission();
}
async function bots(){
  const [items,th]=await Promise.all([api('bots'),api('bots/thresholds')]);
  if(!token)return;
  $('bot-default-threshold').value=th.bot_reply_threshold??0.85;
  clearDirty('bot-default-threshold-form');
  table('bots',['机器人 QQ','生效阈值','专属阈值调整','群冷却','操作'],items.map(b=>[
    b.user_id,
    botThresholdLabel(b.custom_threshold,b.effective_threshold),
    botThresholdControl(b.user_id,b.custom_threshold,b.effective_threshold,syncActiveAdmissionOrBots),
    b.groups.map(g=>g.group_id+': '+(g.used??0)+' / '+(g.max_turns??2)+'轮'+(g.is_target?' · 当前目标':' · 本群共用额度')+(g.cooldown_seconds?' · 冷却 '+g.cooldown_seconds+'s':' · 窗口剩余 '+g.window_seconds+'s')).join('；')||'无冷却',
    button('移出管理',async()=>{
      if(!(await confirmDialog('移出后将按普通成员策略处理，确定移出外部 Bot '+b.user_id+'？',{danger:true,confirmLabel:'移出管理'})))return;
      await api('bots','PUT',{user_id:b.user_id,action:'remove'});
      await syncActiveAdmissionOrBots();
      notice('已移出外部 Bot '+b.user_id);
    },'danger')
  ]));
}
$('bot-default-threshold-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{const v=parseBotThreshold($('bot-default-threshold').value);await api('bots/thresholds','PUT',{bot_reply_threshold:v});await syncActiveAdmissionOrBots();notice('默认 Bot 回复阈值已更新为 '+v.toFixed(2));});});
$('add-bot').addEventListener('click',e=>busy(e.target,async()=>{const th=parseBotThreshold($('bot-custom-threshold').value,true);await api('bots','PUT',{user_id:$('bot-id').value.trim(),action:'add',threshold:th});$('bot-id').value='';$('bot-custom-threshold').value='';await syncActiveAdmissionOrBots();notice('外部 Bot 已加入管理');}));

let admissionState=null;
const admissionLists=[['allowed_groups','admission-groups','QQ群号'],['allowed_users','admission-users','好友 QQ']];
async function admission(){
  const d=await api('admission');if(!token)return;
  admissionState=d;const c=d.config;
  $('admission-private-mode').value=c.private_mode;
  $('admission-bot-threshold').value=c.bot_reply_threshold??0.85;
  clearDirty('admission-bot-threshold-form');
  text('admission-source',d.source==='admin'?'当前名单来自 SQLite 后台配置':'当前名单来自本次启动时的 .env');
  text('admission-owner','Owner（固定保留）：'+(d.owner_qq_id||'尚未配置')+' · 机器人自身：'+d.onebot_self_id);
  text('admission-group-scope','群白名单仅决定群准入：该群所有非黑名单成员均可按群互动策略交流，不受私聊用户白名单限制。私聊仍单独按下方「私聊准入」的模式与好友名单判断。');
  text('admission-environment',JSON.stringify(d.environment,null,2));
  for(const [key,id,label] of admissionLists){
    table(id,[label,'操作'],c[key].map(value=>[value,button('移除',async()=>{
      if(!(await confirmDialog('确认从白名单移除 '+value+'？',{danger:true,confirmLabel:'移除'})))return;
      await api('admission/change','POST',{list:key,action:'remove',id:value});
      await admission();
      notice('名单已更新，立即生效');
    },'danger')]));
  }
  table('admission-bots',['外部 Bot QQ','当前生效阈值','专属阈值调整','操作'],c.ignored_bot_ids.map(uid=>{
    const custom=c.bot_reply_thresholds?.[uid],effective=custom??c.bot_reply_threshold??0.85;
    return[uid,botThresholdLabel(custom,effective),botThresholdControl(uid,custom,effective,syncActiveAdmissionOrBots),button('移除',async()=>{
      if(!(await confirmDialog('确认移出外部 Bot '+uid+'？移出后将按普通成员策略处理。',{danger:true,confirmLabel:'移除'})))return;
      await api('admission/change','POST',{list:'ignored_bot_ids',action:'remove',id:uid});
      await admission();
      notice('已移除外部 Bot '+uid);
    },'danger')];
  }));
}
$('refresh-admission').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('admission','刷新名单')))return;
  await admission();
}));
$('admission-bot-threshold-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{const v=parseBotThreshold($('admission-bot-threshold').value);await api('bots/thresholds','PUT',{bot_reply_threshold:v});await admission();notice('默认 Bot 回复阈值已保存为 '+v.toFixed(2));});});
$('admission-bot-hardblock').addEventListener('click',e=>busy(e.target,async()=>{await api('bots/thresholds','PUT',{bot_reply_threshold:1});await admission();notice('默认 Bot 回复阈值已设为 1.00（0 Token 完全屏蔽）');}));
for(const [form,input,key] of [['admission-groups-form','admission-group-id','allowed_groups'],['admission-users-form','admission-user-id','allowed_users'],['admission-bots-form','admission-bot-id','ignored_bot_ids']]){
  $(form).addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{
    const id=$(input).value.trim();
    if(!/^[1-9][0-9]{0,19}$/.test(id))throw Error('请填写不含前导零的纯数字 QQ / 群号');
    if(admissionState?.config[key].includes(id))throw Error('该号码已在名单中，请勿重复添加');
    if(key!=='allowed_groups'&&[admissionState?.owner_qq_id,admissionState?.onebot_self_id].includes(id))throw Error('Owner 和机器人自身是受保护身份');
    if(key==='ignored_bot_ids'){const th=parseBotThreshold($('admission-bot-custom-threshold').value,true);await api('bots','PUT',{user_id:id,action:'add',threshold:th});$('admission-bot-custom-threshold').value='';}
    else{await api('admission/change','POST',{list:key,action:'add',id});}
    $(input).value='';
    await admission();
    notice('名单已保存，立即生效');
  });});
}
$('admission-private-mode').addEventListener('change',e=>busy(e.target,async()=>{const mode=e.target.value;const current=(await api('admission')).config;await api('admission','PUT',{...current,private_mode:mode});await admission();notice('私聊准入模式已更新');}));
$('reset-admission').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复本次启动时 .env 中的群、好友、外部 Bot 名单和回复阈值？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('admission','DELETE');await admission();notice('已恢复 .env 初始名单，立即生效');
}));

const compressionFields={group:['messages','chars','keep','min_chars','max_messages'],private:['messages','chars','keep']};
async function compression(){
  const d=await api('compression');if(!token)return;
  const c=d.config;
  $('compression-enabled').checked=c.enabled;
  $('compression-high-tokens').value=c.history_high_tokens??d.history_high_tokens_estimate;
  for(const [kind,fields] of Object.entries(compressionFields))for(const name of fields)$(kind+'-'+name).value=c[kind][name];
  text('compression-source',d.source==='admin'?'当前使用 SQLite 热配置（优先于 .env）':'当前使用启动时的 .env / 默认配置');
  text('compression-summary',d.history_strategy==='prefix_half'?'当前：追加历史，估算超过 '+(c.history_high_tokens??d.history_high_tokens_estimate)+' Token 或 '+d.history_max_bytes+' 字节时整批折半。半价双轨归档使用 medium；手动提炼只做快照，不裁切活跃历史。下列条数设置仅用于兼容模式。':'群聊：'+c.group.messages+' 条或 ≥'+c.group.chars+' 字；不足 '+c.group.min_chars+' 字且少于 '+c.group.max_messages+' 条时暂缓条数触发，保留 '+c.group.keep+' 条。私聊：'+c.private.messages+' 条或 ≥'+c.private.chars+' 字，保留 '+c.private.keep+' 条。');
  clearDirty('compression-form');
}
function formatSessionMemorySummary(s){if(!s.enabled)return s.recent_count+' 条 · 记忆已关闭';if(s.dual_ready||(s.dual_entries_count??0)>0)return '热窗口 '+s.recent_count+' 条 · 双轨 '+(s.dual_entries_count??0)+' 条（'+(s.dual_chars??0)+' 字）';return '热窗口 '+s.recent_count+' 条 · 旧摘要 '+s.summary_chars+' 字';}
function flexRetryText(retry){
  if(!retry)return '';
  const when=dateTime(retry.next_retry_at);
  const schedule=retry.next_retry_at*1000>Date.now()?'下次重试 '+when:'已到重试时间 '+when+'，等待调度';
  return 'Flex 容量繁忙，等待重试 · '+retry.slices+' 段 · 第 '+retry.retry_number+' 次 · '+schedule;
}
async function sessions(){
  table('sessions',['会话','热窗口 / 长期记忆','归档状态','操作'],(await api('sessions')).map(s=>[
    s.platform+' '+s.kind+':'+s.conversation_id,formatSessionMemorySummary(s),
    [flexRetryText(s.flex_retry),s.archive_unconfirmed_slices?'归档结果未确认，已暂停自动重试 · '+s.archive_unconfirmed_slices+' 段':'',
      s.archive_failed_slices?'最终失败 '+s.archive_failed_slices+' 段':''].filter(Boolean).join(' / ')||'—',
    button('查看与管理',()=>openSession(s))
  ]));
}
async function openSession(s){
  session=s;const d=await api('sessions/'+s.id);session.version=d.version;
  text('session-title',s.kind+':'+s.conversation_id);
  text('session-info','热窗口 '+d.recent_count+' 条 · 记忆'+(d.enabled?'开启':'关闭')+(d.dual_ready?' · 双轨已接管（有效 '+(d.dual_entries_count??0)+' 条 / '+(d.dual_chars??0)+' 字）':''));
  $('summary-text').value=d.rolling_summary;
  $('summary-text').placeholder=d.dual_ready?'当前会话已升级为下方「双轨记忆治理」，旧版单体滚动摘要已停用（此处为空属正常状态）':'暂无旧版滚动摘要';
  $('save-summary').disabled=!!d.dual_ready;$('summary-text').readOnly=!!d.dual_ready;
  await dualMemory();
  if(!$('session-dialog').open)$('session-dialog').showModal();
}
let memoryView=null;
async function dualMemory(){
  const d=await api('sessions/'+session.id+'/memory/search','POST',{query:$('memory-query').value});
  memoryView=d;session.version=d.version;
  const jobNames={pending:'待提炼',attempted:'提炼中',unconfirmed:'归档结果未确认，已暂停自动重试',done:'已归档',failed:'最终失败'};
  const jobEntries=Object.entries(d.jobs||{});
  const jobText=jobEntries.length?jobEntries.map(([k,v])=>(jobNames[k]||k)+':'+v).join(' / '):'无积压切片';
  text('memory-state',(d.ready?'双轨已就绪':'兼容旧摘要，等待首次提炼')+' · 版本 '+d.version+' · 归档 '+jobText+(d.flex_retry?' · '+flexRetryText(d.flex_retry):''));
  table('memory-entries',['条目 / 类型','正文','状态','操作'],d.entries.map(v=>{
    const actions=el('div',undefined,'row');
    if(!v.deleted)actions.append(button('编辑',async()=>{
      const value=await promptDialog('编辑记忆正文',{
        message:'条目：'+v.id+' / '+v.kind,
        hint:'保存后将经过 source 安全筛查并自动设为人工保护锁定。',
        defaultValue:v.text,
        multiline:true,
        placeholder:'输入记忆正文…',
        confirmLabel:'保存修改'
      });
      if(value===null)return;
      let expires=v.expires;
      if(v.slot==='state'){
        const rawDays=await promptDialog('短期状态有效天数',{
          message:'请设置此短期状态条目的有效天数（1–14 的整数）。',
          defaultValue:'3',
          placeholder:'1–14',
          confirmLabel:'确定'
        });
        if(rawDays===null)return;
        const days=Number(rawDays.trim());
        if(!Number.isInteger(days)||days<1||days>14)throw Error('有效天数需为 1–14');
        expires=Date.now()/1000+days*86400;
      }
      await memoryChange(v,'edit',value,expires);
    }));
    if(!v.deleted)actions.append(button('擦除',async()=>{
      if(await confirmDialog('擦除此条记忆并保留保护墓碑？',{danger:true,confirmLabel:'擦除'})){
        await memoryChange(v,'delete');
      }
    },'danger'));
    if(v.locked)actions.append(button('解锁',async()=>{
      if(await confirmDialog('解锁后允许后续新批次提炼更新这条记忆，确定解锁？',{confirmLabel:'解锁'})){
        await memoryChange(v,'unlock');
      }
    }));
    return[v.id+' / '+v.kind,v.deleted?'已擦除':v.text,(v.deleted?'墓碑 · ':'')+(v.locked?'人工保护':'自动更新')+(v.expires?' · 到期 '+dateTime(v.expires):''),actions];
  }));
  $('memory-mirror').hidden=session.kind!=='group';
  $('memory-mirror-enabled').checked=!!d.mirror_enabled;
  text('memory-mirror-state','最近同步 '+dateTime(d.mirror.last_success)+(d.mirror.last_error?' · '+d.mirror.last_error:''));
}
async function memoryChange(v,action,value='',expires=0){await api('sessions/'+session.id+'/memory','PUT',{id:v.id,version:memoryView.version,action,text:value,expires});await dualMemory();notice('记忆修改已生效');}
$('memory-search').addEventListener('click',e=>busy(e.target,dualMemory));
$('memory-mirror-save').addEventListener('click',e=>busy(e.target,async()=>{await api('sessions/'+session.id+'/memory/mirror','PUT',{enabled:$('memory-mirror-enabled').checked,version:memoryView.version});await dualMemory();notice('镜像开关已保存');}));
for(const direction of ['push','pull'])$('memory-'+direction).addEventListener('click',e=>busy(e.target,async()=>{const d=await api('sessions/'+session.id+'/memory/'+direction,'POST');await dualMemory();notice('群档案同步：'+d.status);}));
$('refresh-sessions').addEventListener('click',e=>busy(e.target,sessions));
$('compression-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{const ht=Number($('compression-high-tokens').value);if(!Number.isInteger(ht)||ht<1000||ht>40000)throw Error('热历史折半阈值须为 1000～40000 的整数');const c={enabled:$('compression-enabled').checked,history_high_tokens:ht};for(const [kind,fields] of Object.entries(compressionFields)){c[kind]={};for(const name of fields)c[kind][name]=Number($(kind+'-'+name).value);}await api('compression','PUT',c);await compression();notice('压缩参数已保存并立即生效');});});
$('reset-compression').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复本次启动读取的 .env 压缩配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('compression','DELETE');await compression();notice('已恢复启动时的环境配置');
}));
$('save-summary').addEventListener('click',e=>busy(e.target,async()=>{await api('sessions/'+session.id+'/summary','PUT',{text:$('summary-text').value,version:session.version});notice('摘要已保存，旧压缩结果已失效');await openSession(session);await sessions();}));
$('compress-session').addEventListener('click',e=>busy(e.target,async()=>{const v=await api('sessions/'+session.id+'/compress','POST');notice(v.reason==='flex_capacity_busy'?'Flex 容量繁忙，下次重试 '+dateTime(v.next_retry_at):v.reason==='flex_capacity_exhausted'?'Flex 归档最终失败，已用完重试':'压缩结果：'+v.status);await openSession(session);await sessions();}));
$('reset-session').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('删除此会话的滚动摘要和全部热消息？此操作不可撤销。',{danger:true,confirmLabel:'彻底重置'})))return;
  await api('sessions/'+session.id+'/reset','POST');notice('会话已彻底重置');await openSession(session);await sessions();
}));
$('close-dialog').addEventListener('click',()=>$('session-dialog').close());

async function patrolLimits(){
  const d=await api('patrol-limits');if(!token)return;
  $('patrol-max-subs').value=d.config.max_subscriptions;
  $('patrol-interval').value=d.config.patrol_interval_seconds;
  text('patrol-limits-source',d.source==='admin'?'当前配额来自后台 SQLite（重启保留）':'当前配额来自本次启动读取的 .env');
  for(const id of ['save-patrol-limits','reset-patrol-limits'])$(id).disabled=false;
  clearDirty('patrol-limits-form');
}
$('refresh-patrol-limits').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('patrol','刷新配额')))return;
  await patrolLimits();
}));
$('patrol-limits-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-patrol-limits'),async()=>{const maxSubs=Number($('patrol-max-subs').value),interval=Number($('patrol-interval').value);if(!Number.isInteger(maxSubs)||maxSubs<1||maxSubs>50||!Number.isInteger(interval)||interval<900||interval>86400)throw Error('订阅上限须为 1～50，轮询间隔须为 900～86400 秒');await api('patrol-limits','PUT',{max_subscriptions:maxSubs,patrol_interval_seconds:interval});notice('巡逻配额与间隔已保存并立即生效');await Promise.all([patrolLimits(),subscriptions()]);});});
$('reset-patrol-limits').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复本次启动读取的 .env 订阅上限与轮询间隔？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('patrol-limits','DELETE');notice('已恢复 .env 巡逻配额与间隔');await Promise.all([patrolLimits(),subscriptions()]);
}));
const subKindLabels={up:'UP 主',live:'直播间'};
async function subscriptions(){
  table('subs',['类型','ID','下次巡逻 / 状态','操作'],(await api('subscriptions')).map(s=>[
    subKindLabels[s.kind]||s.kind,s.uid,
    dateTime(s.next_poll)+(s.kind==='live'?' · '+(({0:'未开播',1:'直播中',2:'轮播'})[s.last_status]??'尚未巡逻'):' '),
    button('删除',async()=>{
      if(!(await confirmDialog('删除此 '+(subKindLabels[s.kind]||s.kind)+' 订阅（'+s.uid+'）？',{danger:true,confirmLabel:'删除'})))return;
      await api('subscriptions/'+s.kind+'/'+encodeURIComponent(s.id),'DELETE');await subscriptions();
    },'danger')
  ]));
}
$('add-sub').addEventListener('click',e=>busy(e.target,async()=>{await api('subscriptions','POST',{kind:$('sub-kind').value,uid:$('sub-uid').value.trim()});$('sub-uid').value='';await subscriptions();}));

async function driveSync(){
  const view=await api('drive-sync');
  text('drive-connection',view.status.detail);
  const access={unconfigured:'等待配置 Drive',unauthorized:'等待归档授权',inactive:'模块未就绪'};
  table('drive-consumers',['调用模块','同步内容','当前状态','最近尝试','最近成功'],view.consumers.map(item=>{
    const result=item.in_progress?'同步中':item.last_error==='unconfirmed'?'回执待核实':
      item.last_error?'同步失败 · '+item.last_error:item.last_success?'已同步':'等待首次同步';
    return [item.title,item.usage,access[item.access]||result,dateTime(item.last_attempt),dateTime(item.last_success)];
  }));
}
$('refresh-drive-sync').addEventListener('click',e=>busy(e.target,driveSync));

async function diaries(){
  const [rows,drive]=await Promise.all([api('diaries'),api('diaries/status')]);
  text('journal-drive-status','Google Drive · '+(drive?.detail||'状态未知'));
  table('diaries',['日期','Drive 同步','操作'],rows.map(d=>[
    d.day,d.last_error?d.last_error+' · 重试 '+dateTime(d.next_attempt):d.last_success?'成功 '+dateTime(d.last_success):drive?.state==='unconfigured'?'等待配置':'待同步',
    button('预览',async()=>text('diary-preview',(await api('diaries/'+d.day)).markdown))
  ]));
}
const notebookSourceLabels={drive:'Google Drive 单文件',local:'本地快照文件'};
function showNotebookAvailability(enabled){
  document.querySelectorAll('[data-action="notebook"]').forEach(b=>b.disabled=!enabled||b.hasAttribute('aria-busy'));
  text('notebook-action-note',enabled?'读取 Owner 的只读快照':'只读快照未启用');
}
async function notebook(){
  const n=await api('notebook'),am=n.antigravity_memory||{};
  showNotebookAvailability(n.enabled);
  if(!n.enabled&&am.enabled){
    text('notebook-state','双端记忆桥 Antigravity_Memory 已接管（只读挂载） · 常驻备忘 '+(am.pinned_chars??0)+' 字 · 索引切片 '+(am.indexed_chunks??0)+' 条 · 桌面活跃 '+(am.latest_iso||dateTime(am.latest_timestamp))+'（旧版单文件快照未启用）');
    $('notebook-text').value=am.pinned_text||'（Antigravity_Memory 已连接，当前未解析到常驻备忘段落）';
    $('notebook-text').readOnly=true;$('save-notebook').disabled=true;return;
  }
  text('notebook-state',(n.enabled?'已启用':'未启用')+' · '+(notebookSourceLabels[n.source]||n.source)+' · '+(n.loaded_at?dateTime(n.loaded_at):'尚未加载')+(n.last_error?' · '+n.last_error:''));
  $('notebook-text').value=n.text||'';
  $('notebook-text').placeholder=n.enabled?'':'旧版单文件《小本本》快照未启用';
  $('notebook-text').readOnly=!n.enabled||n.source!=='local';
  $('save-notebook').disabled=!n.enabled||n.source!=='local';
}
$('save-notebook').addEventListener('click',e=>busy(e.target,async()=>{await api('notebook','PUT',{text:$('notebook-text').value});notice('本地快照已通过筛查并保存');await notebook();}));

const archiveLogLabels={
  memory_archive_unconfirmed:'归档中断，结果未确认，已暂停自动重试',
  memory_flex_retry_scheduled:'Flex 容量繁忙，等待重试',
  memory_flex_retry_succeeded:'Flex 重试成功，已归档',
  memory_flex_retry_exhausted:'Flex 归档最终失败，已用完重试'
};
async function logs(){
  table('log-list',['时间','来源','事件','调用详情'],(await api('logs')).map(v=>{
    const flex503=v.code==='gemini_unavailable'&&v.provider_diagnostics?.some(a=>a.model_lane==='memory_flex'&&a.http_status===503);
    return [dateTime(v.at),v.source,archiveLogLabels[v.code]||(flex503?'Flex 容量繁忙（HTTP 503）':v.code),diagnosticCell(v)];
  }));
}
$('refresh-logs').addEventListener('click',e=>busy(e.target,logs));

trackDirty('bot-default-threshold-form','全局默认 Bot 阈值');
trackDirty('admission-bot-threshold-form','默认 Bot 回复阈值');
trackDirty('compression-form','滚动压缩参数');
trackDirty('patrol-limits-form','订阅配额与轮询间隔');

onLogout(()=>{
  admissionState=null;memoryView=null;
  for(const id of ['bot-id','bot-custom-threshold','sub-uid'])$(id).value='';
  for(const id of ['groups','bots','admission-groups','admission-users','admission-bots','sessions','memory-entries','subs','drive-consumers','diaries','log-list'])$(id).replaceChildren();
  for(const id of ['admission-source','admission-owner','admission-group-scope','admission-environment','compression-source','compression-summary','patrol-limits-source','drive-connection','journal-drive-status','notebook-state','notebook-action-note','memory-state','memory-mirror-state'])text(id,'');
  text('diary-preview','选择日期即可阅读日记。');
  for(const id of ['save-patrol-limits','reset-patrol-limits'])$(id).disabled=true;
});
