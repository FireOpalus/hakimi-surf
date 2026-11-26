const modulePage=new Map([...$('platform-nav-group').querySelectorAll('[data-module]')].map(group=>[
  group.dataset.module,group.querySelector('[role=tab]').dataset.tab
]));
const pluginPages=new Map();
const regionName=code=>({CN:'境内节点',KR:'境外节点'})[code]||code;
const weatherLocationPath='life/modules/weather/location';
const weatherSources={environment:'环境默认',coordinates:'手动经纬度',city:'城市搜索',server_ip:'服务器 IP'};
let weatherCandidate=null,weatherPlaces=[],weatherLookupVersion=0,weatherSaving=false;

function formatWeatherPlace(place){
  const coordinates=[place.latitude,place.longitude].map(n=>Number(n.toFixed(5))).join(', ');
  return place.label?place.label+'（'+coordinates+'）':coordinates;
}
function showWeatherLocation(place){
  text('weather-current',place?'当前地点 · '+formatWeatherPlace(place)+' · '+weatherSources[place.source]:'尚未设置观测地点');
  $('weather-reset').hidden=!place||place.source==='environment';
  $('weather-editor').disabled=weatherSaving;
}
function setWeatherCandidate(place,message=''){
  weatherCandidate=place;
  $('weather-save').disabled=!place;
  text('weather-preview',place?'待保存 · '+formatWeatherPlace(place):message);
}
function clearWeatherLookup(){
  weatherLookupVersion++;
  weatherPlaces=[];
  $('weather-city-select').replaceChildren();
  $('weather-results').hidden=true;
  setWeatherCandidate(null);
}
function previewCoordinates(){
  const lat=$('weather-lat'),lon=$('weather-lon');
  const valid=lat.validity.valid&&lon.validity.valid;
  setWeatherCandidate(valid?{source:'coordinates',latitude:lat.valueAsNumber,longitude:lon.valueAsNumber}:null,
    lat.value||lon.value?'纬度范围为 −90 至 90，经度范围为 −180 至 180。':'');
}
function changeWeatherMethod(){
  clearWeatherLookup();
  const method=$('weather-methods').querySelector('input:checked').value;
  for(const name of ['city','coordinates','server_ip'])$('weather-mode-'+name).hidden=name!==method;
  if(method==='coordinates')previewCoordinates();
}
function resetWeatherEditor(){
  for(const id of ['weather-query','weather-lat','weather-lon'])$(id).value='';
  $('weather-methods').querySelector('[value=city]').checked=true;
  changeWeatherMethod();
}
async function lookupWeather(method){
  clearWeatherLookup();
  const version=weatherLookupVersion,city=method==='city';
  const queryText=$('weather-query').value.trim();
  if(city&&queryText.length<2){setWeatherCandidate(null,'请输入至少两个字符。');return;}
  setWeatherCandidate(null,city?'正在查找城市…':'正在识别服务器地点…');
  try{
    const result=await api(weatherLocationPath+(city?'/cities':'/detect'),'POST',city?{query:queryText}:undefined);
    if(version!==weatherLookupVersion)return;
    if(!city){setWeatherCandidate(result);return;}
    weatherPlaces=result.places;
    if(!weatherPlaces.length){setWeatherCandidate(null,'没有找到该城市，可换个名称或输入经纬度。');return;}
    const select=$('weather-city-select');
    select.add(new Option('请选择地点',''));
    weatherPlaces.forEach((p,i)=>select.add(new Option(formatWeatherPlace(p),String(i))));
    $('weather-results').hidden=false;
    if(weatherPlaces.length===1){select.value='0';setWeatherCandidate(weatherPlaces[0]);}
    else{setWeatherCandidate(null,'找到 '+weatherPlaces.length+' 个地点，请按地区选择。');select.focus();}
  }catch(error){
    if(error.name!=='AbortError'&&version===weatherLookupVersion)setWeatherCandidate(null,error.message);
  }
}
async function persistWeatherPlace(reset){
  if(weatherSaving||(!reset&&!weatherCandidate))return;
  if(reset&&!(await confirmDialog('恢复环境变量中的默认地点？若未配置经纬度，需要先选择地点才能查询天气。',{danger:true,confirmLabel:'恢复默认'})))return;
  weatherSaving=true;$('weather-editor').disabled=true;
  try{
    const result=await api(weatherLocationPath,reset?'DELETE':'PUT',reset?undefined:weatherCandidate);
    resetWeatherEditor();
    applyModuleDetail(result);
    notice(reset?'已恢复环境默认地点':'地点已保存，下次查询将使用此地点');
  }catch(error){if(error.name!=='AbortError')notice(error.message,true);}
  finally{weatherSaving=false;if(token)$('weather-editor').disabled=false;}
}
$('weather-methods').addEventListener('change',changeWeatherMethod);
$('weather-city-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,()=>lookupWeather('city'));});
$('weather-query').addEventListener('input',clearWeatherLookup);
$('weather-city-select').addEventListener('change',e=>setWeatherCandidate(weatherPlaces[e.target.value]||null));
$('weather-detect').addEventListener('click',e=>busy(e.currentTarget,()=>lookupWeather('server_ip')));
for(const id of ['weather-lat','weather-lon'])$(id).addEventListener('input',previewCoordinates);
$('weather-save').addEventListener('click',()=>persistWeatherPlace(false));
$('weather-reset').addEventListener('click',()=>persistWeatherPlace(true));

function renderModuleActions(target,actions){
  if(!actions.length){target.replaceChildren(el('p','当前模块没有注册动作。','muted'));return;}
  const modes={active:'按动作名',shadow:'对照旧路由',legacy:'仅旧路由',off:'停用'};
  table(target,['动作','发送范围','当前状态','路由方式'],actions.map(a=>{
    const control=el('select');
    control.setAttribute('aria-label',a.name+' 路由方式');
    for(const mode of (a.compatibility?Object.keys(modes):['active','off'])){
      const option=el('option',modes[mode]);option.value=mode;control.append(option);
    }
    control.value=a.mode;
    control.addEventListener('change',()=>busy(control,async()=>{
      try{
        const result=await api('life/actions/'+encodeURIComponent(a.name),'PUT',{mode:control.value,view:'module'});
        applyModuleDetail(result);
        notice('已保存 '+a.name+' 的路由方式');
      }catch(error){control.value=a.mode;throw error;}
    }));
    return [a.name+' · '+a.description,({owner:'仅 Owner',social:'获准群 / Owner',internal:'模块内部'})[a.audience],a.available?'可用':a.reason,control];
  }));
}
function moduleSchedulePanel(d){
  const panel=el('section',undefined,'module-schedule'),spec=d.scheduling;
  if(!spec)return panel;
  const fields=Object.entries(spec.fields),timers=spec.timers;
  if(!fields.length&&!timers.length)return panel;
  panel.append(el('h3','模块调度'));
  for(const t of timers){
    const when=t.next_at?dateTime(t.next_at):'暂无可执行时段';
    const cadence=t.cadence==='hourly'?'每小时整点':(t.at||'当日可发送时段结束前 15 分钟');
    panel.append(el('p',t.label+' · '+cadence+' · 下次排程 '+when,'muted'));
  }
  if(!fields.length)return panel;
  const form=el('form'),grid=el('div',undefined,'grid'),controls=[];
  for(const [key,field] of fields){
    const label=el('label'),input=el(field.enum?'select':'input');
    input.name=key;input.setAttribute('aria-label',field.title||key);
    if(field.enum){
      for(const option of field.enum)input.add(new Option(String(option),String(option)));
      input.value=String(spec.values[key]);
    }else if(field.type==='boolean'){
      input.type='checkbox';input.checked=spec.values[key];label.append(input,document.createTextNode(' '+(field.title||key)));
    }else{
      input.type=field.type==='integer'||field.type==='number'?'number':'text';
      if(input.type==='number')input.step=field.type==='integer'?'1':'any';
      if(field.minimum!==undefined)input.min=field.minimum;
      if(field.maximum!==undefined)input.max=field.maximum;
      if(field.minLength!==undefined)input.minLength=field.minLength;
      if(field.maxLength!==undefined)input.maxLength=field.maxLength;
      if(field.pattern)input.pattern=field.pattern;
      input.required=true;input.value=spec.values[key];
    }
    if(input.type!=='checkbox')label.append(document.createTextNode(field.title||key),input);
    if(field.description)label.append(el('small',field.description,'muted'));
    grid.append(label);controls.push([key,field,input]);
  }
  const path='life/modules/'+encodeURIComponent(d.id)+'/schedule';
  const buttons=el('div',undefined,'row'),save=el('button','保存模块调度','primary'),reset=el('button','恢复模块默认值','secondary');
  save.type='submit';reset.type='button';buttons.append(save,reset);form.append(grid,buttons);panel.append(form);
  const refresh=result=>panel.replaceWith(moduleSchedulePanel(result));
  form.addEventListener('submit',e=>{e.preventDefault();busy(save,async()=>{
    const values={};
    for(const [key,field,input] of controls){
      let value=input.value;
      if(field.enum)value=field.enum[input.selectedIndex];
      else if(field.type==='boolean')value=input.checked;
      else if(input.type==='number')value=input.valueAsNumber;
      if(value!==spec.values[key])values[key]=value;
    }
    if(!Object.keys(values).length){notice('当前设置已保存');return;}
    refresh(await api(path,'PUT',values));notice('模块调度设置已保存');
  });});
  reset.addEventListener('click',()=>busy(reset,async()=>{
    if(!(await confirmDialog('恢复此模块的默认调度设置？',{danger:true,confirmLabel:'恢复默认'})))return;
    refresh(await api(path,'DELETE'));notice('已恢复模块调度默认值');
  }));
  return panel;
}
const switchingModules=new Set();
const moduleTitle=m=>m.title||(modulePage.has(m.id)?sections.get(modulePage.get(m.id)).title:m.id);
function moduleIssue(d){
  return d.configuration_issue==='weather_location_missing'?'尚未设置观测地点':d.configuration_issue||'';
}
function syncModuleSwitches(id,enabled){
  for(const input of document.querySelectorAll('input[data-module-switch]')){
    if(input.dataset.moduleSwitch!==id)continue;
    input.checked=enabled;
    input.disabled=switchingModules.has(id);
    input.closest('label').lastElementChild.textContent=enabled?'已开启':'已关闭';
  }
}
function moduleSwitch(d){
  const label=el('label',undefined,'module-switch'),input=el('input'),track=el('span',undefined,'track');
  input.type='checkbox';input.setAttribute('role','switch');input.dataset.moduleSwitch=d.id;
  input.checked=d.enabled;input.disabled=switchingModules.has(d.id);
  input.setAttribute('aria-label',moduleTitle(d)+'模块开关');track.setAttribute('aria-hidden','true');
  label.append(input,track,el('span',d.enabled?'已开启':'已关闭'));
  input.addEventListener('change',async()=>{
    const wanted=input.checked;
    if(switchingModules.has(d.id))return;
    switchingModules.add(d.id);
    syncModuleSwitches(d.id,wanted);
    let confirmed=!wanted;
    try{
      const result=await api('life/modules/'+encodeURIComponent(d.id)+'/enabled','PUT',{enabled:wanted});
      confirmed=result.enabled;
      applyModuleDetail(result);
      notice(moduleTitle(d)+(confirmed?'已开启':'已关闭'));
    }catch(error){
      if(error.name!=='AbortError'){
        notice(error.message,true);
        if(token){
          try{const current=await api('life/modules/'+encodeURIComponent(d.id));confirmed=current.enabled;applyModuleDetail(current);}catch{}
        }
      }
    }finally{
      switchingModules.delete(d.id);syncModuleSwitches(d.id,confirmed);
    }
  });
  return label;
}
function renderModuleTools(container,d){
  container.dataset.moduleTools=d.id;
  const actions=el('div',undefined,'table'),summary=el('p',undefined,'muted');
  const weather=d.id==='weather';
  const read=el('button',weather?'查询当前天气':'读取传感器摘要','secondary');read.type='button';
  summary.setAttribute('aria-live','polite');
  read.addEventListener('click',()=>busy(read,async()=>{
    summary.textContent='';
    const result=await api('life/modules/'+encodeURIComponent(d.id)+(weather?'/current':'/sensor'));
    summary.textContent=weather?result.summary+' · '+result.source:result.summary||'暂无传感器摘要';
  }));
  read.disabled=!d.enabled||!!d.configuration_issue||(weather&&!d.actions.some(a=>a.name==='weather.read'&&a.available));
  const state=el('p',regionName(d.region)+' · '+(!d.enabled?'模块已关闭':moduleIssue(d)||(weather?'按需查询':d.proactive_paused?'主动动作已暂停':'允许调度主动动作')),'muted');
  container.replaceChildren(state,moduleSchedulePanel(d),actions,read,summary);
  renderModuleActions(actions,d.actions);
}
async function loadModule(id){
  const tools=$(modulePage.get(id)+'-actions');
  tools.replaceChildren();
  if(id==='qq_social'){
    text('connection-label','等待连接状态');$('connection-dot').className='status-dot';
    text('ws-source','—');text('ws-heartbeat','—');
  }else if(id==='bilibili_surf'){
    text('bili-module-backend','—');text('bili-module-autonomous','—');
  }else if(id==='minecraft'){
    $('mc-overview').replaceChildren();text('life-mc-status','');
    text('mc-plan-summary','');$('mc-waypoints-list').replaceChildren();$('mc-action-log').replaceChildren();
    mcControls.forEach(control=>$(control).disabled=true);
  }
  const d=await api('life/modules/'+encodeURIComponent(id));
  applyModuleDetail(d);
}
function applyModuleDetail(d){
  const id=d.id,key=modulePage.get(id);
  if(key){
    if(key===tab){
      const slot=$('module-heading-switch');
      if(slot.querySelector('input')?.dataset.moduleSwitch!==id)slot.replaceChildren(moduleSwitch(d));
      slot.hidden=false;
    }
    renderModuleTools($(key+'-actions'),d);
  }else{
    for(const target of document.querySelectorAll('[data-module-tools]')){
      if(target.dataset.moduleTools===id)renderModuleTools(target,d);
    }
  }
  syncModuleSwitches(id,d.enabled);
  if(id==='qq_social')updateConnection(d.status);
  if(id==='weather')showWeatherLocation(d.status.location);
  if(id==='bilibili_surf'){
    text('bili-module-backend',d.status.cn_delegate?'境内专线':d.status.backend==='remote_node'?'境内代理模式':'境外本机');
    text('bili-module-autonomous',d.status.autonomous?'已开启':'已关闭');
  }
  if(id==='minecraft'){
    renderMinecraft(d.status);
    for(const control of mcControls)$(control).disabled=!d.enabled;
  }
  if(id==='bilibili_surf')$('bili').querySelector('[data-action="patrol"]').disabled=!d.enabled;
}
function catalogCard(m,{page,indicator,meta,note}){
  const title=moduleTitle(m),card=el('article',undefined,'module-card');
  const heading=el('div',undefined,'module-name-row'),glyph=el('span',undefined,'module-icon');
  const icon=$('catalog-icon-'+m.id)?.content.firstElementChild||$('nav-'+(page||'modules')).querySelector('svg');
  glyph.append(icon.cloneNode(true));
  heading.append(el('h3',title),indicator);
  const foot=el('div',undefined,'module-foot');
  foot.append(el('small',meta));
  card.append(glyph,heading);
  if(m.description)card.append(el('p',m.description));
  if(note)card.append(el('p',note,'module-state-note'));
  if(page){
    const open=el('button','打开模块 →','module-open');open.type='button';
    open.setAttribute('aria-label','打开'+title);open.dataset.openTab=page;foot.append(open);
  }else{
    foot.append(el('span','后台常驻 · 无独立页面','module-open-none'));
  }
  card.append(foot);
  return card;
}
function moduleCard(m){
  const page=sections.get(m.panel)?.module===m.id?m.panel:modulePage.get(m.id);
  const card=catalogCard(m,{page,indicator:moduleSwitch(m),
    meta:regionName(m.region)+' · '+m.action_count+' 项动作',note:moduleIssue(m)});
  card.dataset.moduleId=m.id;
  return card;
}
function registerPluginPage(m){
  if(modulePage.has(m.id))return;
  const key='plugin-'+m.id, group=el('div'), panel=el('section',undefined,'tab');
  group.dataset.module=m.id;
  const navBtn=$('nav-modules').cloneNode(true);
  navBtn.id='nav-'+key;navBtn.dataset.tab=key;navBtn.dataset.description=m.description||'';
  navBtn.setAttribute('aria-controls',key);navBtn.setAttribute('aria-selected','false');navBtn.tabIndex=-1;
  const tplIcon=$('catalog-icon-'+m.id)?.content.firstElementChild;
  if(tplIcon)navBtn.querySelector('svg')?.replaceWith(tplIcon.cloneNode(true));
  navBtn.querySelector('span').textContent=m.title||m.id;
  navBtn.addEventListener('click',()=>navigate(key));
  group.append(navBtn);$('platform-nav-group').append(group);
  panel.id=key;panel.hidden=true;panel.setAttribute('role','tabpanel');panel.setAttribute('aria-labelledby',navBtn.id);
  const tools=el('div',undefined,'card');tools.id=key+'-actions';panel.append(tools);
  const protocols=m.chat_platforms||[],privacy=el('div',undefined,'table');
  if(protocols.length){
    const card=el('section',undefined,'card');card.append(el('h2','会话隐私'),privacy);panel.append(card);
  }
  $('modules').after(panel);
  const section={button:navBtn,workspace:'modules',module:m.id,title:m.title||m.id,group:'平台模块'};
  sections.set(key,section);tabButtons.push(navBtn);modulePage.set(m.id,key);
  finderEntries.push({key,label:section.title,context:sectionLocation(section)});
  const path='life/modules/'+encodeURIComponent(m.id)+'/privacy';
  function draw(rows){
    if(!token)return;
    table(privacy,['会话','账号','群组或用户','隐私等级'],rows.map(row=>{
      const select=el('select');select.setAttribute('aria-label',row.target_id+' 隐私等级');
      for(const [value,label] of [['default','默认（高隐私）'],['high','高隐私'],['low','低隐私']]){
        const option=el('option',label);option.value=value;select.append(option);
      }
      select.value=row.override_privacy;
      select.addEventListener('change',()=>busy(select,async()=>{
        try{draw(await api(path,'PUT',{key:row.key,privacy:select.value}));notice('会话隐私等级已保存');}
        catch(error){select.value=row.override_privacy;throw error;}
      }));
      return [row.kind==='group'?'群聊':'私聊',row.platform+' / '+row.bot_id,row.target_id,select];
    }));
  }
  const readers=[()=>loadModule(m.id)];
  if(protocols.length)readers.push(()=>api(path).then(draw));
  if(m.owner_delivery){
    const card=el('section',undefined,'card'),state=el('p',undefined,'muted'),row=el('div',undefined,'row');
    state.setAttribute('aria-live','polite');
    const route='life/modules/'+encodeURIComponent(m.id)+'/owner-delivery';
    const read=async()=>{
      const d=await api(route);
      state.textContent=(d.connected?'在线':'等待连接')+' · '+(d.delivery_enabled?'允许投递':'已停用投递')+
        (d.next_digest_at?' · 下次日报 '+lifeFmt(d.next_digest_at,d.timezone):'');
    };
    readers.push(read);
    for(const [action,label] of [['test','发送测试私信（0 Token）'],['digest','立即生成并发送今日汇报']]){
      const actBtn=el('button',label,'secondary');actBtn.type='button';
      actBtn.addEventListener('click',()=>busy(actBtn,async()=>{
        const result=await api(route+'/'+action,'POST');
        notice(deliveryStages[result.receipt?.stage]||dmStatus[result.delivery]||dmStatus[result.status]||'已处理');
        await read();
      }));row.append(actBtn);
    }
    card.append(el('h2','Owner 投递'),state,row);panel.append(card);
  }
  tabLoaders[key]=()=>Promise.all(readers.map(read=>read()));
  pluginPages.set(m.id,()=>{
    delete tabLoaders[key];sections.delete(key);modulePage.delete(m.id);
    tabButtons.splice(tabButtons.indexOf(navBtn),1);
    const index=finderEntries.findIndex(entry=>entry.key===key);if(index>=0)finderEntries.splice(index,1);
    group.remove();panel.remove();pluginPages.delete(m.id);
  });
}
function systemCard(m){
  const status=m.status||{};
  const page=m.panel&&sections.has(m.panel)?m.panel:null;
  const note=[status.last_error,status.last_success&&'上次同步 '+dateTime(status.last_success)].filter(Boolean).join(' · ');
  const card=catalogCard(m,{page,indicator:el('span','常驻','module-resident'),meta:status.detail,note});
  card.dataset.systemModule=m.id;
  return card;
}
async function systemDirectory(){
  const catalog=$('system-catalog');
  try{
    const list=await api('system-modules');catalog.replaceChildren(...list.map(systemCard));
  }catch(error){
    if(error.name==='AbortError')throw error;
    catalog.replaceChildren(el('p','系统状态暂时无法读取，请刷新目录。','empty-state'));
  }
}
async function moduleDirectory(){
  const container=$('module-catalog');
  try{
    const list=await api('life/modules');
    const registered=new Set(list.map(m=>m.id));
    for(const [id,remove] of pluginPages)if(!registered.has(id))remove();
    list.forEach(registerPluginPage);
    for(const group of $('modules-nav').querySelectorAll('[data-module]'))group.hidden=!registered.has(group.dataset.module);
    container.replaceChildren(...list.map(moduleCard));
    if(!list.length)container.append(el('p','当前没有已安装的模块。','empty-state'));
  }catch(error){
    if(error.name==='AbortError')throw error;
    container.replaceChildren(el('p','模块目录暂时无法读取，请点击刷新目录重试。','empty-state'));
  }
}

const colabErrorLabels={
  colab_unconfigured:'Colab Worker 尚未配置（请检查 COLAB_WORKER_URL / TOKEN）',
  colab_disabled:'Colab 云端算力调度当前已关闭',
  colab_worker_unavailable:'Colab Worker 离线或不可达',
  colab_gpu_not_allowed:'该作业类型不支持所选 GPU 型号',
  colab_daily_job_limit:'今日 Colab 作业次数已达上限',
  colab_daily_cu_budget:'今日 Colab CU 预算已耗尽',
  colab_job_limit_reached:'Colab 排队/并发作业已达上限',
  colab_invalid_job:'作业类型未注册或参数无效',
  colab_job_not_found:'未找到指定作业',
  colab_job_not_cancellable:'该作业当前状态不可取消',
  ref_voice_not_found:'Drive models/ 未找到可用参考干声',
  svc_all_engines_failed:'双引擎 SVC 推理均失败',
  invalid_song_engine:'翻唱引擎参数无效'
};
const colabJobTypeLabels={rvc_cover:'翻唱学歌 (SVC/RVC)',rvc_train:'RVC 音色训练'};
const colabEngineLabels={dual:'双引擎亲挑',yingmusic_svc:'YingMusic-SVC',seed_vc:'Seed-VC',rvc:'RVC'};
const colabJobStatusLabels={queued:'排队中',running:'运行中',succeeded:'已完成',failed:'失败',cancelled:'已取消'};
const colabJobStatusVariant={queued:'warn',running:'paid',succeeded:'ok',failed:'err',cancelled:'muted'};

function throwColabError(err){
  if(err?.name==='AbortError')throw err;
  const msg=String(err?.message||'操作失败');
  const code=msg.split('（')[0].trim();
  throw Error(colabErrorLabels[code]||msg);
}
function renderColabOverview(d){
  if(!d)return;
  cachedColabOverview=d;
  colabUnavailableReason='';
  const w=d.worker||{},c=d.config||{},t=d.today||{};
  const warmSessions=Array.isArray(d.warm_sessions)?d.warm_sessions:(Array.isArray(w.warm_sessions)?w.warm_sessions:[]);
  const warmLead=warmSessions[0]||null;
  $('colab-pills').replaceChildren(...[
    d.configured?pill('Worker 已配置','ok'):pill('未配置 COLAB_WORKER_URL','muted'),
    w.reachable?pill('Worker 在线','ok'):pill(d.configured?'Worker 离线':'未连接',d.configured?'err':'muted'),
    c.enabled?pill('调度开启 · 按模块自选 GPU','vertex'):pill('调度已暂停','warn'),
    ((w.active_sessions||0)>0||w.running_job_id)?pill('活跃会话 '+(w.active_sessions||0)+(w.running_job_id?' · 作业 '+String(w.running_job_id).slice(0,8):''),'paid'):pill('当前无活跃会话','muted'),
    warmLead?pill('热机保活 '+warmSessions.length+' · '+String(warmLead.gpu||'L4')+' 剩余 '+formatTtlCompact(warmLead.remaining_ttl_seconds),'ok'):null
  ].filter(Boolean));
  const cuAvail=w.compute_units!=null?Number(w.compute_units):null;
  const jobsCount=Number(t.jobs_count??0);
  const cuUsed=Number(t.cu_used??0);
  $('colab-kpi-strip').replaceChildren(
    kpiCard('Compute Units 余额',cuAvail!=null?cuAvail.toFixed(2)+' CU':'未知',w.reachable?'Colab 账号实时可用算力余额':'Worker 离线时暂不可查询',cuAvail!=null?(cuAvail<2?'warn':'ok'):''),
    kpiCard('运行中会话 / 作业',String(w.active_sessions??0)+' 会话',w.running_job_id?'运行作业 '+String(w.running_job_id).slice(0,12):'暂无运行中作业',(w.active_sessions||0)>0?'accent':''),
    kpiCard('今日作业次数',jobsCount+' 次','不设每日次数上限 · 排队上限 20',''),
    kpiCard('今日 CU 消耗',cuUsed.toFixed(2)+' CU','不设每日 CU 上限 · 仅实时统计','')
  );
  const jobSpecs=Array.isArray(d.job_types)?d.job_types:[];
  const supportedTypes=Array.isArray(d.supported_job_types)?d.supported_job_types:(jobSpecs.length?jobSpecs.map(s=>s.job_type):(Array.isArray(w.supported_job_types)?w.supported_job_types:[]));
  const defaultTimeout=c.job_timeout_seconds??1800;
  const orphanMins=c.auto_terminate_orphan_minutes??20;
  const policyCount=jobSpecs.length||supportedTypes.length;
  text('colab-state','已注册作业：'+policyCount+' 类 · 默认单作业超时：'+defaultTimeout+' 秒 · 孤儿会话清退：'+orphanMins+' 分钟 · 队列上限 20'+(w.error?' · Worker 状态提示：'+(colabErrorLabels[w.error]||w.error):''));
  text('colab-job-types','已注册作业类型：'+(supportedTypes.length?supportedTypes.map(jt=>colabJobTypeLabels[jt]?colabJobTypeLabels[jt]+'（'+jt+'）':jt).join('、'):'暂无已注册作业类型'));

  table('colab-job-policies-table',['作业类型','所属模块','首选 GPU','回退顺序','允许卡型','单作业超时'],jobSpecs.map(spec=>{
    const jt=String(spec?.job_type||'');
    const jtLabel=colabJobTypeLabels[jt]?colabJobTypeLabels[jt]+'（'+jt+'）':(jt||'—');
    const owner=String(spec?.owner_module||'').trim();
    const defGpu=String(spec?.default_gpu||'').trim();
    const fallbacks=Array.isArray(spec?.fallback_gpus)?spec.fallback_gpus.map(g=>String(g||'').trim()).filter(Boolean):[];
    const allowed=Array.isArray(spec?.allowed_gpus)?spec.allowed_gpus.map(g=>String(g||'').trim()).filter(Boolean):[];
    const specTimeout=spec?.timeout_seconds!=null&&Number.isFinite(Number(spec.timeout_seconds))?Number(spec.timeout_seconds):defaultTimeout;
    return [
      jtLabel,
      owner?el('span',owner,'code-mono'):'—',
      defGpu?pill(defGpu,(defGpu==='L4'||defGpu==='T4')?'ok':'vertex'):'—',
      fallbacks.length?fallbacks.join(' → '):'不自动回退',
      allowed.length?allowed.join(' / '):'—',
      specTimeout+' 秒'
    ];
  }));

  $('colab-enabled').checked=Boolean(c.enabled);
  $('colab-job-timeout').value=defaultTimeout;
  $('colab-orphan-minutes').value=orphanMins;
  text('colab-config-source',(d.source==='sqlite'||d.source==='admin')?'当前配置来源：后台 SQLite 覆盖（重启保留）':'当前配置来源：默认 / 环境变量配置');
  $('colab-save').disabled=false;
  $('colab-reset').disabled=false;
  $('colab-terminate-sessions').disabled=!d.configured;
  clearDirty('colab-config-form');
  syncTrendButtons();
  renderColabAnalyticsOnPage(d,'colab');

  table('colab-jobs-table',['作业 ID','类型','目标','状态','GPU','CU 消耗','错误信息','操作'],(d.jobs||[]).map(job=>{
    const p=job.payload||{};
    const r=job.result&&typeof job.result==='object'?job.result:{};
    const target=p.title||p.model_name||p.query||p.target||p.inbox_filename||'—';
    const engReq=String(r.engine_requested||p.engine||'').trim();
    const engUsed=String(r.engine_used||'').trim();
    const cands=Array.isArray(r.candidates)?r.candidates:[];
    const targetMeta=[];
    if(engReq)targetMeta.push(colabEngineLabels[engReq]||engReq);
    if(cands.length){
      const okCount=cands.filter(c=>c&&c.status==='succeeded').length;
      targetMeta.push(okCount+'/'+cands.length+' 候选成功'+(engUsed&&engUsed!==engReq?' → '+(colabEngineLabels[engUsed]||engUsed):''));
    }else if(engUsed&&engUsed!==engReq){
      targetMeta.push('→ '+(colabEngineLabels[engUsed]||engUsed));
    }
    if(p.pitch)targetMeta.push('Pitch '+(p.pitch>0?'+'+p.pitch:p.pitch));
    const targetDisplay=target+(targetMeta.length?'（'+targetMeta.join(' · ')+'）':'');
    const gpu=job.assigned_gpu||job.preferred_gpu||'—';
    const cu=job.cu_used!=null?Number(job.cu_used).toFixed(3):'—';
    const failedCands=cands
      .filter(c=>c&&c.status==='failed')
      .map(c=>(colabEngineLabels[c.engine]||c.engine||c.key||'候选')+':'+(colabErrorLabels[c.error_code]||c.error_code||c.error||'失败'));
    const errText=[colabErrorLabels[job.error_code]||job.error_code,job.error,...failedCands].filter(Boolean).join(' · ')||'—';
    const canCancel=job.status==='queued'||job.status==='running';
    const actionNode=canCancel?button('取消',async()=>{
      if(!(await confirmDialog('确认取消 Colab 作业「'+target+'」('+(String(job.id||'').slice(0,8))+')？',{danger:true,confirmLabel:'取消作业'})))return;
      try{await api('colab/jobs/'+encodeURIComponent(job.id)+'/cancel','POST');}
      catch(err){throwColabError(err);}
      notice('已取消作业 '+String(job.id||'').slice(0,8));
      await colabCompute();
    },'danger'):el('span',job.finished_at?dateTime(job.finished_at):dateTime(job.created_at),'muted');
    return [
      el('span',String(job.id||'').slice(0,10),'code-mono'),
      colabJobTypeLabels[job.job_type]||job.job_type||'—',
      targetDisplay,
      pill(colabJobStatusLabels[job.status]||job.status||'—',colabJobStatusVariant[job.status]||'muted'),
      gpu,
      cu,
      errText,
      actionNode
    ];
  }));
}
async function colabCompute(){
  let d;
  try{d=await api('colab');}
  catch(err){throwColabError(err);}
  if(!token)return;
  renderColabOverview(d);
}
$('refresh-colab').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('colab-compute','刷新 Colab 状态')))return;
  await colabCompute();
}));
$('colab-refresh-jobs').addEventListener('click',e=>busy(e.target,async()=>{
  try{renderColabOverview(await api('colab'));}
  catch(err){throwColabError(err);}
  notice('已刷新 Colab 作业队列');
}));
$('colab-terminate-sessions').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('确认紧急清退全部活跃 Colab GPU 会话？正在运行的云端 GPU 作业将被立即中止。',{danger:true,confirmLabel:'紧急清退全部会话'})))return;
  let res;
  try{res=await api('colab/terminate-sessions','POST');}
  catch(err){throwColabError(err);}
  notice('已清退会话 '+(res?.terminated_sessions??0)+' 个 · 取消作业 '+(res?.cancelled_jobs??0)+' 个'+(res?.detail?' · '+res.detail:''));
  await colabCompute();
}));
$('colab-config-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('colab-save'),async()=>{
    const timeoutSec=Number($('colab-job-timeout').value);
    const orphanMins=Number($('colab-orphan-minutes').value);
    if(!Number.isInteger(timeoutSec)||timeoutSec<120||timeoutSec>7200)throw Error('默认单作业超时秒数须为 120～7200 的整数');
    if(!Number.isInteger(orphanMins)||orphanMins<5||orphanMins>120)throw Error('孤儿会话自动清退分钟数须为 5～120 的整数');
    const payload={
      enabled:$('colab-enabled').checked,
      job_timeout_seconds:timeoutSec,
      auto_terminate_orphan_minutes:orphanMins
    };
    let next;
    try{next=await api('colab','PUT',payload);}
    catch(err){throwColabError(err);}
    renderColabOverview(next);
    notice('Colab 算力配置已保存并立即生效');
  });
});
$('colab-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复默认 Colab 算力调度与防失控配置？',{danger:true,confirmLabel:'恢复默认'})))return;
  let next;
  try{next=await api('colab','DELETE');}
  catch(err){throwColabError(err);}
  renderColabOverview(next);
  notice('已恢复默认 Colab 算力配置');
}));

const tabLoaders={
  overview:dashboard,ledger,'api-routing':providers,'reply-settings':replySettings,
  thinking:()=>Promise.all([thinking(),generationCaps()]),filter:filters,admission,
  modules:()=>Promise.all([moduleDirectory(),systemDirectory()]),journal:diaries,'drive-sync':driveSync,'event-ledger':eventLedger,'voice-companion':voiceCompanion,'screen-watch':screenWatch,'colab-compute':colabCompute,'identity-center':identityCenter,'plugin-bus':pluginBus,
  qq:()=>Promise.all([loadModule('qq_social'),qqPrivacy(),longnick()]),bili:()=>loadModule('bilibili_surf'),
  weather:()=>loadModule('weather'),minecraft:()=>loadModule('minecraft'),'group-policy':()=>Promise.all([groups(),bots()]),
  memory:()=>Promise.all([sessions(),compression()]),
  'bili-account':biliAccount,community:async()=>{
    const [moderation,profiles]=await Promise.allSettled([community(),affection()]);
    if(moderation.status==='rejected')throw moderation.reason;
    if(profiles.status==='rejected'&&profiles.reason.name!=='AbortError')notice('好感度接口暂不可用：'+profiles.reason.message,true);
  },
  memes,songs,proactive,life,patrol:()=>Promise.all([patrolLimits(),subscriptions()]),
  notebook,logs,sandbox:groups
};
function refreshTab(key=tab){return tabLoaders[key]();}

const tabButtons=[...$('sidebar').querySelectorAll('[role=tab]')];
const workspaceNames={core:'主控台',modules:'功能模块'};
const lastTab={core:'overview',modules:'modules'};
const sections=new Map(tabButtons.map(btn=>{
  const platform=btn.closest('[data-module]'),group=btn.closest('.nav-group');
  return [btn.dataset.tab,{
    workspace:btn.closest('nav').dataset.workspace,module:platform?.dataset.module,
    button:btn,title:btn.querySelector('span').textContent,
    group:(platform?.querySelector('[role=tab] span')||group.querySelector('.nav-group-label'))?.textContent
  }];
}));
const sectionLocation=section=>[workspaceNames[section.workspace],section.group].filter(Boolean).join(' / ');

function syncHash(key,replace=false){
  const desired='#'+key;
  if(location.hash===desired)return;
  try{
    if(replace)history.replaceState({tab:key},'',desired);
    else history.pushState({tab:key},'',desired);
  }catch{}
}

function selectTab(key){
  const section=sections.get(key);
  if(!section)return;
  tab=key;
  const workspace=section.workspace;
  lastTab[workspace]=key;
  $('core-nav').hidden=workspace!=='core';
  $('modules-nav').hidden=workspace!=='modules';
  for(const b of $('workspace-switch').querySelectorAll('button')){
    b.setAttribute('aria-pressed',String(b.dataset.workspaceTarget===workspace));
  }
  text('drawer-title',workspaceNames[workspace]);
  for(const [id,{button:tabBtn,module}] of sections){
    if(tabBtn.hasAttribute('data-subpage'))tabBtn.hidden=module!==section.module;
    const active=id===key;
    $(id).hidden=!active;tabBtn.setAttribute('aria-selected',String(active));tabBtn.tabIndex=active?0:-1;
  }
  text('page-title',section.title);
  const headingSwitch=$('module-heading-switch');
  if(modulePage.get(headingSwitch.querySelector('input')?.dataset.moduleSwitch)!==key){
    headingSwitch.replaceChildren();headingSwitch.hidden=true;
  }
  text('page-kicker',sectionLocation(section));
  if(workspace==='modules'&&key!=='modules'){
    const crumb=(label,targetKey)=>{
      const b=el('button',label,'crumb-link');b.type='button';b.dataset.openTab=targetKey;return b;
    };
    const kicker=$('page-kicker');
    kicker.replaceChildren(crumb('模块目录','modules'));
    if(section.button.hasAttribute('data-subpage')){
      const parent=modulePage.get(section.module);
      kicker.append(' / ',crumb(sections.get(parent).title,parent));
    }
  }
  $('last-updated').hidden=key!=='overview';
  $('refresh-module').hidden=!section.module||section.button.hasAttribute('data-subpage');
  text('page-description',section.button.dataset.description);
  $('page-description').hidden=key==='overview';
  $('overview-day').hidden=$('overview-tools').hidden=key!=='overview';
  updatePageDirtyBadge();
  document.title=section.title+' · 洁米奈星夜';
  window.scrollTo({top:0,behavior:'instant'});
}

const panelLoads=new WeakMap();
// 已成功加载过的面板再次进入时保留旧数据直接显示（仅在刷新期间锁定交互），不再整页变暗。
const loadedPanels=new Set();
onLogout(()=>loadedPanels.clear());
let activeLoads=0;
function trackLoad(promise){
  activeLoads++;$('load-bar').classList.add('active');
  return promise.finally(()=>{
    activeLoads=Math.max(0,activeLoads-1);
    if(!activeLoads)$('load-bar').classList.remove('active');
  });
}
let lastVisit=0;
let navPromptPending=false;
async function navigate(key,{focusContent=false,heading=null,fromHash=false,replaceHash=false}={}){
  if(!token||!sections.has(key))return;
  if(key!==tab&&isTabDirty(tab)){
    if(navPromptPending)return;
    navPromptPending=true;
    let ok=false;
    try{ok=await confirmDiscardTabChanges(tab,'切换页面');}
    finally{navPromptPending=false;}
    if(!ok){
      if(fromHash)syncHash(tab,true);
      return;
    }
  }
  const panel=$(key),visit=++lastVisit;
  const drawerOpen=$('sidebar').classList.contains('open');
  closeNavigation();
  const changed=key!==tab;
  selectTab(key);
  if(!fromHash&&(changed||replaceHash||location.hash!=='#'+key)){
    syncHash(key,replaceHash||!changed);
  }
  if(drawerOpen||focusContent)$('page-title').focus({preventScroll:true});
  let loading=panelLoads.get(panel);
  if(!loading){
    panel.inert=true;panel.setAttribute('aria-busy','true');
    if(loadedPanels.has(key))panel.dataset.refreshing='1';
    loading=trackLoad(refreshTab(key).then(()=>{loadedPanels.add(key);})).finally(()=>{
      panel.inert=false;panel.removeAttribute('aria-busy');delete panel.dataset.refreshing;panelLoads.delete(panel);
    });
    panelLoads.set(panel,loading);
  }
  try{await loading;}
  catch(e){if(visit===lastVisit&&token&&e.name!=='AbortError')notice(e.message,true);return;}
  if(!heading||visit!==lastVisit||!token)return;
  revealSectionFor(heading);
  if(heading.closest('[hidden]'))return;
  for(let parent=heading.parentElement;parent&&parent!==panel;parent=parent.parentElement){
    if(parent.tagName==='DETAILS')parent.open=true;
  }
  heading.focus({preventScroll:true});heading.scrollIntoView({block:'start'});
}

function handleHashRoute(){
  if(!token)return;
  const key=location.hash.replace(/^#/,'');
  if(!key||key===tab||!sections.has(key))return;
  navigate(key,{fromHash:true});
}
window.addEventListener('hashchange',handleHashRoute);
window.addEventListener('popstate',handleHashRoute);

$('login-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy(e.submitter,async()=>{
    $('login-error').hidden=true;$('token').removeAttribute('aria-invalid');
    token=$('token').value;$('token').value='';
    try{
      const first=await fetchOverviewPayload();
      await dashboard(first);
    }catch(err){
      if(err.name==='AbortError')return;
      token='';text('login-error',err.message);$('login-error').hidden=false;
      $('token').setAttribute('aria-invalid','true');$('token').focus();return;
    }
    if(!token)return;
    $('login').hidden=true;$('app').hidden=false;hideNotice();
    for(const id of ['logout','open-finder','menu-toggle','workspace-switch'])$(id).hidden=false;
    document.body.classList.add('authenticated');
    const hashKey=location.hash.replace(/^#/,'');
    if(hashKey&&hashKey!=='overview'&&sections.has(hashKey)){
      selectTab('overview');
      navigate(hashKey,{fromHash:true,focusContent:true});
    }else{
      selectTab('overview');
      syncHash('overview',true);
      $('page-title').focus({preventScroll:true});
    }
    timer=setInterval(()=>{
      if(document.hidden||!token)return;
      const read=tab==='overview'?()=>dashboard(undefined,{lightweight:true}):tab==='filter'?decisionAudit:tab==='logs'?logs:null;
      if(read)read().catch(err=>{if(err.name!=='AbortError')notice(err.message,true);});
    },15000);
  });
});
$('logout').addEventListener('click',logout);

function updateConnection(d){
  text('connection-label',({online:'OneBot 在线',reconnecting:'OneBot 重连中',http_mode:'HTTP 接入'})[d.onebot]||'OneBot 未连接');
  $('connection-dot').className='status-dot '+(d.onebot==='online'?'online':d.onebot==='reconnecting'?'offline':'');
  const ws=d.onebot_detail||{};
  text('ws-heartbeat',ws.last_heartbeat_at?dateTime(ws.last_heartbeat_at):'暂无心跳');
  text('ws-source',ws.source_ip||'暂无连接');
}
function stampDashboard(){
  const now=new Date();$('last-updated').dateTime=now.toISOString();
  text('last-updated','更新于 '+now.toLocaleTimeString('zh-CN',{timeZone:displayZone,hour12:false}));
}
const narrowLayout=matchMedia('(max-width:900px)');
function closeNavigation(){
  $('sidebar').classList.remove('open');$('nav-scrim').hidden=true;
  $('menu-toggle').setAttribute('aria-expanded','false');document.body.classList.remove('nav-open');
  $('sidebar').inert=narrowLayout.matches;
  tabButtons.forEach(b=>b.tabIndex=b.dataset.tab===tab?0:-1);
}
function openNavigation(){
  $('sidebar').inert=false;$('sidebar').classList.add('open');$('nav-scrim').hidden=false;
  $('menu-toggle').setAttribute('aria-expanded','true');document.body.classList.add('nav-open');
  $('nav-'+tab).focus();
}
function dismissNavigation(){closeNavigation();$('menu-toggle').focus();}
$('menu-toggle').addEventListener('click',()=>$('sidebar').classList.contains('open')?dismissNavigation():openNavigation());
$('drawer-close').addEventListener('click',dismissNavigation);
$('nav-scrim').addEventListener('click',dismissNavigation);
$('sidebar').addEventListener('keydown',e=>{
  if(!$('sidebar').classList.contains('open'))return;
  if(e.key==='Escape'){e.preventDefault();dismissNavigation();}
  if(e.key==='Tab'){
    e.preventDefault();
    const current=tabButtons.find(b=>b.tabIndex===0)||$('nav-'+tab);
    (document.activeElement===$('drawer-close')?current:$('drawer-close')).focus();
  }
});
$('sidebar').addEventListener('keydown',e=>{
  if(!['ArrowDown','ArrowUp','Home','End'].includes(e.key))return;
  const visible=tabButtons.filter(b=>!b.closest('[hidden]'));
  const index=visible.indexOf(document.activeElement);
  if(index<0)return;
  e.preventDefault();
  const last=visible.length-1;
  const next=e.key==='Home'?0:e.key==='End'?last:(index+(e.key==='ArrowDown'?1:-1)+visible.length)%visible.length;
  visible.forEach((b,i)=>b.tabIndex=i===next?0:-1);visible[next].focus();
});
narrowLayout.addEventListener('change',closeNavigation);
closeNavigation();

const finder=$('module-finder'),query=$('finder-query'),resultList=$('finder-results');
const finderEntries=[];
for(const [key,section] of sections){
  const locationText=sectionLocation(section);
  finderEntries.push({key,label:section.title,context:locationText});
  for(const heading of $(key).querySelectorAll('.card h2, .card h3')){
    const label=heading.textContent.trim();
    if(!label||label===section.title)continue;
    heading.tabIndex=-1;
    finderEntries.push({key,label,context:locationText+' / '+section.title,heading});
  }
}
let matches=[],activeResult=0;
function markResult(index,scroll=false){
  activeResult=index;
  [...resultList.children].forEach((item,i)=>item.setAttribute('aria-selected',String(i===index)));
  const option=resultList.children[index];
  if(option){
    query.setAttribute('aria-activedescendant',option.id);
    if(scroll)option.scrollIntoView({block:'nearest'});
  }else query.removeAttribute('aria-activedescendant');
}
function filterFinder(){
  const normalize=s=>s.normalize('NFKC').toLocaleLowerCase().replace(/\s+/g,'');
  const term=normalize(query.value);
  matches=finderEntries.filter(item=>{
    if(sections.get(item.key).button.closest('[data-module][hidden]'))return false;
    if(!term)return !item.heading;
    if(item.heading?.closest('[hidden]:not(.tab)'))return false;
    return normalize([item.label,item.context,item.key].join(' ')).includes(term);
  });
  if(term){
    const rank=item=>normalize(item.label)===term?0:normalize(item.label).startsWith(term)?1:normalize(item.label).includes(term)?2:3;
    matches.sort((a,b)=>rank(a)-rank(b));
  }
  resultList.replaceChildren(...matches.map((item,i)=>{
    const option=el('div',undefined,'finder-option');
    option.id='finder-option-'+i;option.dataset.result=String(i);option.setAttribute('role','option');
    option.append(el('strong',item.label),el('small',item.context));
    return option;
  }));
  resultList.scrollTop=0;$('finder-empty').hidden=matches.length>0;markResult(0);
}
function openFinder(){
  if(!token||$('app').hidden||$('session-dialog').open||$('modal-dialog').open)return;
  if(!finder.open){
    closeNavigation();query.value='';filterFinder();finder.showModal();
  }
  query.focus();
}
function chooseResult(index){
  const item=matches[index];if(!item)return;
  finder.close();navigate(item.key,{focusContent:true,heading:item.heading});
}
$('open-finder').addEventListener('click',openFinder);
$('finder-close').addEventListener('click',()=>finder.close());
finder.addEventListener('keydown',e=>{
  if(e.key==='Escape'&&!e.isComposing){e.preventDefault();finder.close();}
});
finder.addEventListener('close',()=>{
  if(finder.open)return;
  query.value='';matches=[];resultList.replaceChildren();query.removeAttribute('aria-activedescendant');
});
query.addEventListener('input',filterFinder);
query.addEventListener('keydown',e=>{
  if(e.isComposing||!matches.length)return;
  if(e.key==='Enter'){e.preventDefault();chooseResult(activeResult);}
  if(e.key==='ArrowDown'||e.key==='ArrowUp'){
    e.preventDefault();markResult((activeResult+(e.key==='ArrowDown'?1:-1)+matches.length)%matches.length,true);
  }
});
resultList.addEventListener('click',e=>{
  const option=e.target.closest('[data-result]');if(option)chooseResult(Number(option.dataset.result));
});
if(/Mac|iPhone|iPad/i.test(navigator.platform))text('finder-shortcut','⌘ K');
document.addEventListener('keydown',e=>{
  if(token&&!e.isComposing&&(e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'&&!$('session-dialog').open&&!$('modal-dialog').open){
    e.preventDefault();openFinder();
  }
});
function setTheme(dark){
  document.documentElement.dataset.theme=dark?'dark':'light';
  $('theme-toggle').setAttribute('aria-pressed',String(dark));
  $('theme-toggle').setAttribute('aria-label',dark?'切换浅色主题':'切换深色主题');
}
// 主题已由 <head> 预载脚本在首帧前写入 data-theme，这里只同步按钮状态。
setTheme(document.documentElement.dataset.theme==='dark');
$('theme-toggle').addEventListener('click',()=>{const dark=document.documentElement.dataset.theme!=='dark';setTheme(dark);try{localStorage.setItem('hakimi-theme',dark?'dark':'light');}catch{}});

tabButtons.forEach(tabBtn=>tabBtn.addEventListener('click',()=>navigate(tabBtn.dataset.tab)));
$('workspace-switch').addEventListener('click',e=>{
  const btn=e.target.closest('[data-workspace-target]');
  if(btn)navigate(lastTab[btn.dataset.workspaceTarget],{focusContent:true});
});
$('app').addEventListener('click',e=>{
  const btn=e.target.closest('[data-open-tab]');
  if(btn)navigate(btn.dataset.openTab,{focusContent:true});
});
$('reload-modules').addEventListener('click',()=>navigate('modules'));
$('refresh-module').addEventListener('click',async()=>{
  if(!(await confirmDiscardTabChanges(tab,'刷新模块状态')))return;
  await navigate(tab);
});
document.querySelectorAll('[data-action]').forEach(b=>b.addEventListener('click',()=>busy(b,async()=>{notice('正在执行，请稍候…');const v=await api('actions/'+b.dataset.action,'POST');notice(v.note||v.status||'快照已刷新');await refreshTab();})));
$('refresh').addEventListener('click',e=>busy(e.currentTarget,()=>dashboard()));

trackDirty('colab-config-form','Colab 算力配置');

onLogout(()=>{
  closeNavigation();
  for(const remove of pluginPages.values())remove();
  lastTab.core='overview';lastTab.modules='modules';
  $('workspace-switch').hidden=true;
  text('connection-label','等待连接状态');
  $('connection-dot').className='status-dot';
  text('ws-heartbeat','—');text('ws-source','—');
  for(const id of ['module-catalog','system-catalog','qq-actions','bili-actions','minecraft-actions','weather-actions','module-heading-switch','colab-pills','colab-kpi-strip','colab-job-policies-table','colab-jobs-table'])$(id).replaceChildren();
  $('module-heading-switch').hidden=true;
  resetWeatherEditor();
  $('weather-editor').disabled=true;
  $('weather-reset').hidden=true;
  text('weather-current','正在读取地点…');
  for(const group of $('modules-nav').querySelectorAll('[data-module]'))group.hidden=false;
  for(const id of ['bili-module-backend','bili-module-autonomous'])text(id,'—');
  for(const id of ['colab-state','colab-job-types','colab-config-source'])text(id,'');
  for(const id of ['colab-terminate-sessions','colab-save','colab-reset'])$(id).disabled=true;
});
