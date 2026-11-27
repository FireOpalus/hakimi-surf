function providerBadge(key){
  if(key==='vertex'||key==='vertex_paid')return pill('Vertex AI','vertex');
  if(key==='gemini_free'||key==='free')return pill('Free Tier','free');
  return pill('Gemini API','paid');
}
function decisionTierBadge(tier){
  if(tier==='free'||tier==='gemini_free')return pill('Free Tier','free');
  if(tier==='paid_fallback'||tier==='fallback')return pill('Paid 兜底','warn');
  if(tier==='paid'||tier==='gemini_paid')return pill('Paid Tier','paid');
  return pill(tier||'—','muted');
}
function privacyBadge(level){
  if(level==='high')return pill('high · 高隐私','paid');
  if(level==='low')return pill('low · 低隐私','free');
  return pill(level||'—','muted');
}
function renderProviders(d){
  if(!d||!d.config)return;
  cachedProvidersOverview=d;
  const c=d.config,creds=d.credentials||{};
  $('prov-cred-pills').replaceChildren(
    creds.gemini_paid_configured?pill('Paid Gemini · 已配置 (.env)','paid'):pill('Paid Gemini · 未配置','err'),
    creds.gemini_free_configured?pill('Free Tier Gemini · 已配置 ('+(creds.gemini_free_source||'ok')+')','free'):pill('Free Tier Gemini · 未配置','muted'),
    creds.vertex_api_key_configured?pill('Vertex AI API Key · 已配置 ('+(creds.vertex_api_key_source||'ok')+')','vertex'):pill('Vertex AI API Key · 未配置','muted'),
    creds.gcp_service_account_configured?pill('GCP SA JSON · 已配置 ('+(creds.gcp_service_account_email||'已加密')+(creds.gcp_service_account_project?' · '+creds.gcp_service_account_project:'')+')','ok'):pill('GCP Service Account JSON · 未配置','muted')
  );
  text('prov-cred-status','凭据来源汇总：Free Key '+(creds.gemini_free_source||'none')+' · Vertex Key '+(creds.vertex_api_key_source||'none')+(creds.gcp_service_account_configured?' · SA 邮箱 '+(creds.gcp_service_account_email||'已隐藏')+'（项目 '+(creds.gcp_service_account_project||'未指定')+'）':' · 尚未上传 Service Account JSON'));
  $('prov-decision-tokens').value=c.decision_context_tokens??2500;
  $('prov-free-fallback').checked=c.free_tier_fallback_to_paid!==false;
  $('prov-vertex-enabled').checked=Boolean(c.vertex_enabled);
  $('prov-vertex-auth-mode').value=c.vertex_auth_mode||'api_key';
  $('prov-vertex-project').value=c.vertex_project||'';
  $('prov-vertex-location').value=c.vertex_location||'global';
  $('prov-vertex-protocol').value=c.vertex_protocol||'auto';
  $('prov-vertex-fallback').checked=c.vertex_fallback_to_gemini!==false;
  text('prov-policy-source','当前策略来源：'+(d.source==='admin'||d.source==='sqlite'?'后台 SQLite（重启保留）':'.env 初始值')+' · Vertex '+(c.vertex_enabled?'开启':'关闭'));
  for(const id of ['prov-save-creds','prov-save-policy','prov-reset-policy'])$(id).disabled=false;
  clearDirty('prov-policy-form');
}
function renderQqPrivacy(data){
  if(!token)return;
  $('prov-default-group-privacy').value=data.defaults.default_group_privacy;
  $('prov-default-private-privacy').value=data.defaults.default_private_privacy;
  $('qq-privacy-default-save').disabled=$('prov-add-session-policy').disabled=false;
  clearDirty('qq-privacy-defaults');
  table('prov-sessions-table',['会话','群号或用户 ID','当前等级','隐私设置'],data.sessions.map(item=>{
    const choice=el('select',undefined,'table-select');
    choice.setAttribute('aria-label',(item.key||item.target_id)+' 隐私等级');
    for(const [value,label] of [['default','跟随默认'],['high','高隐私'],['low','低隐私']]){
      const opt=el('option',label);opt.value=value;choice.append(opt);
    }
    choice.value=item.override_privacy;
    choice.addEventListener('change',()=>busy(choice,async()=>{
      try{
        renderQqPrivacy(await api('qq/privacy','POST',{kind:item.kind,platform:item.platform,target_id:item.target_id,privacy:choice.value}));
        notice('会话隐私等级已保存');
      }catch(error){choice.value=item.override_privacy;throw error;}
    }));
    return [item.is_owner?'Owner 私聊':item.kind==='group'?'群聊':'私聊',item.key,privacyBadge(item.effective_privacy),choice];
  }));
}
async function qqPrivacy(){renderQqPrivacy(await api('qq/privacy'));}
$('qq-privacy-defaults').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('qq-privacy-default-save'),async()=>{
    renderQqPrivacy(await api('qq/privacy','PUT',{
      default_group_privacy:$('prov-default-group-privacy').value,
      default_private_privacy:$('prov-default-private-privacy').value
    }));
    notice('QQ 默认隐私等级已保存');
  });
});
async function providers(){
  const d=await api('global/providers');
  if(!token)return;
  renderProviders(d);
}
$('refresh-providers').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('api-routing','刷新路由与凭据')))return;
  await providers();
}));
$('prov-sa-file').addEventListener('change',e=>busy($('prov-save-creds'),()=>loadJsonFileInto(e.target,$('prov-sa-json'))));
function syncCredentialClearState(){
  const clearing=['prov-clear-free','prov-clear-vertex','prov-clear-sa'].some(id=>$(id).checked);
  $('prov-save-creds').className=clearing?'danger':'primary';
  $('prov-save-creds').textContent=clearing?'保存并清除所选凭据':'加密保存凭据';
}
for(const id of ['prov-clear-free','prov-clear-vertex','prov-clear-sa'])$(id).addEventListener('change',syncCredentialClearState);
$('prov-cred-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('prov-save-creds'),async()=>{
    const payload={
      gemini_free_api_key:$('prov-free-key').value.trim(),
      vertex_api_key:$('prov-vertex-key').value.trim(),
      gcp_service_account_json:$('prov-sa-json').value.trim(),
      clear_gemini_free_api_key:$('prov-clear-free').checked,
      clear_vertex_api_key:$('prov-clear-vertex').checked,
      clear_gcp_service_account_json:$('prov-clear-sa').checked,
      clear:false
    };
    if(!payload.gemini_free_api_key&&!payload.vertex_api_key&&!payload.gcp_service_account_json&&!payload.clear_gemini_free_api_key&&!payload.clear_vertex_api_key&&!payload.clear_gcp_service_account_json){
      throw Error('请先输入需要更新的凭据，或勾选要清除的项目');
    }
    const clearing=[
      payload.clear_gemini_free_api_key&&'Free Tier Gemini Key',
      payload.clear_vertex_api_key&&'Vertex AI API Key',
      payload.clear_gcp_service_account_json&&'GCP Service Account JSON'
    ].filter(Boolean);
    if(clearing.length&&!(await confirmDialog('将永久清除后台保存的：'+clearing.join('、')+'。相关通道会立即回退或停用。',{title:'清除已保存的凭据？',danger:true,confirmLabel:'确认清除并保存'})))return;
    if(payload.gcp_service_account_json){
      try{const parsed=JSON.parse(payload.gcp_service_account_json);if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw 0;}catch{throw Error('Service Account JSON 格式无效，请检查内容');}
    }
    const next=await api('global/providers/credentials','POST',payload);
    $('prov-free-key').value='';$('prov-vertex-key').value='';$('prov-sa-json').value='';$('prov-sa-file').value='';
    $('prov-clear-free').checked=false;$('prov-clear-vertex').checked=false;$('prov-clear-sa').checked=false;
    syncCredentialClearState();
    clearDirty('prov-cred-form');
    renderProviders(next);
    notice('API 凭据与服务账号已加密更新');
  });
});
$('prov-policy-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('prov-save-policy'),async()=>{
    const tokens=Number($('prov-decision-tokens').value);
    if(!Number.isInteger(tokens)||tokens<600||tokens>4200)throw Error('Decisions 上下文 Token 上限须为 600～4200 的整数');
    const payload={
      decision_context_tokens:tokens,
      free_tier_fallback_to_paid:$('prov-free-fallback').checked,
      vertex_enabled:$('prov-vertex-enabled').checked,
      vertex_auth_mode:$('prov-vertex-auth-mode').value,
      vertex_project:$('prov-vertex-project').value.trim(),
      vertex_location:$('prov-vertex-location').value.trim()||'global',
      vertex_protocol:$('prov-vertex-protocol').value,
      vertex_fallback_to_gemini:$('prov-vertex-fallback').checked
    };
    const next=await api('global/providers','PUT',payload);
    renderProviders(next);
    notice('全局模型通道已保存并立即生效');
  });
});
$('prov-reset-policy').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复 .env 中的全局模型通道设置？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('global/providers','DELETE');
  renderProviders(next);
  notice('已恢复 .env 默认路由策略');
}));
$('prov-custom-session-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('prov-add-session-policy'),async()=>{
    const kind=$('prov-custom-kind').value,id=$('prov-custom-id').value.trim();
    const official=id.startsWith('qq:'),target=official?id.slice(3):id;
    if(official?!/^[A-Za-z0-9_-]{1,128}$/.test(target):!/^[1-9][0-9]{0,19}$/.test(target)){
      throw Error('请填写 QQ 号码，官方机器人的会话写作 qq:OpenID');
    }
    renderQqPrivacy(await api('qq/privacy','POST',{kind,platform:official?'qq':'onebot',target_id:target,privacy:$('prov-custom-privacy').value}));
    $('prov-custom-id').value='';
    notice('会话隐私等级已保存');
  });
});

const thinkingLevels={auto:'auto · 自动分档',low:'low · 轻量',medium:'medium · 均衡',high:'high · 深入',xhigh:'xhigh · 最高'};
let thinkingReady=false,thinkingDirty=false,scenePolicy=null;
const sceneEdits=new Map();
function thinkingChanged(){thinkingDirty=true;text('thinking-edit-state','有待保存的修改');markDirty('thinking-policy');}
function displayModelSettings(d){
  if(!token)return;
  scenePolicy=d;sceneEdits.clear();
  const model=$('primary-model');
  model.replaceChildren(...Object.entries(d.models).map(([value,label])=>selectOption(value,label)));
  model.value=d.primary_model;
  renderSceneGroups();
  text('thinking-source',d.source==='sqlite'?'已保存至 SQLite':'使用默认设置');
  text('thinking-edit-state','');thinkingReady=true;thinkingDirty=false;
  clearDirty('thinking-policy');
  for(const id of ['primary-model','thinking-all','apply-thinking-all','save-thinking','reset-thinking'])$(id).disabled=false;
}
function renderSceneGroups(){
  const mapping=scenePolicy.model_levels[$('primary-model').value];
  for(const option of $('thinking-all').options)option.disabled=mapping[option.value]!==option.value;
  if($('thinking-all').selectedOptions[0].disabled)$('thinking-all').value='medium';
  $('thinking-fields').replaceChildren(...scenePolicy.groups.map(group=>{
    const box=el('fieldset'),legend=el('legend',group.label),fields=el('div',undefined,'scene-fields');
    for(const scene of group.scenes){
      const field=el('label',scene.label),select=el('select');
      select.id='thinking-'+scene.key;select.dataset.scene=scene.key;
      const levels=Array.from(new Set(scene.levels.map(value=>mapping[value])));
      select.replaceChildren(...levels.map(value=>selectOption(value,(thinkingLevels[value]||value)+(value===mapping[scene.default]?'（默认）':''))));
      select.value=mapping[sceneEdits.get(scene.key)??scene.value];select.required=true;field.append(select);
      fields.append(field);
    }
    box.append(legend,fields);return box;
  }));
}
function resetThinkingDraft(){
  sceneEdits.clear();
  thinkingDirty=false;
  text('thinking-edit-state','');
  if(scenePolicy){
    $('primary-model').value=scenePolicy.primary_model;
    renderSceneGroups();
  }
}
async function thinking(){displayModelSettings(await api('models'));}
$('primary-model').addEventListener('change',()=>{renderSceneGroups();thinkingChanged();});
$('thinking-fields').addEventListener('change',e=>{sceneEdits.set(e.target.dataset.scene,e.target.value);thinkingChanged();});
$('refresh-thinking').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('thinking','刷新模型与思考设置')))return;
  resetThinkingDraft();
  await Promise.all([thinking(),generationCaps()]);
}));
$('apply-thinking-all').addEventListener('click',()=>{
  const level=$('thinking-all').value;
  for(const select of $('thinking-fields').querySelectorAll('select'))if(Array.from(select.options).some(o=>o.value===level)){select.value=level;sceneEdits.set(select.dataset.scene,level);}
  thinkingChanged();
});
$('thinking-policy').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{
  if(!thinkingReady)return;
  const map=scenePolicy.model_levels[$('primary-model').value];
  const levels=Object.fromEntries(Array.from(sceneEdits,([key,value])=>[key,map[value]]));
  displayModelSettings(await api('models','PUT',{primary_model:$('primary-model').value,levels}));
  notice('已保存，下一次生成使用新设置');
});});
$('reset-thinking').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复主模型及已安装场景的默认设置？',{danger:true,confirmLabel:'恢复默认'})))return;
  displayModelSettings(await api('models','DELETE'));notice('已恢复模型与思考默认设置');
}));

let genCapFields={};
async function generationCaps(){
  const d=await api('generation-caps');if(!token)return;
  genCapFields=d.fields;
  $('gen-caps-fields').replaceChildren(...Object.entries(genCapFields).map(([key,field])=>{
    const label=el('label',field.title+'（'+field.minimum+'–'+field.maximum+'）'),input=el('input');
    input.id='gen-cap-'+key;input.type='number';input.min=field.minimum;input.max=field.maximum;input.step='1';input.required=true;input.value=d.config[key];
    label.append(input);return label;
  }));
  text('gen-caps-source',d.source==='admin'?'当前配置来自 SQLite 后台保存，重启后保留':'当前配置来自本次启动读取的 .env');
  for(const id of ['save-gen-caps','reset-gen-caps'])$(id).disabled=false;
  clearDirty('gen-caps-form');
}
$('refresh-gen-caps').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('thinking','刷新输出上限')))return;
  resetThinkingDraft();
  await Promise.all([thinking(),generationCaps()]);
}));
$('gen-caps-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-gen-caps'),async()=>{const payload={};for(const key of Object.keys(genCapFields))payload[key]=Number($('gen-cap-'+key).value);await api('generation-caps','PUT',payload);notice('输出上限已保存并立即生效');await generationCaps();});});
$('reset-gen-caps').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复本次启动读取的 .env 输出上限？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('generation-caps','DELETE');await generationCaps();notice('已恢复 .env 输出上限');
}));

const thresholdInputs={injection_threshold:'filter-injection',unsafe_threshold:'filter-unsafe',reply_threshold:'filter-reply',owner_injection_threshold:'filter-owner-injection',owner_unsafe_threshold:'filter-owner-unsafe'};
const debounceFields=[['group_debounce_seconds','普通群聊等待'],['mention_debounce_seconds','私聊 / 被 @ / 引用 Bot 等待'],['ordinary_incomplete_seconds','普通群聊未说完时等待'],['mention_incomplete_seconds','私聊 / 被 @ 未说完时等待'],['group_debounce_max_seconds','同批次最长等待']];
const rateLimitFields=[['user_requests_per_minute','单用户每分钟消息上限（1–100）',1,100,'1',true],['global_requests_per_minute','全局每分钟消息上限（1–1000）',1,1000,'1',true],['queue_capacity','待处理队列容量（1–10000）',1,10000,'1',true],['send_delay_min','发送最短延迟（0–5 秒）',0,5,'0.1',false],['send_delay_max','发送最长延迟（0–10 秒）',0,10,'0.1',false]];

async function replySettings(){
  const [d,w,rl,il,ct]=await Promise.all([api('debounce'),api('web-search'),api('rate-limits'),api('image-limits'),api('content-tools')]);
  if(!token)return;
  $('debounce-fields').replaceChildren(...debounceFields.map(([key,label])=>{const f=el('label',label),n=el('input');n.id='debounce-'+key;n.type='number';n.min='0';n.max='30';n.step='0.1';n.required=true;n.value=d.config[key];f.append(n);return f;}));
  text('debounce-source','当前来源：'+(d.source==='sqlite'?'后台 SQLite（重启保留）':'.env 初始值'));
  $('rate-limits-fields').replaceChildren(...rateLimitFields.map(([key,label,min,max,step])=>{const f=el('label',label),n=el('input');n.id='rate-limit-'+key;n.type='number';n.min=String(min);n.max=String(max);n.step=step;n.required=true;n.value=rl.config[key];f.append(n);return f;}));
  text('rate-limits-source','当前来源：'+(rl.source==='admin'?'后台 SQLite（重启保留）':'.env 初始值'));
  $('img-max-mib').value=Number((il.config.image_max_bytes/1048576).toFixed(2));
  $('img-max-mp').value=Number((il.config.image_max_pixels/1e6).toFixed(2));
  $('img-std-edge').value=il.config.image_standard_max_edge;
  $('img-tall-edge').value=il.config.image_tall_max_edge;
  text('image-limits-source','当前来源：'+(il.source==='admin'?'后台 SQLite（重启保留）':'.env 初始值'));
  $('file-read-enabled').checked=Boolean(ct.config.file_read_enabled);
  $('file-max-kib').value=Math.round(ct.config.file_max_bytes/1024);
  $('file-daily-limit').value=ct.config.file_daily_limit_per_user;
  $('text-card-mode').value=ct.config.text_card_mode;
  text('content-tools-source','当前来源：'+(ct.source==='admin'?'后台 SQLite（重启保留）':'.env 初始值'));
  $('search-enabled').checked=w.config.enabled;$('search-limit').value=w.config.daily_limit;$('search-monthly-limit').value=w.config.monthly_google_limit??5000;
  const wm=w.monthly||{},ds=w.deepseek||{};
  text('search-status',(w.config.enabled?'已开启':'已关闭')+' · 来源 '+w.source+' · 今日 '+w.today.attempts+'/'+w.config.daily_limit+' · 本月 Google '+(wm.google_queries??0)+'/'+(wm.google_limit??5000)+' 次 · 本月 DeepSeek '+(wm.deepseek_attempts??0)+' 次 · 当前引擎 '+(wm.active_provider==='deepseek'?'DeepSeek':'Google Search')+'（联网不计费）');
  $('deepseek-base-url').value=ds.base_url||'https://api.deepseek.com';$('deepseek-model').value=ds.model||'deepseek-flash';
  text('deepseek-status','API Key：'+(ds.api_key_configured?'已加密保存（隐藏 · 来源 '+ds.source+'）':'未配置')+' · 端点 '+ds.base_url+' · 备选模型 '+ds.model);
  for(const id of ['save-debounce','reset-debounce','save-rate-limits','reset-rate-limits','save-image-limits','reset-image-limits','save-content-tools','reset-content-tools','save-search','reset-search','save-deepseek','clear-deepseek'])$(id).disabled=false;
  clearTabDirty('reply-settings');
}
function previewWakeWindow(){
  const sleeping=$('rest-enabled').checked;
  $('rest-start').disabled=$('rest-end').disabled=!sleeping;
  const wakeStr=sleeping?$('rest-end').value+'–'+$('rest-start').value:'全天';
  const quietNote=sleeping?' · 睡眠时段同步免打扰':'';
  text('wake-window','清醒活动时段 · '+wakeStr+($('rest-day-enabled').checked?'，小憩时暂停':'')+quietNote);
}
for(const id of ['rest-enabled','rest-day-enabled','rest-start','rest-end'])$(id).addEventListener('input',previewWakeWindow);
for(const id of ['refresh-reply-settings','refresh-rate-limits','refresh-image-limits','refresh-content-tools']){
  $(id).addEventListener('click',e=>busy(e.target,async()=>{
    if(!(await confirmDiscardTabChanges('reply-settings','刷新配置')))return;
    await replySettings();
  }));
}
$('debounce-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-debounce'),async()=>{const c={};for(const [key] of debounceFields)c[key]=Number($('debounce-'+key).value);await api('debounce','PUT',c);notice('防抖时间已热更新，未开始的等待批次已调整');await replySettings();});});
$('rate-limits-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-rate-limits'),async()=>{const c={};for(const [key,label,min,max,_,isInt] of rateLimitFields){const v=Number($('rate-limit-'+key).value);if(!Number.isFinite(v)||v<min||v>max||(isInt&&!Number.isInteger(v)))throw Error(label+' 参数超出有效范围');c[key]=v;}if(c.send_delay_max<c.send_delay_min)throw Error('发送最长延迟不得小于最短延迟');await api('rate-limits','PUT',c);notice('请求限流与发送延迟已保存并立即生效');await replySettings();});});
$('image-limits-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-image-limits'),async()=>{const mib=Number($('img-max-mib').value),mp=Number($('img-max-mp').value),std=Number($('img-std-edge').value),tall=Number($('img-tall-edge').value);if(!Number.isFinite(mib)||mib<1||mib>20)throw Error('原图大小上限须为 1–20 MiB');if(!Number.isFinite(mp)||mp<1||mp>80)throw Error('原图总像素上限须为 1–80 MP');if(!Number.isInteger(std)||std<512||std>2560)throw Error('常规图长边须为 512–2560 的整数');if(!Number.isInteger(tall)||tall<1024||tall>4096||tall<std)throw Error('长截图长边须为 1024–4096 的整数且不得小于常规图长边');const bytes=Math.min(20971520,Math.max(1048576,Math.round(mib*1048576))),pixels=Math.min(80000000,Math.max(65536,Math.round(mp*1000000)));await api('image-limits','PUT',{image_max_bytes:bytes,image_max_pixels:pixels,image_standard_max_edge:std,image_tall_max_edge:tall});notice('图片尺寸与压缩设置已保存并立即生效');await replySettings();});});
$('content-tools-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-content-tools'),async()=>{const kib=Number($('file-max-kib').value),daily=Number($('file-daily-limit').value);if(!Number.isInteger(kib)||kib<16||kib>256)throw Error('单文件大小上限须为 16–256 KiB 的整数');if(!Number.isInteger(daily)||daily<0||daily>100)throw Error('单用户每日上限须为 0–100 的整数');await api('content-tools','PUT',{file_read_enabled:$('file-read-enabled').checked,file_max_bytes:kib*1024,file_daily_limit_per_user:daily,text_card_mode:$('text-card-mode').value});notice('文件阅读与卡片设置已保存并立即生效');await replySettings();});});
$('search-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-search'),async()=>{const daily=Number($('search-limit').value),monthly=Number($('search-monthly-limit').value);if(!Number.isInteger(daily)||daily<0||daily>500)throw Error('每日搜索上限须为 0–500 的整数');if(!Number.isInteger(monthly)||monthly<0||monthly>100000)throw Error('每月 Google 免费上限须为 0–100000 的整数');await api('web-search','PUT',{enabled:$('search-enabled').checked,daily_limit:daily,monthly_google_limit:monthly});notice('联网配置已保存并立即生效');await replySettings();});});
$('deepseek-form').addEventListener('submit',e=>{e.preventDefault();busy($('save-deepseek'),async()=>{await api('web-search/deepseek','POST',{api_key:$('deepseek-key').value.trim(),base_url:$('deepseek-base-url').value.trim(),model:$('deepseek-model').value.trim()});$('deepseek-key').value='';notice('DeepSeek 备用搜索配置已加密保存');await replySettings();});});
$('clear-deepseek').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('清除已保存的 DeepSeek 密钥与端点覆盖？',{danger:true,confirmLabel:'清除密钥'})))return;
  await api('web-search/deepseek','POST',{clear:true});$('deepseek-key').value='';notice('已清除 DeepSeek 后台密钥');await replySettings();
}));
for(const [id,path] of [['reset-debounce','debounce'],['reset-rate-limits','rate-limits'],['reset-image-limits','image-limits'],['reset-content-tools','content-tools'],['reset-search','web-search']]){
  $(id).addEventListener('click',e=>busy(e.target,async()=>{
    if(!(await confirmDialog('恢复本次启动读取的 .env 初始值？',{danger:true,confirmLabel:'恢复默认'})))return;
    await api(path,'DELETE');notice('已恢复 .env 初始值');await replySettings();
  }));
}

function auditScore(value,direct=false){const box=el('div');if(value?.skipped){box.append(el('span',direct?'直接回应，跳过相关性':'未执行相关性判定'));return box;}if(!value||value.noul===null||value.noul===undefined){box.append(el('span','未知'));return box;}const bar=el('progress');bar.max=1;bar.value=value.noul;bar.setAttribute('aria-label','判定概率 '+value.noul);box.append(bar,el('small','p='+Number(value.noul).toFixed(3)+' · c='+(value.confidence===null||value.confidence===undefined?'未知':Number(value.confidence).toFixed(3))));return box;}
const rejectDetails={budget_exhausted_user_daily:'budget_exhausted · 单用户日上限',budget_exhausted_daily:'budget_exhausted · 全局日上限',budget_exhausted_monthly:'budget_exhausted · 全局月上限',budget_exhausted_patrol_daily:'budget_exhausted · 巡逻日上限',budget_exhausted_memory_daily:'budget_exhausted · 记忆日上限',budget_exhausted_proactive_daily:'budget_exhausted · 主动生成日上限',budget_exhausted_admin_daily:'budget_exhausted · 沙盒日上限',budget_exhausted_search_daily:'budget_exhausted · 联网日上限',budget_exhausted_total:'budget_exhausted · 累计总上限',budget_exhausted_gcp_credit_exhausted:'gcp_credit_exhausted · GCP 赠金已耗尽'};
const modelFailureLabels={
  stage:{decision_message:'来信安全检查',decision_source:'来源安全检查',decision_output:'回复发送前检查',generation:'主模型生成',search:'联网搜索'},
  cause:{deadline:'达到本地调用时限',connect_timeout:'建立连接超时',read_timeout:'读取上游响应超时',write_timeout:'发送请求超时',pool_timeout:'等待连接池超时',sdk_timeout:'SDK 超时',timeout:'请求超时',http:'上游返回 HTTP 错误',dns:'域名解析失败',tls:'TLS 连接失败',connection:'连接失败',transport:'网络传输异常',stream_error:'上游流返回错误',circuit_open:'本地熔断冷却',unknown:'未识别的上游异常'},
  provider:{gemini_free:'Gemini Developer API（免费）',gemini_paid:'Gemini Developer API（付费）',vertex_paid:'Vertex AI'},
  serviceTier:{standard:'Standard PayGo',priority:'Priority PayGo'},
  lane:{decision:'gemini-decision-1',hydrogen:'Hydrogen',neon:'Neon',carbon:'Carbon',memory_flex:'Hydrogen Flex',neon_flex:'Neon Flex',carbon_flex:'Carbon Flex'},
  stream:{waiting:'等待上游首个流事件',started:'已收流事件，尚无正文',partial_output:'已收到部分正文'},
  fallback:{succeeded:'回退成功',failed:'回退失败',cancelled:'回退已取消',not_dispatched:'备用请求未发出',disabled:'回退已关闭',ineligible:'此类错误跳过回退',partial_output:'已有部分正文，跳过回退'}
};
function diagnosticCell(row){
  const attempts=row.provider_diagnostics||[];
  if(!attempts.length){
    const old=row.legacy_ledger_call;
    if(old){
      const box=el('details');box.append(el('summary','历史账本 · '+modelFailureLabels.lane[old.model_lane]));
      box.append(el('p',dateTime(old.started_at)+' · '+modelFailureLabels.provider[old.provider]+' · 同一任务最后一次模型调用，账本未记录用量。'));
      box.append(el('small','当时未记录异常类型、耗时和 HTTP 状态码。'));
      return box;
    }
    const known=[row.reason,row.code,row.execution_detail].some(v=>/^(gemini_unavailable|gemini_circuit_open|stream_provider_error|generation_incomplete)/.test(v||''));
    return known?'旧记录未保存调用详情':'—';
  }
  const tags=modelFailureLabels, box=el('details'),last=attempts[attempts.length-1];
  const capacity=last.model_lane==='memory_flex'&&last.http_status===503;
  const stages=[...new Set(attempts.map(a=>tags.stage[a.stage]).filter(Boolean))];
  const summaryText=capacity?'Flex 容量繁忙（HTTP 503）':(stages.length>1?stages.join(' + ')+'（'+attempts.length+' 条）':tags.stage[last.stage]+' · '+tags.cause[last.cause]+(attempts.length>1?'（'+attempts.length+' 次）':''));
  box.append(el('summary',summaryText));
  const seconds=ms=>(ms/1000).toLocaleString('zh-CN',{maximumFractionDigits:3})+' 秒';
  for(const a of attempts){
    const parts=[tags.stage[a.stage],tags.provider[a.provider],a.service_tier&&tags.serviceTier[a.service_tier],tags.lane[a.model_lane],tags.cause[a.cause]];
    if(a.http_status)parts.push('HTTP '+a.http_status);
    if(a.upstream_code)parts.push(a.upstream_code);
    if(a.exception_type)parts.push(a.exception_type);
    if(a.cause==='circuit_open')parts.push('本次未发出请求','剩余冷却 '+seconds(a.cooldown_ms));
    else{
      if(a.elapsed_ms!=null)parts.push('耗时 '+seconds(a.elapsed_ms));
      if(a.timeout_ms!=null)parts.push('时限 '+seconds(a.timeout_ms));
      if(a.stream_phase)parts.push(tags.stream[a.stream_phase]);
    }
    if(a.fallback)parts.push(tags.fallback[a.fallback]);
    if(a.next_retry_at)parts.push('第 '+a.retry_number+' 次重试安排在 '+dateTime(a.next_retry_at));
    box.append(el('p',parts.filter(Boolean).join(' · ')));
  }
  return box;
}
function auditOutcome(r){
  const s=r.execution_status, raw=r.execution_detail||'', incident=r.provider_incident, ws=r.web_search;
  const details=[], reason=rejectDetails[raw]||raw;
  const lookupScore=r.scores?.need_external_lookup?.noul;
  if(ws){
    const trig={explicit:'指令',gate:'门卫',model:'模型主动'}[ws.trigger]||'';
    const st={grounded:'已检索',unavailable:'检索失败',disabled:'检索已关',skipped_query:'跳过检索'}[ws.status]||ws.status;
    const prov={google:'Google',deepseek:'DeepSeek',remote:'远程插件',searxng:'SearXNG',tavily:'Tavily',brave:'Brave',duckduckgo:'DuckDuckGo'}[ws.provider]||ws.provider||'';
    const extra=[];
    if(trig)extra.push(trig);
    if(prov)extra.push(ws.fallback?'兜底 '+prov:prov);
    if(ws.retried)extra.push(ws.status==='grounded'&&!ws.fallback?'重试成功':'已重试');
    if(ws.reason&&ws.status!=='grounded')extra.push(ws.reason);
    if(lookupScore!==null&&lookupScore!==undefined)extra.push('p='+Number(lookupScore).toFixed(3));
    details.push('联网 '+st+(extra.length?'（'+extra.join(' / ')+'）':''));
  }else if(lookupScore!==null&&lookupScore!==undefined&&Number(lookupScore)>=0.35){
    details.push('联网分 p='+Number(lookupScore).toFixed(3));
  }
  if(incident){
    const provider=incident.provider==='vertex_paid'?'Vertex AI':'Gemini Developer API';
    const code=incident.code==='unknown'?'上游未提供可识别错误码':incident.code;
    const status=incident.http_status&&String(incident.http_status)!==code?incident.http_status+' / ':'';
    details.push(provider+' · '+status+code);
    const target=incident.fallback_target==='vertex_priority'?'Vertex Priority PayGo':'Gemini Developer API';
    const fallbackLabels={
      succeeded:target+' 回退成功',failed:target+' 回退失败',
      cancelled:'回退已取消',not_dispatched:'备用请求未发出',partial_output:'已产生部分正文，跳过回退',
      disabled:'回退开关已关闭',ineligible:'此错误不触发跨服务回退'
    };
    if(incident.fallback){
      const cause=incident.fallback_code||rejectDetails[incident.fallback_detail]||incident.fallback_detail;
      details.push((fallbackLabels[incident.fallback]||incident.fallback)+(cause?'（'+cause+'）':''));
    }
  }
  const outcomes={sent:'已发送',abstained:'主模型弃权',cancelled:'已取消',blocked:'发送拦截',
    expired:'发送过期',uncertain:'发送待确认',interrupted:'批次合并重排',failed:'执行异常',
    queued:'待发送',sending:'发送中',processing:'处理中',no_reply:'未生成回复'};
  if(s==='rejected'&&raw==='stream_provider_error'){
    details.push(incident?'模型流中断':'模型流中断（历史记录未保存上游错误码）');
  }else if(s){
    const outcome=s==='rejected'?(raw.startsWith('budget_')?'预算限制':raw==='content_blocked'?'内容安全拦截':'处理未完成'):(outcomes[s]||s);
    details.push(outcome+(reason?'（'+reason+'）':''));
  }
  return details.length?' · '+details.join(' · '):'';
}
async function decisionAudit(){
  const rows=await api('decision-audit');if(!token)return;
  table('decision-audit',['时间 / 场景','隐私 / 门卫路由','脱敏摘要','注入 p / c','违规 p / c','回应 p / c','结果 / 原因','调用详情'],rows.map(r=>{
    const routeBox=el('div',undefined,'stack-xs');
    if(r.privacy_level)routeBox.append(privacyBadge(r.privacy_level));
    if(r.decision_tier)routeBox.append(decisionTierBadge(r.decision_tier));
    if(!r.privacy_level&&!r.decision_tier)routeBox.append(el('span','—','muted'));
    return[
      dateTime(r.at)+' · '+r.source+' · '+r.surface+(r.group_id?' · 群 '+r.group_id:'')+(r.user_id_mask?' · '+r.user_id_mask:''),
      routeBox,
      r.excerpt,
      auditScore(r.scores?.is_prompt_injection),
      auditScore(r.scores?.is_unsafe_or_spam),
      auditScore(r.scores?.should_reply,r.reply_bypassed),
      (r.result==='blocked'&&/^gemini_(?:unavailable|circuit_open)/.test(r.reason)?'检查未完成':r.result)+' · '+r.reason+(r.reply_bypassed?' · 明确请求，跳过插话门槛':'')+(r.attention_reason?' · '+r.attention_reason:'')+(r.thinking_level?' · 思考 '+(r.model_lane==='carbon'?'Carbon/':'')+r.thinking_level:'')+auditOutcome(r),diagnosticCell(r)
    ];
  }));
}
async function filters(){
  const d=await api('filter');if(!token)return;
  for(const [key,id] of Object.entries(thresholdInputs))$(id).value=d.config[key];
  $('filter-bot-reply').value=d.bot_reply_threshold??0.85;
  text('filter-source','配置来自 '+d.source+' · 外部来源 / 输出阈值：注入 '+d.source_thresholds.injection+'，违规 '+d.source_thresholds.unsafe);
  clearDirty('filter-policy');
  await decisionAudit();
}
$('filter-policy').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{const value={};for(const [key,id] of Object.entries(thresholdInputs)){value[key]=Number($(id).value);if(!Number.isFinite(value[key])||value[key]<.01||value[key]>.99)throw Error('阈值须为 0.01～0.99');}const botReply=parseBotThreshold($('filter-bot-reply').value);await Promise.all([api('filter','PUT',value),api('bots/thresholds','PUT',{bot_reply_threshold:botReply})]);notice('来信阈值与 Bot 回复门槛已保存，群活跃度保持不变');await filters();});});
$('reset-filter').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复 .env 来信阈值并移除后台群活跃度预设？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('filter','DELETE');notice('已恢复环境阈值与默认群活跃度');await filters();
}));
$('refresh-filter').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('filter','刷新配置与审计')))return;
  await filters();
}));

$('simulate').addEventListener('click',e=>busy(e.target,async()=>{
  text('preview-text','正在筛查与生成…');
  const v=await api('sandbox','POST',{identity:$('sim-identity').value,scene:$('sim-scene').value,group_id:$('sim-group').value,text:$('sim-text').value});
  $('scores').replaceChildren();
  for(const k of ['is_prompt_injection','is_unsafe_or_spam','should_reply','need_memory_recall','need_external_lookup']){
    const a=v.decisions.answers?.[k];
    const n=el('div',undefined,'score');n.append(el('p',k));
    if(a?.skipped){n.append(el('small',v.decisions.reply_bypassed?'直接回应，跳过相关性':'未执行相关性判定'));}
    else if(a?.noul===null||a?.noul===undefined){n.append(el('small','未调用 / 未知'));}
    else{n.append(auditScore(a));}
    $('scores').append(n);
  }
  text('decision-result',(v.decisions.final_result||v.decisions.result||'未调用')+(v.decisions.reason?' · '+v.decisions.reason:''));
  text('decision-raw',JSON.stringify(v.decisions,null,2));
  text('preview-meta',v.preview.persona+' 人设 · '+(v.preview.model||'')+' · thinking_level='+v.preview.thinking_level+' · 群策略 '+v.group_policy);
  text('preview-text',v.preview.text||'本次没有生成回复');
}));

trackDirty('prov-cred-form','API 凭据');
trackDirty('prov-policy-form','全局模型通道');
trackDirty('qq-privacy-defaults','QQ 默认隐私等级');
trackDirty('thinking-policy','模型与思考设置');
if(typeof registerDiscardHandler==='function')registerDiscardHandler('thinking-policy',resetThinkingDraft);
trackDirty('gen-caps-form','输出 Token 上限');
trackDirty('debounce-form','回复防抖时间');
trackDirty('rate-limits-form','请求限流与延迟');
trackDirty('image-limits-form','图片尺寸限制');
trackDirty('content-tools-form','文件阅读与卡片');
trackDirty('search-form','联网配置');
trackDirty('deepseek-form','DeepSeek 配置');
trackDirty('filter-policy','Decisions 阈值');

onLogout(()=>{
  thinkingReady=false;thinkingDirty=false;scenePolicy=null;sceneEdits.clear();genCapFields={};
  for(const id of ['prov-cred-pills','prov-sessions-table','thinking-fields','primary-model','gen-caps-fields','debounce-fields','rate-limits-fields','decision-audit','scores'])$(id).replaceChildren();
  for(const id of ['prov-cred-status','prov-policy-source','thinking-source','thinking-edit-state','gen-caps-source','debounce-source','rate-limits-source','image-limits-source','content-tools-source','search-status','deepseek-status','filter-source','decision-raw','preview-meta'])text(id,'');
  text('decision-result','尚未运行');text('preview-text','尚未运行');
  for(const id of ['prov-save-creds','prov-save-policy','prov-reset-policy','prov-add-session-policy','qq-privacy-default-save','primary-model','thinking-all','apply-thinking-all','save-thinking','reset-thinking','save-gen-caps','reset-gen-caps','save-debounce','reset-debounce','save-rate-limits','reset-rate-limits','save-image-limits','reset-image-limits','save-content-tools','reset-content-tools','save-search','reset-search','save-deepseek','clear-deepseek'])$(id).disabled=true;
});
