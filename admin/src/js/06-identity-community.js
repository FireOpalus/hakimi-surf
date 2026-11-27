let identityState=null,pluginBusState=null,currentPluginToken='',currentPluginTokenId='';
const DEFAULT_PLUGIN_SCOPES=['actions','sensors','thinking_scenes','chat','media','owner_notify','node_bridge'];

const idAccSourceLabels={auto:'自动建档',admin:'后台手工绑定',bind_code:'/bind 验证码',owner_config:'Owner 配置'};
function isSoleOwnerAccount(person,account){
  if(!person.is_owner)return false;
  const accounts=person.accounts||[];
  if(accounts.length<=1)return true;
  if(account.source==='owner_config'){
    return accounts.filter(a=>a.source==='owner_config').length<=1;
  }
  return false;
}
function formatPersonLv(p){
  const parts=[];
  if(p.lv!==undefined&&p.lv!==null&&p.lv!==''){
    const raw=String(p.lv);
    parts.push(raw.startsWith('Lv')?raw:('Lv.'+raw));
  }
  if(p.tier_label)parts.push(String(p.tier_label));
  if(p.score!==undefined&&p.score!==null&&p.score!=='')parts.push(String(p.score)+' 分');
  return parts.join(' · ');
}
function renderIdentityPersonsList(){
  const host=$('identity-persons-list');
  const persons=identityState?.persons||[];
  const q=($('identity-search').value||'').trim().toLowerCase();
  const filtered=q?persons.filter(p=>{
    const hay=[
      p.person_id,p.display_name,p.tier_label,
      ...(p.l2_tags||[]),
      ...((p.accounts||[]).flatMap(a=>[a.platform,a.user_id,a.nickname,a.source]))
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(q);
  }):persons;
  if(!filtered.length){
    host.replaceChildren(el('p',persons.length?'没有匹配的身份档案，请调整筛选条件。':'暂无统一身份档案。','empty-state'));
    return;
  }
  host.replaceChildren(...filtered.map(p=>{
    const card=el('article',undefined,'sub-card');
    const head=el('div',undefined,'sub-card-head');
    const titleRow=el('div',undefined,'row');
    titleRow.append(el('h3',p.display_name||p.person_id),el('code',p.person_id,'code-mono'));
    const pills=el('div',undefined,'row');
    if(p.is_owner)pills.append(pill('Owner · 受保护','paid'));
    else if(p.muted)pills.append(pill('小黑屋屏蔽中','err'));
    else pills.append(pill('正常互动','ok'));
    const lvDesc=formatPersonLv(p);
    if(lvDesc)pills.append(pill(lvDesc,p.is_owner?'paid':p.muted?'err':'vertex'));
    pills.append(pill('更新 '+dateTime(p.updated_at),'muted'));
    head.append(titleRow,pills);
    card.append(head);

    const tags=(p.l2_tags||[]).filter(Boolean);
    const tagRow=el('div',undefined,'pill-row');
    if(tags.length){
      tagRow.append(el('small','L2 标签：','muted'),...tags.map(t=>pill(t,'muted')));
    }else{
      tagRow.append(el('small','L2 标签：暂无','muted'));
    }
    card.append(tagRow);

    const accWrap=el('div',undefined,'table');
    const accList=p.accounts||[];
    table(accWrap,['平台','账号 ID','昵称','来源','绑定时间','操作'],accList.map(acc=>{
      const plat=acc.platform||'—';
      const platPill=pill(plat,plat==='qq'?'vertex':plat==='onebot'?'paid':'muted');
      const srcPill=pill(idAccSourceLabels[acc.source]||acc.source||'—',acc.source==='owner_config'?'paid':'muted');
      const op=isSoleOwnerAccount(p,acc)?pill('唯一 Owner 账号','paid'):button('解绑',async()=>{
        if(!(await confirmDialog('确认解绑 '+plat+':'+acc.user_id+'？解绑后该账号将拆分为独立 Person 档案。',{danger:true,confirmLabel:'解绑账号'})))return;
        const next=await api('identity/unlink','POST',{platform:plat,user_id:acc.user_id});
        await applyIdentityResponse(next);
        notice('已解绑 '+plat+':'+acc.user_id);
      },'secondary');
      return[platPill,el('span',acc.user_id||'—','code-mono'),acc.nickname||'—',srcPill,dateTime(acc.linked_at),op];
    }));
    card.append(accWrap);

    const form=el('form',undefined,'inline-edit-form');
    const grid=el('div',undefined,'grid');
    const nameLabel=el('label','显示名称（display_name）');
    const nameInput=el('input');nameInput.type='text';nameInput.maxLength=64;
    nameInput.value=p.display_name||'';nameInput.placeholder='用于后台展示的名称';
    nameLabel.append(nameInput);

    const tagsLabel=el('label','L2 画像标签（逗号或换行分隔，最多 8 个）');
    const tagsInput=el('textarea');tagsInput.rows=2;tagsInput.maxLength=400;
    tagsInput.value=tags.join(', ');tagsInput.placeholder='例如：喜欢猫猫, 深夜党（最多 8 条，单条 ≤ 32 字）';
    tagsLabel.append(tagsInput);
    grid.append(nameLabel,tagsLabel);
    form.append(grid);

    const footerRow=el('div',undefined,'row');
    const mutedInput=el('input');mutedInput.type='checkbox';mutedInput.checked=Boolean(p.muted);
    if(p.is_owner){
      mutedInput.disabled=true;
    }else{
      const muteLabel=el('label');
      muteLabel.append(mutedInput,' 跨平台小黑屋屏蔽（muted）');
      footerRow.append(muteLabel);
    }
    const saveBtn=el('button','保存档案','primary');saveBtn.type='submit';
    footerRow.append(saveBtn);
    form.append(footerRow);
    if(typeof enhanceLabels==='function')enhanceLabels(form);

    form.addEventListener('submit',e=>{
      e.preventDefault();
      busy(saveBtn,async()=>{
        const rawTags=tagsInput.value.trim();
        const parsed=rawTags?rawTags.split(/[\n,，]+/).map(s=>s.trim()).filter(Boolean):[];
        const deduped=[...new Set(parsed)];
        if(deduped.length>8)throw Error('L2 标签最多 8 个（当前 '+deduped.length+' 个）');
        for(const item of deduped){
          if(item.length>32)throw Error('单个 L2 标签不能超过 32 个字符');
        }
        const trimmedName=nameInput.value.trim();
        if(!trimmedName&&p.display_name)throw Error('显示名称不能为空');
        const payload={l2_tags:deduped};
        if(trimmedName)payload.display_name=trimmedName;
        if(!p.is_owner)payload.muted=mutedInput.checked;
        const next=await api('identity/persons/'+encodeURIComponent(p.person_id),'PUT',payload);
        await applyIdentityResponse(next);
        notice('已更新身份档案：'+(payload.display_name||p.person_id));
      });
    });
    const fold=el('details',undefined,'card-edit-fold');
    fold.append(el('summary','编辑档案 / 标签 / 屏蔽'),form);
    card.append(fold);
    return card;
  }));
}
function renderIdentityCenter(d){
  if(!d||!Array.isArray(d.persons))return;
  identityState=d;
  const persons=d.persons;
  const totalAccounts=persons.reduce((sum,p)=>sum+((p.accounts||[]).length),0);
  const mutedCount=persons.filter(p=>p.muted).length;
  const recapCount=Number(d.owner_recap_count??0);
  const bindCount=Number(d.pending_bind_codes??0);
  $('identity-kpis').replaceChildren(
    kpiCard('统一身份总数',String(persons.length),'已绑定账号 '+totalAccounts+' 个'+(mutedCount?' · 已屏蔽 '+mutedCount+' 人':''),'accent'),
    kpiCard('Owner 跨端摘要缓存',String(recapCount),'RAM 环形缓存（12h TTL，上限 6 条）',recapCount>0?'ok':''),
    kpiCard('活跃 /bind 绑定码',String(bindCount),'一次性 8 位验证码（300s TTL）',bindCount>0?'warn':'')
  );
  text('identity-state','共 '+persons.length+' 个统一身份 · '+totalAccounts+' 个关联账号 · Owner 跨端近期摘要 '+recapCount+' 条 · 待兑换 /bind 码 '+bindCount+' 个');
  $('id-person-options').replaceChildren(...persons.map(p=>{
    const opt=el('option');opt.value=p.person_id;
    opt.label=(p.display_name||p.person_id)+(p.is_owner?' [Owner]':'')+' ('+(p.accounts||[]).length+' 账号)';
    return opt;
  }));
  const seenPlatforms=new Set(['onebot','qq','discord','oopz','telegram']);
  for(const p of persons)for(const a of (p.accounts||[]))if(a.platform)seenPlatforms.add(a.platform);
  $('id-platform-options').replaceChildren(...[...seenPlatforms].map(plat=>{
    const opt=el('option');opt.value=plat;return opt;
  }));
  for(const id of ['id-link-submit','id-merge-submit'])$(id).disabled=false;
  renderIdentityPersonsList();
}
async function applyIdentityResponse(next){
  if(next&&Array.isArray(next.persons))renderIdentityCenter(next);
  else await identityCenter();
}
async function identityCenter(){
  const d=await api('identity/persons');
  if(!token)return;
  renderIdentityCenter(d);
}
$('refresh-identity').addEventListener('click',e=>busy(e.target,identityCenter));
const debouncedIdentitySearch=debounceFn(()=>{if(identityState)renderIdentityPersonsList();},150);
$('identity-search').addEventListener('input',debouncedIdentitySearch);
$('id-link-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('id-link-submit'),async()=>{
    const personId=$('id-link-person').value.trim();
    const platform=$('id-link-platform').value.trim().toLowerCase();
    const userId=$('id-link-uid').value.trim();
    const nickname=$('id-link-nickname').value.trim();
    if(!personId)throw Error('请填写目标 person_id');
    if(!/^[a-z][a-z0-9_]{0,47}$/.test(platform))throw Error('平台标识须以小写字母开头，仅含小写字母、数字和下划线');
    if(!userId)throw Error('请填写账号 user_id');
    const next=await api('identity/link','POST',{person_id:personId,platform,user_id:userId,nickname});
    $('id-link-uid').value='';$('id-link-nickname').value='';
    await applyIdentityResponse(next);
    notice('已将 '+platform+':'+userId+' 绑定到 '+personId);
  });
});
$('id-merge-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('id-merge-submit'),async()=>{
    const src=$('id-merge-source').value.trim();
    const dst=$('id-merge-target').value.trim();
    if(!src||!dst)throw Error('请选择源身份与目标身份');
    if(src===dst)throw Error('源身份与目标身份不能相同');
    if(!(await confirmDialog('确认将 '+src+' 合并至 '+dst+'？源身份的账号将全部转入目标身份，若任一方存在屏蔽或好感为 0 则合并后强制全平台屏蔽并归零。',{danger:true,confirmLabel:'确认合并'})))return;
    const next=await api('identity/merge','POST',{source_person_id:src,target_person_id:dst});
    $('id-merge-source').value='';$('id-merge-target').value='';
    await applyIdentityResponse(next);
    notice('已将 '+src+' 合并至 '+dst);
  });
});

const PLUGIN_SCOPE_LABELS={
  actions:'actions · 动作注册与执行',
  sensors:'sensors · 传感器与世界事件',
  thinking_scenes:'thinking_scenes · 思考场景与生成',
  chat:'chat · 聊天平台收发',
  media:'media · 媒体资源能力',
  owner_notify:'owner_notify · Owner 主动提醒',
  node_bridge:'节点桥接 (node_bridge)'
};
function clearPluginTokenBanner(){
  currentPluginToken='';
  currentPluginTokenId='';
  text('pb-token-meta','');
  text('pb-token-value','');
  $('pb-token-banner').hidden=true;
}
function showPluginTokenBanner(res,fallbackId=''){
  if(!res||!res.token)return;
  currentPluginToken=String(res.token);
  currentPluginTokenId=String(res.plugin_id||fallbackId||'');
  text('pb-token-meta','插件 ID：'+(currentPluginTokenId||'—')+(res.ws_path?' · WS 路径：'+res.ws_path:''));
  text('pb-token-value',currentPluginToken);
  $('pb-token-banner').hidden=false;
}
function renderScopeCheckboxes(container,catalog,selected){
  const list=Array.isArray(catalog)&&catalog.length?catalog:DEFAULT_PLUGIN_SCOPES;
  const chosen=new Set(selected||[]);
  container.replaceChildren(...list.map(scope=>{
    const label=el('label');
    const cb=el('input');cb.type='checkbox';cb.value=scope;cb.checked=chosen.has(scope);
    label.append(cb,el('span',PLUGIN_SCOPE_LABELS[scope]||scope));
    return label;
  }));
}
renderScopeCheckboxes($('pb-create-scopes'),DEFAULT_PLUGIN_SCOPES,[]);
function renderPluginBus(d){
  if(!d)return;
  pluginBusState=d;
  const catalog=Array.isArray(d.allowed_scopes_catalog)&&d.allowed_scopes_catalog.length?d.allowed_scopes_catalog:DEFAULT_PLUGIN_SCOPES;
  const plugins=Array.isArray(d.plugins)?d.plugins:[];
  const connectedCount=plugins.filter(p=>p.online).length;
  $('pb-pills').replaceChildren(
    d.ws_enabled?pill('WS 总线已启用','ok'):pill('WS 总线未启用','warn'),
    pill('接入路径 '+(d.ws_path||'/ws/plugin'),'paid'),
    pill('在线 '+connectedCount+' / 已签发 '+plugins.length,connectedCount>0?'ok':'muted')
  );
  text('pb-state',catalog.length?'可签发的权限范围：'+catalog.join('、'):'');
  const createChecked=[...$('pb-create-scopes').querySelectorAll('input[type=checkbox]:checked')].map(c=>c.value);
  renderScopeCheckboxes($('pb-create-scopes'),catalog,createChecked);
  $('pb-create-submit').disabled=false;

  const host=$('pb-plugins-list');
  if(!plugins.length){
    host.replaceChildren(el('p','暂未签发远程插件凭据，可在上方表单创建。','empty-state'));
    return;
  }
  host.replaceChildren(...plugins.map(p=>{
    const card=el('article',undefined,'sub-card');
    const head=el('div',undefined,'sub-card-head');
    const titleRow=el('div',undefined,'row');
    titleRow.append(el('h3',p.display_name||p.plugin_id),el('code',p.plugin_id,'code-mono'));
    const pills=el('div',undefined,'row');
    pills.append(
      p.online?pill('在线','ok'):pill('离线','muted'),
      p.enabled?pill('凭据已启用','paid'):pill('凭据已禁用','err'),
      pill(regionName(p.node_region||'KR'),p.node_region==='CN'?'vertex':'muted')
    );
    head.append(titleRow,pills);
    card.append(head);

    const chatPlats=(p.chat_platforms||[]).filter(Boolean);
    const scenes=(p.thinking_scenes||[]).map(s=>typeof s==='string'?s:(s?.scene||s?.name||String(s))).filter(Boolean);
    const metaParts=[
      '来源 IP：'+(p.remote_ip||'—'),
      '建连：'+dateTime(p.connected_at),
      '最近活跃：'+dateTime(p.last_seen_at),
      '动作数：'+(p.actions_count??0)
    ];
    if(chatPlats.length)metaParts.push('聊天平台：'+chatPlats.join(', '));
    if(scenes.length)metaParts.push('思考场景：'+scenes.join(', '));
    card.append(el('p',metaParts.join(' · '),'muted'));

    const scopeRow=el('div',undefined,'pill-row');
    const scopes=Array.isArray(p.scopes)?p.scopes:[];
    if(scopes.length)scopeRow.append(el('small','已授权 Scope：','muted'),...scopes.map(s=>pill(s,'paid')));
    else scopeRow.append(el('small','已授权 Scope：无（仅心跳）','muted'));
    card.append(scopeRow);

    const opRow=el('div',undefined,'row');
    const toggleBtn=button(p.enabled?'禁用凭据':'启用凭据',async()=>{
      const next=await api('plugins/'+encodeURIComponent(p.plugin_id),'PUT',{enabled:!p.enabled});
      await applyPluginBusResponse(next);
      notice((p.enabled?'已禁用插件 ':'已启用插件 ')+p.plugin_id);
    },'secondary');
    const rotateBtn=button('轮换 Token',async()=>{
      if(!(await confirmDialog('确认轮换 '+p.plugin_id+' 的连接 Token？旧 Token 将立即失效，当前在线连接也会被断开。',{danger:true,confirmLabel:'轮换 Token'})))return;
      const res=await api('plugins/'+encodeURIComponent(p.plugin_id)+'/rotate','POST');
      await applyPluginBusResponse(res?.state||null);
      showPluginTokenBanner(res,p.plugin_id);
      notice('已轮换 '+p.plugin_id+' 的 Token，请立即保存新 Token');
    },'secondary');
    const discBtn=button('断开会话',async()=>{
      const next=await api('plugins/'+encodeURIComponent(p.plugin_id)+'/disconnect','POST');
      await applyPluginBusResponse(next);
      notice('已断开 '+p.plugin_id+' 的当前会话');
    },'secondary');
    discBtn.disabled=!p.online;
    const delBtn=button('吊销删除',async()=>{
      if(!(await confirmDialog('确认永久吊销并删除远程插件 '+p.plugin_id+'？当前在线连接将立即终止。',{danger:true,confirmLabel:'吊销删除'})))return;
      const next=await api('plugins/'+encodeURIComponent(p.plugin_id),'DELETE');
      if(currentPluginTokenId===p.plugin_id)clearPluginTokenBanner();
      await applyPluginBusResponse(next);
      notice('已吊销插件 '+p.plugin_id);
    },'danger');
    opRow.append(toggleBtn,rotateBtn,discBtn,delBtn);
    card.append(opRow);

    const form=el('form',undefined,'inline-edit-form');
    const grid=el('div',undefined,'grid');
    const nameLabel=el('label','展示名称（display_name）');
    const nameInput=el('input');nameInput.type='text';nameInput.maxLength=64;nameInput.required=true;
    nameInput.value=p.display_name||'';
    nameLabel.append(nameInput);

    const regionLabel=el('label','节点区域（node_region）');
    const regionSel=el('select');
    for(const [val,lbl] of [['KR','KR · 境外节点'],['CN','CN · 境内节点']]){
      const o=el('option',lbl);o.value=val;regionSel.append(o);
    }
    regionSel.value=p.node_region==='CN'?'CN':'KR';
    regionLabel.append(regionSel);
    grid.append(nameLabel,regionLabel);
    form.append(grid);

    const scopesWrap=el('div',undefined,'scope-checks');
    renderScopeCheckboxes(scopesWrap,catalog,scopes);
    form.append(scopesWrap);

    const editFoot=el('div',undefined,'row');
    const enabledCb=el('input');enabledCb.type='checkbox';enabledCb.checked=Boolean(p.enabled);
    const enabledLabel=el('label');enabledLabel.append(enabledCb,' 启用该插件凭据');
    const saveBtn=el('button','保存配置','primary');saveBtn.type='submit';
    editFoot.append(enabledLabel,saveBtn);
    form.append(editFoot);
    if(typeof enhanceLabels==='function')enhanceLabels(form);

    form.addEventListener('submit',e=>{
      e.preventDefault();
      busy(saveBtn,async()=>{
        const displayName=nameInput.value.trim();
        if(!displayName)throw Error('展示名称不能为空');
        const nextScopes=[...scopesWrap.querySelectorAll('input[type=checkbox]:checked')].map(c=>c.value);
        const payload={
          display_name:displayName,
          scopes:nextScopes,
          node_region:regionSel.value==='CN'?'CN':'KR',
          enabled:enabledCb.checked
        };
        const next=await api('plugins/'+encodeURIComponent(p.plugin_id),'PUT',payload);
        await applyPluginBusResponse(next);
        notice('已保存插件配置：'+p.plugin_id);
      });
    });
    card.append(form);
    return card;
  }));
}
async function applyPluginBusResponse(next){
  if(next&&Array.isArray(next.plugins)){
    renderPluginBus(next);
  }else{
    const fresh=await api('plugins');
    if(!token)return;
    renderPluginBus(fresh);
  }
}
async function pluginBus(){
  clearPluginTokenBanner();
  const d=await api('plugins');
  if(!token)return;
  renderPluginBus(d);
}
$('refresh-plugin-bus').addEventListener('click',e=>busy(e.target,pluginBus));
$('pb-token-copy').addEventListener('click',e=>busy(e.target,async()=>{
  if(!currentPluginToken)throw Error('当前没有可复制的一次性 Token');
  if(!navigator.clipboard||!navigator.clipboard.writeText)throw Error('当前浏览器环境不支持一键复制，请手动全选 Token 复制');
  await navigator.clipboard.writeText(currentPluginToken);
  notice('已复制插件 Token 到剪贴板');
}));
$('pb-token-dismiss').addEventListener('click',clearPluginTokenBanner);
$('pb-create-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('pb-create-submit'),async()=>{
    const pluginId=$('pb-create-id').value.trim();
    const displayName=$('pb-create-name').value.trim();
    const nodeRegion=$('pb-create-region').value==='CN'?'CN':'KR';
    const scopes=[...$('pb-create-scopes').querySelectorAll('input[type=checkbox]:checked')].map(c=>c.value);
    if(!/^[a-z][a-z0-9_]{1,47}$/.test(pluginId))throw Error('插件 ID 须为 2～48 位，以小写字母开头，仅含小写字母、数字和下划线');
    if(!displayName)throw Error('显示名称不能为空');
    const res=await api('plugins','POST',{plugin_id:pluginId,display_name:displayName,scopes,node_region:nodeRegion});
    $('pb-create-id').value='';
    $('pb-create-name').value='';
    $('pb-create-region').value='KR';
    renderScopeCheckboxes($('pb-create-scopes'),pluginBusState?.allowed_scopes_catalog||DEFAULT_PLUGIN_SCOPES,[]);
    await applyPluginBusResponse(res?.state||null);
    showPluginTokenBanner(res,pluginId);
    notice('已签发远程插件凭据：'+pluginId+'（请立即保存一次性 Token）');
  });
});

let affectionState=null;
let affHistoryState=null;
let affHistoryFilter={mode:'all',user_id:'',platform:'onebot',nickname:''};
const affHistoryKindLabels={
  chat:'对话互动',violation:'安全违规',annoyance:'高频打扰',admin:'后台调分',
  admin_reset:'档案重置',unmute_recovery:'解封恢复',cooldown_recovery:'冷静期回暖',merge:'身份合并'
};
const defaultAffTiers=[
  {level:0,score_range:'0',tier:'blacklisted',tier_label:'拉黑绝交',summary:'直接回复 0% · 预算 0x · 0 图 · 触发告别语并自动移入小黑屋'},
  {level:1,score_range:'1-19',tier:'hostile',tier_label:'厌烦防备',summary:'直接回复 50% · 门槛 0.95 · 预算 0.30x · 0 图 · 禁戳与全部扩展'},
  {level:2,score_range:'20-39',tier:'cold',tier_label:'冷淡疏离',summary:'直接回复 85% · 门槛 +0.16 · 预算 0.60x · 1 张低清图'},
  {level:3,score_range:'40-59',tier:'normal',tier_label:'普通群友',summary:'直接回复 100% · 默认门槛 · 预算 1.00x · 解锁表情包、贴表情、B站摘要、联网、长卡片、双轨画像'},
  {level:4,score_range:'60-79',tier:'familiar',tier_label:'投缘熟人',summary:'直接回复 100% · 门槛 -0.08 · 预算 1.15x · 解锁主动戳、读文件'},
  {level:5,score_range:'80-100',tier:'bestie',tier_label:'死党铁磁',summary:'直接回复 100% · 门槛 -0.14 · 预算 1.35x · 解锁看原片（非 Owner 封顶 100，仅 Tier 3）'},
  {level:6,score_range:'100-120',tier:'owner_bond',tier_label:'专属羁绊 (Owner)',owner_exclusive:true,exclusive_perks:['唯一 100–120 专属分域','Carbon 深度推理 (Tier 4) 独享','2.50x 单日预算倍率','群聊合批绝对优先护航','永不降级与免拉黑静音'],summary:'全天下唯一 BOISER 独享（门槛 -0.20 · 2.50x · Carbon Tier 4 · 合批绝对优先护航 · 100～120 永不降级）'}
];
function affTierPill(level,tier,label){
  const lv=Number.isFinite(Number(level))?Number(level):({blacklisted:0,hostile:1,cold:2,normal:3,familiar:4,bestie:5,owner_bond:6}[tier]??3);
  const textLabel=label||('Lv.'+lv);
  if(lv>=6)return pill(textLabel,'paid');
  if(lv===5)return pill(textLabel,'ok');
  if(lv===4)return pill(textLabel,'free');
  if(lv===2)return pill(textLabel,'warn');
  if(lv<=1)return pill(textLabel,'err');
  return pill(textLabel,'muted');
}
function describeTierCaps(lv,caps,fb){
  if(!caps)return fb.summary;
  const isExclusive=Boolean(caps.owner_exclusive)||lv>=6;
  const prob=Math.round(Number(caps.direct_reply_prob??1)*100)+'%';
  const mult=(caps.budget_multiplier!=null?String(caps.budget_multiplier):'1')+'x';
  const imgs=(caps.max_images??0)+' 张'+(caps.low_res_images_only&&(caps.max_images??0)>0?'低清':'');
  const thDelta=Number(caps.reply_threshold_delta??0);
  const thNote=lv===0?'':lv===1?' · 门槛 0.95':thDelta>0?(' · 门槛 +'+thDelta.toFixed(2)):thDelta<0?(' · 门槛 '+thDelta.toFixed(2)):'';
  const feats=[];
  if(caps.allow_poke_inbound)feats.push('入站戳');
  if(caps.allow_sticker)feats.push('表情包');
  if(caps.allow_emoji_like)feats.push('贴表情');
  if(caps.allow_bili_summary)feats.push('B站摘要');
  if(caps.allow_poke)feats.push('主动戳');
  if(caps.allow_search)feats.push('联网');
  if(caps.allow_file_read)feats.push('读文件');
  if(caps.allow_text_card)feats.push('卡片');
  if(caps.allow_dual_profile)feats.push('双轨画像');
  if(caps.allow_video_watch)feats.push('看原片');
  if(caps.allow_carbon&&isExclusive)feats.push('Carbon 独享');
  const ownerNote=isExclusive?' · 全天下唯一 BOISER 独享 (100～120 · Tier 4)':lv===5?' · 非 Owner 封顶 100 · Max Tier 3':lv===0?' · 自动入小黑屋':'';
  return '回复 '+prob+thNote+' · 预算 '+mult+' · 图片 '+imgs+' · '+(feats.length?feats.join(' / '):'仅基础防护')+ownerNote;
}
function renderAffectionTiers(tiers){
  const list=Array.isArray(tiers)&&tiers.length?tiers:defaultAffTiers;
  table('aff-tiers-table',['阶梯 / 分数段','称谓 / 标识','能力与权限解锁'],list.map((t,idx)=>{
    const fb=defaultAffTiers[idx]||defaultAffTiers[3];
    const lv=Number.isFinite(Number(t.level))?Number(t.level):(Number.isFinite(Number(t.tier))?Number(t.tier):fb.level);
    const range=t.score_range||t.range||(t.min_score!=null&&t.max_score!=null?(t.min_score===t.max_score?String(t.min_score):t.min_score+'～'+t.max_score):fb.score_range);
    const code=(typeof t.tier==='string'&&t.tier)||t.key||t.code||fb.tier;
    const label=t.tier_label||t.label||t.name||fb.tier_label;
    const isExclusive=Boolean(t.owner_exclusive||t.capabilities?.owner_exclusive||lv>=6);
    const lvLabel=(t.lv||('Lv.'+lv))+'（'+range+(isExclusive?' · 唯一专属':'')+'）';
    const badgeBox=el('div',undefined,'row');
    badgeBox.append(affTierPill(lv,code,label),el('small',code,'code-mono'));
    if(isExclusive)badgeBox.append(pill('✦ 唯一专属 · BOISER','paid'));
    const capText=t.capabilities?describeTierCaps(lv,t.capabilities,fb):(t.summary||t.description||fb.summary);
    const fullNote=(t.summary&&t.capabilities)?(t.summary+' · '+capText):capText;
    const rawPerks=Array.isArray(t.exclusive_perks)?t.exclusive_perks:(t.exclusive_perks?[String(t.exclusive_perks)]:[]);
    const perks=rawPerks.length?rawPerks:(isExclusive&&Array.isArray(fb.exclusive_perks)?fb.exclusive_perks:[]);
    if(isExclusive&&perks.length){
      const capBox=el('div',undefined,'stack-xs');
      capBox.append(el('span',fullNote),el('small','✦ 独享特权：'+perks.join(' · '),'aff-exclusive-note'));
      return[lvLabel,badgeBox,capBox];
    }
    return[lvLabel,badgeBox,fullNote];
  }));
}
function renderAffectionUsers(){
  const users=affectionState?.users||[];
  const cfg=affectionState?.config||{};
  const q=($('aff-search').value||'').trim().toLowerCase();
  const filtered=q?users.filter(u=>{
    const isImmune=u.safety_status==='owner_immune'||Boolean(u.is_owner);
    const safetyTag=isImmune?'owner_immune 风控免疫':u.safety_status==='risk'?'risk 风控高危':u.safety_status==='watch'?'watch 风控观察':(u.safety_status||'');
    const hay=[u.user_id,u.platform,u.nickname,u.lv,'lv.'+(u.level??''),u.tier,u.tier_label,u.last_reason,u.safety_status||'',safetyTag,u.is_owner?'owner boiser 唯一专属':'',u.muted?'muted 小黑屋':''].join(' ').toLowerCase();
    return hay.includes(q);
  }):users;
  text('aff-users-count',q?('筛选匹配 '+filtered.length+' / '+users.length+' 条档案'):('共 '+users.length+' 条用户好感度档案'));
  table('aff-users-table',['用户 / 身份','昵称','好感度进度','阶梯','今日正向增益','最近原因 / 更新','快捷调分与操作'],filtered.map(u=>{
    const plat=u.platform||'onebot';
    const userBox=el('div',undefined,'stack-xs');
    const idLine=el('div',undefined,'row');
    idLine.append(el('span',u.user_id,'code-mono'),pill(plat,plat==='qq'?'vertex':'muted'));
    userBox.append(idLine);
    const isOwnerImmune=u.safety_status==='owner_immune'||Boolean(u.is_owner);
    const isRisk=!isOwnerImmune&&u.safety_status==='risk';
    const isWatch=!isOwnerImmune&&u.safety_status==='watch';
    if(isOwnerImmune||u.muted||isRisk||isWatch){
      const tagLine=el('div',undefined,'row');
      if(isOwnerImmune)tagLine.append(pill('✦ 唯一专属 · 100~120','paid'),pill('风控免疫','ok'));
      if(isRisk)tagLine.append(pill('风控高危 · '+(u.violations_30d??0)+'次/30d','err'));
      else if(isWatch)tagLine.append(pill('风控观察 · '+(u.violations_30d??0)+'次/30d','warn'));
      if(u.muted)tagLine.append(pill('小黑屋屏蔽中','err'));
      userBox.append(tagLine);
    }
    const maxScore=u.is_owner?120:100;
    const minScore=u.is_owner?100:0;
    const scoreVal=Number(u.score??0);
    const meter=el('div',undefined,'aff-meter');
    const prog=el('progress');
    prog.max=maxScore;
    prog.value=Math.max(0,Math.min(maxScore,scoreVal));
    prog.setAttribute('aria-label','好感度 '+scoreVal);
    const scoreLine=el('div',undefined,'aff-score-line');
    const deltaNum=Number(u.last_delta||0);
    const deltaLabel=deltaNum>0?('+'+deltaNum):deltaNum<0?String(deltaNum):'';
    scoreLine.append(el('span',scoreVal+' / '+maxScore),deltaLabel?el('small',deltaLabel,deltaNum>0?'aff-pos':'aff-neg'):el('small',''));
    meter.append(prog,scoreLine);
    const dailyGain=String(u.today_positive_gain??0)+' / '+String(u.daily_positive_cap??cfg.daily_positive_cap??8);
    const reasonBox=el('div',undefined,'stack-xs');
    reasonBox.append(el('span',u.last_reason||'—'),el('small',dateTime(u.updated_at)));
    const actions=el('div',undefined,'row');
    const minusBtn=button('-10',async()=>{
      const nextScore=Math.max(minScore,scoreVal-10);
      await saveAffectionUser(u.user_id,plat,nextScore,u.nickname||'','admin_minus_10');
    });
    if(scoreVal<=minScore)minusBtn.disabled=true;
    const plusBtn=button('+10',async()=>{
      const nextScore=Math.min(maxScore,scoreVal+10);
      await saveAffectionUser(u.user_id,plat,nextScore,u.nickname||'','admin_plus_10');
    });
    if(scoreVal>=maxScore)plusBtn.disabled=true;
    const banBtn=button('拉黑(0)',async()=>{
      if(!(await confirmDialog('确认将 '+u.user_id+' 好感度设为 0 并同步移入小黑屋？',{danger:true,confirmLabel:'拉黑并归零'})))return;
      await saveAffectionUser(u.user_id,plat,0,u.nickname||'','admin_blacklist');
    },'danger');
    if(u.is_owner||scoreVal===0)banBtn.disabled=true;
    const customBtn=button('自定义',async()=>{
      const raw=await promptDialog('自定义好感度分值',{
        message:'设置 '+u.user_id+' 的好感度（'+(u.is_owner?'Owner 保底 100～120':'范围 0～100')+'）',
        defaultValue:String(scoreVal),
        placeholder:minScore+'–'+maxScore,
        confirmLabel:'应用分值'
      });
      if(raw===null)return;
      const target=Number(raw.trim());
      if(!Number.isInteger(target)||target<minScore||target>maxScore)throw Error('好感度须为 '+minScore+'～'+maxScore+' 的整数');
      await saveAffectionUser(u.user_id,plat,target,u.nickname||'','admin_custom');
    });
    const histBtn=button('流水',async()=>{
      const fold=$('community-fold-history');
      if(fold)fold.open=true;
      await loadAffectionHistory({mode:'user',user_id:u.user_id,platform:plat,nickname:u.nickname||''});
      $('aff-history-scope')?.scrollIntoView({behavior:'smooth',block:'nearest'});
    },'secondary');
    const resetBtn=button('重置',async()=>{
      if(!(await confirmDialog('确认重置 '+u.user_id+' 的好感度档案并解除因好感归零触发的屏蔽？',{danger:true,confirmLabel:'重置档案'})))return;
      await resetAffectionUser(u.user_id,plat);
    },'secondary');
    actions.append(minusBtn,plusBtn,banBtn,customBtn,histBtn,resetBtn);
    return[userBox,u.nickname||'—',meter,affTierPill(u.level,u.tier,(u.lv?u.lv+' ':'')+(u.tier_label||u.tier||'')),dailyGain,reasonBox,actions];
  }));
}
function renderAffectionHistory(data){
  const items=Array.isArray(data)?data:(data?.items||[]);
  table('aff-history-table',['时间','用户 / 身份','变动','Δ','类别','原因'],items.map(item=>{
    const plat=item.platform||'onebot';
    const userBox=el('div',undefined,'stack-xs');
    const idLine=el('div',undefined,'row');
    idLine.append(el('span',item.user_id||'—','code-mono'),pill(plat,plat==='qq'?'vertex':'muted'));
    if(item.person_id==='owner')idLine.append(pill('✦ 唯一专属','paid'));
    userBox.append(idLine);
    const sub=[item.nickname,item.person_id&&item.person_id!=='owner'?item.person_id:''].filter(Boolean).join(' · ');
    if(sub)userBox.append(el('small',sub));
    const changeNode=el('span',String(item.prev_score??'—')+' → '+String(item.new_score??'—'),'code-mono');
    const delta=Number(item.delta||0);
    const deltaNode=el('strong',delta>0?('+'+delta):String(delta),delta>0?'aff-pos':delta<0?'aff-neg':'muted');
    const kind=String(item.kind||'');
    const kindLabel=affHistoryKindLabels[kind]?(affHistoryKindLabels[kind]+' ('+kind+')'):(kind||'—');
    const kindVariant=(kind==='violation'||kind==='annoyance')?'err':(kind==='cooldown_recovery'||kind==='unmute_recovery')?'ok':(kind==='admin'||kind==='admin_reset')?'paid':'muted';
    return[dateTime(item.created_at),userBox,changeNode,deltaNode,pill(kindLabel,kindVariant),item.reason||'—'];
  }));
}
async function loadAffectionHistory(filter){
  const nextFilter=(filter&&filter.mode==='user')
    ?{mode:'user',user_id:String(filter.user_id||''),platform:String(filter.platform||'onebot'),nickname:String(filter.nickname||'')}
    :{mode:'all',user_id:'',platform:'onebot',nickname:''};
  let d;
  if(nextFilter.mode==='user'){
    const targetPath=encodeURIComponent((nextFilter.platform||'onebot')+':'+nextFilter.user_id);
    d=await api('affection/users/'+targetPath+'/history');
    if(!token)return;
    affHistoryFilter=nextFilter;
    const who=nextFilter.nickname?(nextFilter.nickname+' ('+nextFilter.user_id+')'):nextFilter.user_id;
    text('aff-history-scope','用户流水 · '+who+' ['+nextFilter.platform+'] · 最近 30 条');
  }else{
    d=await api('affection/history');
    if(!token)return;
    affHistoryFilter=nextFilter;
    text('aff-history-scope','最近全局 50 条加密审计流水');
  }
  affHistoryState=d;
  renderAffectionHistory(d);
}
function renderAffection(d){
  if(!d||!d.config)return;
  affectionState=d;
  const c=d.config;
  $('aff-enabled').checked=Boolean(c.enabled);
  $('aff-initial-score').value=c.initial_score??50;
  $('aff-daily-cap').value=c.daily_positive_cap??8;
  $('aff-unmute-score').value=c.unmute_recovery_score??25;
  $('aff-farewell').value=c.farewell_message||'';
  $('aff-cooldown-days').value=c.cooldown_recovery_days??7;
  $('aff-cooldown-target').value=c.cooldown_recovery_target??40;
  $('aff-penalize-rl').checked=Boolean(c.penalize_rate_limit);
  $('aff-penalize-poke').checked=Boolean(c.penalize_poke_ignored);
  $('aff-decouple-safety').checked=Boolean(c.decouple_safety_violations??true);
  $('aff-viol-inj').value=c.violation_injection_delta??-8;
  $('aff-viol-blk').value=c.violation_content_delta??-10;
  $('aff-viol-floor').value=c.violation_affection_floor??15;
  const srcLabel=(d.source==='admin'||d.source==='sqlite')?'后台 SQLite（重启保留）':'.env 初始值';
  text('affection-source','配置来源：'+srcLabel+' · 当前状态：'+(c.enabled?'已启用':'已停用')+' · 全天下唯一 BOISER 恒定 Lv.6（100～120）· 非 Owner 严禁查询/泄露分值');
  for(const id of ['aff-save-config','aff-reset-config','aff-set-submit'])$(id).disabled=false;
  clearDirty('aff-config-form');
  renderAffectionTiers(d.tiers);
  renderAffectionUsers();
}
async function affection(){
  const targetFilter=affHistoryFilter.mode==='user'?affHistoryFilter:{mode:'all'};
  const [affRes,histRes]=await Promise.allSettled([api('affection'),loadAffectionHistory(targetFilter)]);
  if(!token)return;
  if(affRes.status==='rejected')throw affRes.reason;
  renderAffection(affRes.value);
  if(histRes.status==='rejected'&&histRes.reason?.name!=='AbortError'){
    affHistoryState=null;
    text('aff-history-scope','审计流水暂不可用');
    renderAffectionHistory([]);
  }
}
async function saveAffectionUser(user_id,platform,score,nickname,reason){
  const plat=platform||'onebot';
  const next=await api('affection/users','PUT',{user_id,platform:plat,score,nickname:nickname||'',reason:reason||'admin_set'});
  if(next&&next.config){
    renderAffection(next);
    if(affHistoryFilter.mode==='all'||(affHistoryFilter.mode==='user'&&affHistoryFilter.user_id===user_id&&(affHistoryFilter.platform||'onebot')===plat)){
      await loadAffectionHistory(affHistoryFilter).catch(()=>{});
    }
  }else{
    await affection();
  }
  await community();
  notice('已将 '+user_id+' 好感度设为 '+score+(score===0?'（已同步移入小黑屋）':''));
}
async function resetAffectionUser(user_id,platform){
  const plat=platform||'onebot';
  const next=await api('affection/users/'+encodeURIComponent(plat+':'+user_id),'DELETE');
  if(next&&next.config){
    renderAffection(next);
    if(affHistoryFilter.mode==='all'||(affHistoryFilter.mode==='user'&&affHistoryFilter.user_id===user_id&&(affHistoryFilter.platform||'onebot')===plat)){
      await loadAffectionHistory(affHistoryFilter).catch(()=>{});
    }
  }else{
    await affection();
  }
  await community();
  notice('已重置 '+user_id+' 好感度档案');
}
$('refresh-affection').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('community','刷新好感度')))return;
  await affection();
}));
$('aff-history-all').addEventListener('click',e=>busy(e.target,()=>loadAffectionHistory({mode:'all'})));
$('aff-history-refresh').addEventListener('click',e=>busy(e.target,()=>loadAffectionHistory(affHistoryFilter)));
const debouncedAffSearch=debounceFn(()=>{if(affectionState)renderAffectionUsers();},150);
$('aff-search').addEventListener('input',debouncedAffSearch);
$('aff-config-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('aff-save-config'),async()=>{
    const initScore=Number($('aff-initial-score').value);
    const dailyCap=Number($('aff-daily-cap').value);
    const unmuteScore=Number($('aff-unmute-score').value);
    const cooldownDaysRaw=$('aff-cooldown-days').value.trim();
    const cooldownTargetRaw=$('aff-cooldown-target').value.trim();
    const violInjRaw=$('aff-viol-inj').value.trim();
    const violBlkRaw=$('aff-viol-blk').value.trim();
    const violFloorRaw=$('aff-viol-floor').value.trim();
    const cooldownDays=Number(cooldownDaysRaw);
    const cooldownTarget=Number(cooldownTargetRaw);
    const violInj=Number(violInjRaw);
    const violBlk=Number(violBlkRaw);
    const violFloor=Number(violFloorRaw);
    const farewell=$('aff-farewell').value.trim();
    if(!Number.isInteger(initScore)||initScore<1||initScore>100)throw Error('非 Owner 初始分须为 1–100 的整数');
    if(!Number.isInteger(dailyCap)||dailyCap<0||dailyCap>50)throw Error('每日正向加分上限须为 0–50 的整数');
    if(!Number.isInteger(unmuteScore)||unmuteScore<1||unmuteScore>60)throw Error('解封恢复分须为 1–60 的整数');
    if(!cooldownDaysRaw||!Number.isInteger(cooldownDays)||cooldownDays<0||cooldownDays>90)throw Error('冷静期自动回暖天数须为 0–90 的整数');
    if(!cooldownTargetRaw||!Number.isInteger(cooldownTarget)||cooldownTarget<20||cooldownTarget>60)throw Error('冷静期回暖目标分须为 20–60 的整数');
    if(!violInjRaw||!Number.isInteger(violInj)||violInj<-50||violInj>0)throw Error('注入拦截好感扣分须为 -50–0 的整数');
    if(!violBlkRaw||!Number.isInteger(violBlk)||violBlk<-50||violBlk>0)throw Error('内容拦截好感扣分须为 -50–0 的整数');
    if(!violFloorRaw||!Number.isInteger(violFloor)||violFloor<0||violFloor>50)throw Error('安全拦截好感保底分须为 0–50 的整数');
    if(!farewell||farewell.length>120)throw Error('归零绝交告别语须为 1–120 字');
    const next=await api('affection','PUT',{
      enabled:$('aff-enabled').checked,
      initial_score:initScore,
      daily_positive_cap:dailyCap,
      unmute_recovery_score:unmuteScore,
      farewell_message:farewell,
      cooldown_recovery_days:cooldownDays,
      cooldown_recovery_target:cooldownTarget,
      penalize_rate_limit:$('aff-penalize-rl').checked,
      penalize_poke_ignored:$('aff-penalize-poke').checked,
      decouple_safety_violations:$('aff-decouple-safety').checked,
      violation_injection_delta:violInj,
      violation_content_delta:violBlk,
      violation_affection_floor:violFloor
    });
    renderAffection(next);
    notice('好感度全局策略已保存并立即生效');
  });
});
$('aff-reset-config').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复默认好感度全局策略？',{danger:true,confirmLabel:'恢复默认'})))return;
  const next=await api('affection','DELETE');
  renderAffection(next);
  notice('已恢复默认好感度策略');
}));
$('aff-user-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('aff-set-submit'),async()=>{
    const platform=$('aff-set-platform').value.trim().toLowerCase();
    const uid=$('aff-set-uid').value.trim();
    const score=Number($('aff-set-score').value);
    const nickname=$('aff-set-nickname').value.trim();
    const reason=$('aff-set-reason').value.trim()||'admin_set';
    if(!/^[a-z][a-z0-9_]{0,47}$/.test(platform))throw Error('平台标识须以小写字母开头，仅含小写字母、数字和下划线');
    if(platform==='onebot'&&!/^[1-9][0-9]{0,19}$/.test(uid))throw Error('OneBot QQ 号须为不含前导零的纯数字');
    else if(platform==='qq'&&!/^[A-Za-z0-9_-]{1,128}$/.test(uid))throw Error('QQ 官方 ID 仅支持字母、数字、下划线或短横线');
    else if(platform!=='onebot'&&platform!=='qq'&&!/^[A-Za-z0-9_.:@-]{1,128}$/.test(uid))throw Error('用户 ID 须为 1–128 位字母、数字或 _ . : @ -');
    if(!Number.isInteger(score)||score<0||score>120)throw Error('目标好感度须为 0–120 的整数');
    await saveAffectionUser(uid,platform,score,nickname,reason);
    $('aff-set-uid').value='';
    $('aff-set-nickname').value='';
  });
});

function parseCommunityMuteTarget(value){
  const raw=String(value||'').trim();
  if(!raw)throw Error('请输入纯数字 QQ 号或 平台:用户ID（如 telegram:123456）');
  if(raw.includes(':')){
    const idx=raw.indexOf(':');
    const platform=raw.slice(0,idx).trim().toLowerCase();
    const uid=raw.slice(idx+1).trim();
    if(!/^[a-z][a-z0-9_]{0,47}$/.test(platform))throw Error('平台标识须以小写字母开头，仅含小写字母、数字和下划线');
    if(platform==='onebot'){
      if(!/^[1-9][0-9]{0,19}$/.test(uid))throw Error('OneBot QQ 号须为不含前导零的纯数字');
      return {platform:'onebot',user_id:uid,target:uid};
    }
    if(platform==='qq'){
      if(!/^[A-Za-z0-9_-]{1,128}$/.test(uid))throw Error('QQ 官方 ID 仅支持字母、数字、下划线或短横线');
    }else if(!/^[A-Za-z0-9_.:@-]{1,128}$/.test(uid)){
      throw Error('用户 ID 须为 1–128 位字母、数字或 _ . : @ -');
    }
    return {platform,user_id:uid,target:platform+':'+uid};
  }
  if(!/^[1-9][0-9]{0,19}$/.test(raw))throw Error('请输入纯数字 QQ 号，或使用 平台:用户ID（如 telegram:123456）');
  return {platform:'onebot',user_id:raw,target:raw};
}
function communityPlatformBadge(platform){
  const p=String(platform||'onebot').toLowerCase();
  if(typeof formatPlatformBadge==='function')return formatPlatformBadge(p);
  if(p==='onebot')return pill('QQ','paid');
  if(p==='qq')return pill('QQ 官方','vertex');
  if(p==='telegram')return pill('Telegram','tg');
  return pill(p,'muted');
}
async function syncCommunityAndAffection(){await community();await affection().catch(()=>{});}
const muteDeleteLabels={none:'仅屏蔽',not_requested:'仅屏蔽',not_supported:'不适用（跨平台）',requested:'待删好友',attempted:'删好友中',done:'已删好友',deleted:'已删好友',not_friend:'非好友',uncertain:'结果不确定',unavailable:'暂不可用',failed:'删好友失败'};
const proposalStatusLabels={pending:'待审批',approved:'已批准',rejected:'已驳回'};
const cfgSourceLabels={admin:'SQLite 热配置',sqlite:'SQLite 热配置',env:'.env 默认值',none:'未配置'};
const signStatusLabels={never:'尚未自动换签',ok:'更新成功',updated:'更新成功',skipped:'冷却跳过',disabled:'未启用',failed:'更新失败'};

async function longnick(){
  const n=await api('longnick');if(!token)return;
  $('sign-enabled').checked=n.config.enabled;
  $('sign-cooldown').value=n.config.cooldown_hours;
  clearDirty('sign-config-form');
  const nextAutoText=n.next_auto_after>Date.now()/1000?dateTime(n.next_auto_after):'随时可更新';
  text('sign-state','设置来源：'+(cfgSourceLabels[n.source]||n.source)+' · 最近状态 '+(signStatusLabels[n.status]||n.status)+' · 成功时间 '+(n.last_success?dateTime(n.last_success):'尚无记录')+(n.last_error?' · '+n.last_error:'')+' · 下次自动可更新 '+nextAutoText);
}
async function community(){
  const m=await api('moderation');if(!token)return;
  const mutedRows=[];
  const seenMuted=new Set();
  for(const item of [...(Array.isArray(m.muted_users)?m.muted_users:[]),...(Array.isArray(m.platform_muted_users)?m.platform_muted_users:[])]){
    const plat=String(item.platform||'onebot').toLowerCase();
    const uid=String(item.user_id||'');
    if(!uid)continue;
    const dedupeKey=plat+':'+uid;
    if(seenMuted.has(dedupeKey))continue;
    seenMuted.add(dedupeKey);
    mutedRows.push({...item,platform:plat,user_id:uid,target:item.target||(plat==='onebot'?uid:(plat+':'+uid))});
  }
  mutedRows.sort((a,b)=>Number(b.created||0)-Number(a.created||0));
  table('mute-list',['平台 / 账号','加入时间','好友操作','操作'],mutedRows.map(x=>{
    const acctCell=el('div',undefined,'stack-xs');
    const idRow=el('div',undefined,'platform-cell');
    idRow.append(communityPlatformBadge(x.platform),el('code',x.user_id,'code-mono'));
    acctCell.append(idRow);
    if(x.nickname)acctCell.append(el('small',x.nickname,'muted'));
    const unmuteTarget=x.platform==='onebot'?x.user_id:(x.platform+':'+x.user_id);
    return[
      acctCell,
      dateTime(x.created),
      muteDeleteLabels[x.delete_status]||x.delete_status||'—',
      button('解除屏蔽',async()=>{
        await api('moderation/mute/'+encodeURIComponent(unmuteTarget),'DELETE');
        notice('已解除 '+x.platform+':'+x.user_id+' 的小黑屋屏蔽');
        await syncCommunityAndAffection();
      })
    ];
  }),'当前小黑屋为空，还没有被屏蔽的用户。');
  table('mute-proposals',['编号 / 账号 / 昵称','原因','状态','操作'],(m.proposals||[]).map(x=>{
    const plat=String(x.platform||'onebot').toLowerCase();
    const whoCell=el('div',undefined,'platform-cell');
    whoCell.append(communityPlatformBadge(plat),el('span',x.id+' / '+x.user_id+(x.nickname?' / '+x.nickname:'')));
    const actions=el('div',undefined,'row');
    if(x.status==='pending'){
      actions.append(
        button('批准屏蔽',async()=>{await api('moderation/proposals/'+x.id,'POST',{approve:true});await syncCommunityAndAffection();})
      );
      if(plat==='onebot'){
        actions.append(button('批准并移除好友',async()=>{
          if(!(await confirmDialog('将屏蔽 QQ '+x.user_id+'，并在其为好友时移除好友。确定执行？',{danger:true,confirmLabel:'屏蔽并删好友'})))return;
          await api('moderation/proposals/'+x.id,'POST',{approve:true,delete_friend:true});await syncCommunityAndAffection();
        },'danger'));
      }
      actions.append(button('驳回',async()=>{await api('moderation/proposals/'+x.id,'POST',{approve:false});await community();}));
    }
    return[whoCell,x.reason,proposalStatusLabels[x.status]||x.status,actions];
  }));
}
$('refresh-community').addEventListener('click',e=>busy(e.target,syncCommunityAndAffection));
$('mute-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy(e.submitter,async()=>{
    const parsed=parseCommunityMuteTarget($('mute-qq').value),remove=$('mute-delete').checked;
    if(remove&&parsed.platform!=='onebot')throw Error('仅 QQ（OneBot）账号支持同时移除好友，请取消勾选后再加入小黑屋');
    if(remove&&!(await confirmDialog('确认屏蔽并移除 QQ '+parsed.user_id+' 的好友关系（若存在）？',{danger:true,confirmLabel:'屏蔽并删好友'})))return;
    const r=await api('moderation/users','PUT',{user_id:parsed.target,muted:true,delete_friend:remove});
    notice('屏蔽已生效（'+parsed.platform+':'+parsed.user_id+'） · 好友操作 '+(muteDeleteLabels[r.delete_status]||r.delete_status));
    $('mute-qq').value='';$('mute-delete').checked=false;
    await syncCommunityAndAffection();
  });
});
$('sign-config-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{await api('longnick/config','PUT',{enabled:$('sign-enabled').checked,cooldown_hours:Number($('sign-cooldown').value)});notice('换签设置已保存');await longnick();});});
$('sign-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复每日心情个签的 .env 初始设置？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('longnick/config','DELETE');notice('已恢复 .env 换签设置');await longnick();
}));
$('sign-form').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{const r=await api('longnick','POST',{text:$('sign-text').value});notice('签名更新：'+r.status+(r.error?' · '+r.error:''));await longnick();});});

trackDirty('aff-config-form','好感度全局策略');
trackDirty('sign-config-form','每日心情个签设置');

onLogout(()=>{
  identityState=null;pluginBusState=null;affectionState=null;affHistoryState=null;
  affHistoryFilter={mode:'all',user_id:'',platform:'onebot',nickname:''};
  clearPluginTokenBanner();
  renderScopeCheckboxes($('pb-create-scopes'),DEFAULT_PLUGIN_SCOPES,[]);
  for(const id of ['identity-kpis','identity-persons-list','id-person-options','pb-pills','pb-plugins-list','aff-tiers-table','aff-users-table','aff-history-table','mute-list','mute-proposals'])$(id).replaceChildren();
  for(const id of ['identity-state','pb-state','affection-source','aff-users-count','sign-state'])text(id,'');
  text('aff-history-scope','最近全局 50 条加密审计流水');
  for(const id of ['id-link-submit','id-merge-submit','pb-create-submit','aff-save-config','aff-reset-config','aff-set-submit'])$(id).disabled=true;
});
