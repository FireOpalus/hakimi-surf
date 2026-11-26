let token='',tab='overview',session=null,timer=null;
let sessionRequests=new AbortController();
let displayZone='Asia/Shanghai';
const zoneLabel=zone=>zone==='Asia/Shanghai'?'北京时间':zone;
const $=id=>document.getElementById(id);
const text=(id,value)=>{$(id).textContent=String(value??'');};
const el=(tag,value,cls)=>{
  const n=document.createElement(tag);
  if(value!==undefined)n.textContent=String(value);
  if(cls)n.className=cls;
  return n;
};
const pill=(label,variant='muted')=>el('span',label,'pill pill-'+variant);
const pillRow=(...nodes)=>{
  const d=el('div',undefined,'pill-row');
  d.append(...nodes.filter(Boolean));
  return d;
};
const selectOption=(value,label)=>{
  const option=el('option',label);
  option.value=value;
  return option;
};
const usd=(value,digits=4)=>{
  if(value===null||value===undefined||value==='')return '—';
  const n=Number(value);
  if(!Number.isFinite(n))return String(value);
  return (n<0?'-$':'$')+Math.abs(n).toFixed(digits);
};
function money(amount,digits=6){
  if(amount===null||amount===undefined||amount==='')return '未知';
  const n=Number(amount);
  if(!Number.isFinite(n))return String(amount);
  return usd(n/1e6,digits);
}
function dateTime(t){
  return t?new Date(t*1000).toLocaleString('zh-CN',{timeZone:displayZone,hour12:false}):'—';
}
function debounceFn(fn,ms=150){
  let handle=null;
  return (...args)=>{
    clearTimeout(handle);
    handle=setTimeout(()=>{handle=null;fn(...args);},ms);
  };
}

let noticeTimer=null;
const NOTICE_STACK_MAX=2;
function hideNotice(){
  clearTimeout(noticeTimer);
  noticeTimer=null;
  $('notice').hidden=true;
}
function clearNoticeStack(){
  const stack=$('notice-stack');
  if(stack)stack.replaceChildren();
}
// 新通知到来时，把仍在显示的上一条移入下方的历史栈（最多保留 2 条），避免连续操作时互相覆盖。
function stashNotice(){
  const stack=$('notice-stack');
  const previous=$('notice-text').textContent;
  if(!stack||$('notice').hidden||!previous)return;
  const failed=$('notice').className==='error';
  const item=el('div',undefined,'notice-item'+(failed?' error':''));
  const close=el('button','×','notice-close');
  close.type='button';
  close.setAttribute('aria-label','关闭通知');
  close.addEventListener('click',()=>item.remove?.());
  item.append(el('span',previous,'notice-item-text'),close);
  if(typeof stack.prepend==='function')stack.prepend(item);else stack.append(item);
  while(stack.children.length>NOTICE_STACK_MAX){
    const last=stack.lastElementChild;
    if(!last?.remove)break;
    last.remove();
  }
  if(!failed)setTimeout(()=>item.remove?.(),6000);
}
function notice(value,error=false){
  stashNotice();
  clearTimeout(noticeTimer);
  noticeTimer=null;
  text('notice-text',value);
  $('notice').hidden=false;
  $('notice').className=error?'error':'';
  if(!error)noticeTimer=setTimeout(()=>{$('notice').hidden=true;noticeTimer=null;},6000);
}
$('notice-close').addEventListener('click',hideNotice);

document.addEventListener('visibilitychange',()=>{
  document.body.classList.toggle('page-hidden',document.hidden);
});

const modalEl=$('modal-dialog');
const modalForm=$('modal-form');
const modalInput=$('modal-input');
const modalTextarea=$('modal-textarea');
const modalConfirmBtn=$('modal-confirm');
const modalCancelBtn=$('modal-cancel');
let modalResolver=null;
let modalMode='confirm';
let modalMultiline=false;

function closeModalWith(result){
  const resolve=modalResolver;
  modalResolver=null;
  if(modalEl.open)modalEl.close();
  modalInput.value='';
  modalTextarea.value='';
  if(resolve)resolve(result);
}

modalForm.addEventListener('submit',e=>{
  e.preventDefault();
  if(!modalResolver)return;
  if(modalMode==='prompt'){
    closeModalWith(modalMultiline?modalTextarea.value:modalInput.value);
  }else{
    closeModalWith(true);
  }
});
modalCancelBtn.addEventListener('click',()=>closeModalWith(modalMode==='prompt'?null:false));
$('modal-close').addEventListener('click',()=>closeModalWith(modalMode==='prompt'?null:false));
modalEl.addEventListener('cancel',e=>{
  e.preventDefault();
  closeModalWith(modalMode==='prompt'?null:false);
});
modalEl.addEventListener('click',e=>{
  if(e.target===modalEl)closeModalWith(modalMode==='prompt'?null:false);
});

function openModalDialog(mode,opts={}){
  if(modalResolver)closeModalWith(modalMode==='prompt'?null:false);
  modalMode=mode;
  modalMultiline=Boolean(opts.multiline);
  text('modal-title',opts.title||(mode==='prompt'?'请输入':'确认操作'));
  text('modal-message',opts.message||'');
  $('modal-message').hidden=!opts.message;
  text('modal-hint',opts.hint||'');
  $('modal-hint').hidden=!opts.hint;
  modalCancelBtn.textContent=opts.cancelLabel||'取消';
  modalConfirmBtn.textContent=opts.confirmLabel||(mode==='prompt'?'确定':'确认');
  modalConfirmBtn.className=opts.danger?'danger':'primary';

  const isPrompt=mode==='prompt';
  modalInput.hidden=!isPrompt||modalMultiline;
  modalTextarea.hidden=!isPrompt||!modalMultiline;
  modalInput.disabled=modalInput.hidden;
  modalTextarea.disabled=modalTextarea.hidden;
  if(isPrompt){
    const field=modalMultiline?modalTextarea:modalInput;
    field.value=String(opts.defaultValue??'');
    field.placeholder=opts.placeholder||'';
  }
  return new Promise(resolve=>{
    modalResolver=resolve;
    modalEl.showModal();
    if(isPrompt){
      const field=modalMultiline?modalTextarea:modalInput;
      field.focus();
      field.select();
    }else{
      modalConfirmBtn.focus();
    }
  });
}

function confirmDialog(message,opts={}){
  return openModalDialog('confirm',{
    title:opts.title||'确认操作',
    message,
    hint:opts.hint||'',
    danger:Boolean(opts.danger),
    confirmLabel:opts.confirmLabel||'确认',
    cancelLabel:opts.cancelLabel||'取消'
  });
}

function promptDialog(title,opts={}){
  return openModalDialog('prompt',{
    title,
    message:opts.message||'',
    hint:opts.hint||'',
    defaultValue:opts.defaultValue??'',
    multiline:Boolean(opts.multiline),
    placeholder:opts.placeholder||'',
    confirmLabel:opts.confirmLabel||'确定',
    cancelLabel:opts.cancelLabel||'取消'
  });
}

const dirtyRegistry=new Map();
const dirtyListeners=[];
function onDirtyChange(fn){dirtyListeners.push(fn);}
function openEnclosingDetails(node){
  for(let p=node?.parentElement;p;p=p.parentElement){
    if(p.tagName==='DETAILS')p.open=true;
  }
}
function updatePageDirtyBadge(){
  const dirtyLabels=[];
  for(const entry of dirtyRegistry.values()){
    if(entry.dirty&&entry.tabId===tab)dirtyLabels.push(entry.label);
  }
  for(const fn of dirtyListeners){
    try{fn(dirtyLabels);}catch{}
  }
  const badge=$('page-dirty-badge');
  if(!badge)return;
  badge.hidden=dirtyLabels.length===0;
  badge.textContent=dirtyLabels.length?('有未保存修改：'+dirtyLabels.join('、')):'有未保存修改';
}
function ensureFormDirtyBadge(form){
  const rows=form.querySelectorAll('.row');
  const host=rows.length?rows[rows.length-1]:form;
  let badge=form.querySelector('.dirty-badge[data-dirty-indicator]');
  if(!badge){
    badge=el('span','未保存','dirty-badge');
    badge.dataset.dirtyIndicator='1';
    badge.hidden=true;
    host.append(badge);
  }
  return badge;
}
function trackDirty(formOrId,label){
  const form=typeof formOrId==='string'?$(formOrId):formOrId;
  if(!form||dirtyRegistry.has(form))return;
  const section=form.closest('section.tab');
  const entry={
    form,
    label:label||'表单',
    tabId:section?section.id:'',
    badge:ensureFormDirtyBadge(form),
    dirty:false
  };
  dirtyRegistry.set(form,entry);
  const handleEdit=e=>{
    if(!token)return;
    if(e.target&&e.target.type==='search')return;
    entry.dirty=true;
    entry.badge.hidden=false;
    openEnclosingDetails(entry.form);
    updatePageDirtyBadge();
  };
  form.addEventListener('input',handleEdit);
  form.addEventListener('change',handleEdit);
}
function markDirty(formOrId){
  const form=typeof formOrId==='string'?$(formOrId):formOrId;
  const entry=dirtyRegistry.get(form);
  if(!entry)return;
  entry.dirty=true;
  entry.badge.hidden=false;
  openEnclosingDetails(entry.form);
  updatePageDirtyBadge();
}
function clearDirty(formOrId){
  const form=typeof formOrId==='string'?$(formOrId):formOrId;
  const entry=dirtyRegistry.get(form);
  if(!entry)return;
  entry.dirty=false;
  entry.badge.hidden=true;
  updatePageDirtyBadge();
}
function clearTabDirty(tabId){
  for(const entry of dirtyRegistry.values()){
    if(entry.tabId===tabId){
      entry.dirty=false;
      entry.badge.hidden=true;
    }
  }
  updatePageDirtyBadge();
}
function clearAllDirty(){
  for(const entry of dirtyRegistry.values()){
    entry.dirty=false;
    entry.badge.hidden=true;
  }
  updatePageDirtyBadge();
}
function dirtyFormsForTab(tabId){
  const labels=[];
  for(const entry of dirtyRegistry.values()){
    if(entry.dirty&&entry.tabId===tabId)labels.push(entry.label);
  }
  return labels;
}
function isTabDirty(tabId=tab){
  return dirtyFormsForTab(tabId).length>0;
}
async function confirmDiscardTabChanges(tabId=tab,actionLabel='离开当前页面'){
  const labels=dirtyFormsForTab(tabId);
  if(!labels.length)return true;
  const ok=await confirmDialog(
    '当前页面的「'+labels.join('、')+'」有尚未保存的修改。确定要'+actionLabel+'并放弃修改吗？',
    {title:'放弃未保存的修改？',danger:true,confirmLabel:'放弃修改',cancelLabel:'继续编辑'}
  );
  if(ok)clearTabDirty(tabId);
  return ok;
}

const logoutHooks=[];
function onLogout(fn){logoutHooks.push(fn);}

async function api(path,method='GET',data){
  const signal=sessionRequests.signal;
  const r=await fetch('/admin/api/'+path,{
    method,signal,
    headers:{Authorization:'Bearer '+token,...(data===undefined?{}:{'Content-Type':'application/json'})},
    body:data===undefined?undefined:JSON.stringify(data),
    cache:'no-store',credentials:'omit',redirect:'error'
  });
  let value;
  try{value=await r.json();}catch(e){if(signal.aborted)throw e;throw Error('服务器未返回有效 JSON');}
  if(signal.aborted)throw new DOMException('请求已取消','AbortError');
  if(!r.ok){
    if(r.status===401||r.status===403)logout();
    const hints={
      invalid_admin_input:'输入参数无效，请检查填写内容。',
      module_disabled:'模块已关闭，可在模块详情中开启。',
      weather_location_missing:'请先保存观测地点。',
      weather_unavailable:'天气服务暂时不可用，请稍后再查。',
      weather_stale:'天气数据已过期，请稍后再查。',
      weather_location_changed:'地点已更改，请重新查询。',
      weather_city_lookup_unavailable:'城市查询暂时不可用，可重试或输入经纬度。',
      weather_ip_lookup_unavailable:'暂时无法识别服务器地点，可重试或搜索城市。',
      module_sensor_unavailable:'暂时无法读取传感器，可稍后重试。',
      module_not_registered:'模块当前未注册，请刷新目录。',
      authentication_required:'管理口令无效，请重新输入。',
      login_rate_limited:'登录尝试过于频繁，请稍后再试。',
      admin_disabled:'管理入口已关闭。',
      protected_identity:'受保护的身份或唯一 Owner 账号不允许此操作。',
      person_not_found:'未找到对应的统一身份档案。',
      account_not_found:'未找到对应的平台账号。',
      identity_unavailable:'统一身份服务暂未就绪。'
    };
    const invField=value?.invalid_field||value?.field||r.headers.get('X-Invalid-Field')||'';
    const baseMsg=hints[value?.detail]||value?.detail||('HTTP '+r.status);
    throw Error(invField&&!baseMsg.includes(invField)?(baseMsg+'（字段：'+invField+'）'):baseMsg);
  }
  return value;
}

async function busy(btn,fn){
  if(!btn)return fn();
  btn.disabled=true;btn.setAttribute('aria-busy','true');
  try{await fn();}catch(e){if(e.name!=='AbortError')notice(e.message,true);}
  finally{btn.disabled=false;btn.removeAttribute('aria-busy');}
}

function button(label,fn,cls='secondary'){
  const b=el('button',label,cls);
  b.type='button';
  b.addEventListener('click',()=>busy(b,fn));
  return b;
}

function table(id,headers,rows){
  const target=typeof id==='string'?$(id):id;
  const t=el('table'),thead=el('thead'),head=el('tr'),body=el('tbody');
  headers.forEach(v=>{const th=el('th',v);th.scope='col';head.append(th);});
  thead.append(head);t.append(thead,body);
  rows.forEach(values=>{
    const row=el('tr');
    values.forEach(v=>{
      const cell=el('td');
      if(v instanceof Node)cell.append(v);
      else cell.textContent=String(v??'—');
      row.append(cell);
    });
    body.append(row);
  });
  target.replaceChildren(t);
  if(!rows.length)target.append(el('p','还没有记录，新的动态会出现在这里。','empty-state'));
}

function kpiCard(label,value,note,variant=''){
  const card=el('div',undefined,'kpi-card'+(variant?' kpi-'+variant:''));
  card.append(el('span',label,'kpi-label'),el('strong',value,'kpi-value'));
  if(note)card.append(el('span',note,'kpi-note'));
  return card;
}

async function loadJsonFileInto(fileInput,textarea){
  const file=fileInput.files?.[0];
  if(!file)return;
  if(file.size>65536){fileInput.value='';throw Error('Service Account JSON 文件超过 64 KiB 上限');}
  const raw=await file.text();
  JSON.parse(raw);
  textarea.value=raw;
  textarea.dispatchEvent(new Event('input',{bubbles:true}));
  fileInput.value='';
  notice('已读取 JSON 文件：'+file.name);
}

function logout(){
  sessionRequests.abort();
  sessionRequests=new AbortController();
  if(timer){clearInterval(timer);timer=null;}
  if(modalResolver)closeModalWith(modalMode==='prompt'?null:false);
  if($('module-finder').open)$('module-finder').close();
  if($('session-dialog').open)$('session-dialog').close();
  session=null;
  token='';
  $('token').value='';
  document.body.classList.remove('authenticated');
  $('menu-toggle').hidden=true;
  $('open-finder').hidden=true;
  $('finder-query').value='';
  $('finder-results').replaceChildren();
  $('login-error').hidden=true;
  $('token').removeAttribute('aria-invalid');
  text('login-error','');
  document.title='洁米奈星夜';
  hideNotice();
  clearNoticeStack();
  text('last-updated','尚未刷新');
  $('last-updated').removeAttribute('datetime');

  for(const form of document.querySelectorAll('#app form, #session-dialog form')){
    form.reset();
  }
  for(const input of document.querySelectorAll('#app input[type="password"], #app input[type="file"], #app input[type="search"], #app textarea, #session-dialog textarea, #session-dialog input')){
    if(input.type==='checkbox'||input.type==='radio')input.checked=false;
    else input.value='';
  }
  clearAllDirty();

  for(const hook of logoutHooks){
    try{hook();}catch{}
  }

  $('login').hidden=false;
  $('app').hidden=true;
  $('logout').hidden=true;
  if(location.hash){
    try{history.replaceState(null,'',location.pathname);}catch{}
  }
  $('token').focus();
}
