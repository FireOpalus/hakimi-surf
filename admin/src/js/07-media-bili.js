const biliRunStatusLabels={ok:'完成',done:'完成',completed:'完成',skipped:'跳过',cooldown:'风控冷却',failed:'失败',uncertain:'结果不确定'};
const videoStatusLabels={succeeded:'成功',ok:'成功',done:'成功',failed:'失败',uncertain:'结果不确定',working:'处理中'};

async function biliAccount(){
  const d=await api('bili-account');if(!token)return;
  const c=d.config,h=d.watch_history_counts;
  $('bili-enabled').checked=c.enabled;
  $('bili-like').checked=c.allow_like;
  $('bili-favorite').checked=c.allow_favorite;
  $('bili-folder').value=c.favorite_folder_id;
  $('bili-limit').value=c.daily_action_limit;
  clearDirty('bili-policy');
  text('bili-state','账号 '+(d.account_mask||'未验证')+' · 时区 '+zoneLabel(d.timezone)+' · 配置来自 '+(cfgSourceLabels[d.source]||d.source)+' · 今日互动尝试 '+d.actions_today+' 次'+(d.uncertain_actions?' · 互动回执待核实 '+d.uncertain_actions+' 条':'')+(d.cooldown_until>Date.now()/1000?' · 冷却至 '+dateTime(d.cooldown_until):'')+(d.last_error?' · '+d.last_error:''));
  text('bili-watch-history','学习完成 '+h.done+' 条 · 处理中 '+h.processing+' 条 · 读取暂时失败 '+h.retryable+' 条'+(h.exhausted?'（其中 '+h.exhausted+' 条已达重试上限）':'')+' · 结果待核实 '+h.uncertain+' 条');
  text('bili-credential-state','凭证来源 '+(cfgSourceLabels[d.credential_source]||d.credential_source)+' · '+Object.entries(d.credentials).map(([k,v])=>k+': '+(v?'已设置（隐藏）':'未设置')).join(' · '));
  table('bili-runs',['触发来源','状态','时间','原因'],d.recent_runs.map(x=>[x.slot.startsWith('life-')?'世界引擎':x.slot.startsWith('manual')?'手动':x.slot,biliRunStatusLabels[x.status]||x.status,dateTime(x.created),x.reason||'—']));
  await videoWatch();
}
for(let n=1;n<=20;n++)$('video-watch-limit').add(new Option(n+' 次',String(n),n===3,n===3));
async function videoWatch(){
  const d=await api('bili-account/video-watch');if(!token)return;
  const c=d.config;
  $('video-watch-enabled').checked=c.enabled;
  $('video-watch-seconds').value=c.max_seconds;
  $('video-watch-limit').value=String(c.daily_limit);
  clearDirty('video-watch-policy');
  const remaining=c.daily_limit==='unlimited'?'不限次数':'剩余 '+d.remaining_today+' 次';
  text('video-watch-state','原片分析 '+(c.enabled?'已开启':'已关闭')+' · 配置来自 '+(cfgSourceLabels[d.source]||d.source)+' · 今日已尝试 '+d.attempts_today+' 次 · '+remaining+(d.remote_cleanup_pending?' · 远端文件待清理 '+d.remote_cleanup_pending+' 个（自动重试）':''));
  table('video-watch-attempts',['时间','视频','状态','原因'],(d.recent_attempts||[]).map(x=>[dateTime(x.created),x.bvid,videoStatusLabels[x.status]||x.status,d.reasons?.[x.reason]||x.reason||'—']));
}
$('video-watch-policy').addEventListener('submit',e=>{
  e.preventDefault();
  busy(e.submitter,async()=>{
    const seconds=Number($('video-watch-seconds').value),selected=$('video-watch-limit').value;
    if(!Number.isInteger(seconds)||seconds<30||seconds>Number($('video-watch-seconds').max))throw Error('视频时长须为 30～900 秒的整数');
    const dailyLimit=selected==='unlimited'?'unlimited':Number(selected);
    await api('bili-account/video-watch','PUT',{enabled:$('video-watch-enabled').checked,max_seconds:seconds,daily_limit:dailyLimit});
    notice('看片设置已保存并立即生效');await videoWatch();
  });
});
$('video-watch-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复 .env 中的原片分析设置？',{danger:true,confirmLabel:'恢复默认'})))return;
  await api('bili-account/video-watch','DELETE');notice('看片设置已恢复 .env 初始值');await videoWatch();
}));
$('bili-refresh').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('bili-account','刷新 B 站账号状态')))return;
  await biliAccount();
}));
$('bili-credentials').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{await api('bili-account/credentials','POST',{sessdata:$('bili-sessdata').value.trim(),bili_jct:$('bili-jct').value.trim(),buvid3:$('bili-buvid3').value.trim(),buvid4:$('bili-buvid4').value.trim()});for(const id of ['bili-sessdata','bili-jct','bili-buvid3','bili-buvid4'])$(id).value='';notice('凭证已加密保存；原有风控冷却继续有效');await biliAccount();});});
$('bili-policy').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{await api('bili-account','PUT',{enabled:$('bili-enabled').checked,allow_like:$('bili-like').checked,allow_favorite:$('bili-favorite').checked,favorite_folder_id:$('bili-folder').value.trim(),daily_action_limit:Number($('bili-limit').value)});notice('小号巡逻设置已保存');await biliAccount();});});
$('bili-clear').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('清除保存的 B 站凭证，停止账号巡逻？',{danger:true,confirmLabel:'清除凭证'})))return;
  await api('bili-account/credentials','POST',{clear:true});notice('已清除保存的 B 站凭证');await biliAccount();
}));
$('bili-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('恢复 B 站小号巡逻的 .env 初始设置？',{danger:true,confirmLabel:'恢复 .env 设置'})))return;
  await api('bili-account','DELETE');notice('小号巡逻设置已恢复 .env 初始值');await biliAccount();
}));
for(const [id,action] of [['bili-test','test'],['bili-run','run']]){
  $(id).addEventListener('click',e=>busy(e.target,async()=>{notice('正在执行，低频请求可能需要一些时间…');const r=await api('bili-account/'+action,'POST');notice('小号巡逻：'+r.status+(r.reason?' · '+r.reason:''));await biliAccount();}));
}

let memeGeneration=0;
const memeBlobCache=new Map();
let memeObserver=null;
let memeFetchQueue=[];
let memeActiveFetches=0;
const MEME_CONCURRENCY=4;

function resetMemeLoader(){
  memeGeneration++;
  if(memeObserver){memeObserver.disconnect();memeObserver=null;}
  memeFetchQueue=[];
}
function clearMemePreviews(revokeAll=false){
  resetMemeLoader();
  if(revokeAll){
    for(const url of memeBlobCache.values())URL.revokeObjectURL(url);
    memeBlobCache.clear();
  }
  $('meme-list').replaceChildren();
  text('meme-state','');
}
async function memeRequest(path,method='GET',body){
  const signal=sessionRequests.signal;
  const r=await fetch('/admin/api/memes'+path,{method,signal,headers:{Authorization:'Bearer '+token,...(body?{'Content-Type':body.type||'application/octet-stream'}:{})},body,cache:'no-store',credentials:'omit',redirect:'error'});
  if(!r.ok){
    let detail='HTTP '+r.status;try{detail=(await r.json()).detail||detail;}catch{}
    if(!signal.aborted&&(r.status===401||r.status===403))logout();
    throw Error(detail);
  }
  return r;
}
function pumpMemeQueue(){
  while(memeActiveFetches<MEME_CONCURRENCY&&memeFetchQueue.length&&token){
    const task=memeFetchQueue.shift();
    if(task.generation!==memeGeneration)continue;
    if(memeBlobCache.has(task.id)){
      task.img.src=memeBlobCache.get(task.id);
      continue;
    }
    memeActiveFetches++;
    (async()=>{
      try{
        const response=await memeRequest('/'+encodeURIComponent(task.id)+'/image');
        const blob=await response.blob();
        if(!token){return;}
        let url=memeBlobCache.get(task.id);
        if(!url){
          url=URL.createObjectURL(blob);
          memeBlobCache.set(task.id,url);
        }
        if(task.generation===memeGeneration)task.img.src=url;
      }catch(err){
        if(err?.name!=='AbortError'&&token&&task.generation===memeGeneration){
          task.card.append(el('small','预览暂不可用'));
        }
      }finally{
        memeActiveFetches--;
        pumpMemeQueue();
      }
    })();
  }
}
async function memes(){
  resetMemeLoader();
  const generation=memeGeneration;
  const d=await api('memes');
  if(!token||generation!==memeGeneration)return;
  const validIds=new Set(d.items.map(item=>item.id));
  for(const [id,url] of memeBlobCache){
    if(!validIds.has(id)){URL.revokeObjectURL(url);memeBlobCache.delete(id);}
  }
  text('meme-prompt-count','当前已加载到 Prompt 的有效表情数量：'+(d.loaded_prompt_count??0));
  text('meme-state',d.items.length+' / 100 张 · '+(d.items.reduce((n,item)=>n+item.byte_size,0)/1048576).toFixed(2)+' / 50 MiB');
  const listEl=$('meme-list');
  listEl.replaceChildren();
  if(!d.items.length){listEl.append(el('p','还没有表情包。上传一张即可加入可选标签。','muted'));return;}
  const lazyTargets=new WeakMap();
  if('IntersectionObserver' in window){
    memeObserver=new IntersectionObserver(entries=>{
      for(const entry of entries){
        if(!entry.isIntersecting)continue;
        memeObserver.unobserve(entry.target);
        const task=lazyTargets.get(entry.target);
        if(task&&task.generation===memeGeneration){
          memeFetchQueue.push(task);
          pumpMemeQueue();
        }
      }
    },{rootMargin:'200px 0px'});
  }
  for(const item of d.items){
    const card=el('article',undefined,'meme-item'),img=el('img');
    img.alt=item.label;
    img.decoding='async';
    card.append(
      img,
      el('h3',item.label),
      el('small',item.width+' × '+item.height+' · '+Math.ceil(item.byte_size/1024)+' KiB'),
      button('删除',async()=>{
        if(!(await confirmDialog('确认删除表情「'+item.label+'」？白名单将立即更新。',{danger:true,confirmLabel:'删除表情'})))return;
        await api('memes/'+encodeURIComponent(item.id),'DELETE');
        const oldUrl=memeBlobCache.get(item.id);
        if(oldUrl){URL.revokeObjectURL(oldUrl);memeBlobCache.delete(item.id);}
        notice('表情已移除，白名单立即更新');
        await memes();
      },'danger')
    );
    listEl.append(card);
    if(memeBlobCache.has(item.id)){
      img.src=memeBlobCache.get(item.id);
    }else{
      const task={id:item.id,img,card,generation};
      if(memeObserver){
        lazyTargets.set(card,task);
        memeObserver.observe(card);
      }else{
        memeFetchQueue.push(task);
      }
    }
  }
  if(!memeObserver)pumpMemeQueue();
}
$('meme-refresh').addEventListener('click',e=>busy(e.target,memes));
const memeQueue=[],memeLabelOk=s=>/^[A-Za-z0-9_\-\u3400-\u4dbf\u4e00-\u9fff]{1,24}$/.test(s);
function clearMemeQueue(){for(const item of memeQueue)if(item.url)URL.revokeObjectURL(item.url);memeQueue.length=0;$('meme-pending').replaceChildren();}
function renderMemeQueue(){const box=$('meme-pending');box.replaceChildren();memeQueue.forEach((item,idx)=>{const row=el('div',undefined,'meme-q-item'),img=el('img');img.alt=item.file.name;img.src=item.url;const input=el('input');input.maxLength=24;input.placeholder='标签（1～24字，如 疑惑）';input.value=item.label;input.addEventListener('input',()=>{item.label=input.value.trim();});const info=el('small',Math.ceil(item.file.size/1024)+' KiB · '+item.file.name),del=button('移除',()=>{if(item.url)URL.revokeObjectURL(item.url);memeQueue.splice(idx,1);renderMemeQueue();},'danger');row.append(img,input,info,del);box.append(row);});}
function queueMemeFiles(files){const batch=$('meme-batch-label').value.trim();let added=0,skipped=0;for(const file of files||[]){if(!/\.(?:jpe?g|png|gif|webp)$/i.test(file.name)||file.size>5*1024*1024){skipped++;continue;}const stem=file.name.replace(/\.[^.]+$/,'');const ext=(file.name.match(/\.(jpe?g|png|gif|webp)$/i)||['','jpg'])[1].toLowerCase();const label=memeLabelOk(batch)?batch:(memeLabelOk(stem)?stem:'');memeQueue.push({file,label,ext,url:URL.createObjectURL(file)});added++;}renderMemeQueue();if(skipped)notice('已跳过 '+skipped+' 张不支持格式或超过 5 MiB 的图片',true);else if(added)notice('已加入待传列表：'+added+' 张，确认标签后点击批量上传');}
$('meme-file').addEventListener('change',e=>{queueMemeFiles(e.target.files);e.target.value='';});
$('meme-dropzone').addEventListener('click',()=>$('meme-file').click());
$('meme-dropzone').addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();$('meme-file').click();}});
$('meme-dropzone').addEventListener('dragover',e=>{e.preventDefault();$('meme-dropzone').classList.add('drag');});
$('meme-dropzone').addEventListener('dragleave',()=>$('meme-dropzone').classList.remove('drag'));
$('meme-dropzone').addEventListener('drop',e=>{e.preventDefault();$('meme-dropzone').classList.remove('drag');queueMemeFiles(e.dataTransfer?.files);});
$('meme-apply-batch').addEventListener('click',()=>{const batch=$('meme-batch-label').value.trim();if(!memeLabelOk(batch)){notice('标签须为 1～24 个汉字、字母、数字、下划线或短横线',true);return;}memeQueue.forEach(item=>{item.label=batch;});renderMemeQueue();notice('已统一填充标签：'+batch);});
$('meme-clear-queue').addEventListener('click',clearMemeQueue);
$('meme-upload').addEventListener('submit',e=>{e.preventDefault();busy(e.submitter,async()=>{if(!memeQueue.length&&$('meme-file').files.length)queueMemeFiles($('meme-file').files);if(!memeQueue.length)throw Error('请先选择或拖入图片');const bad=memeQueue.find(item=>!memeLabelOk(item.label));if(bad)throw Error('请为「'+bad.file.name+'」填写 1～24 字有效标签（汉字/字母/数字/_/-）');let ok=0,anim=0;while(memeQueue.length){const item=memeQueue[0];const r=await memeRequest('/upload/'+encodeURIComponent(item.label+'.'+item.ext),'POST',item.file),d=await r.json();if(!token)return;if(item.url)URL.revokeObjectURL(item.url);memeQueue.shift();ok++;if(d.first_frame_only)anim++;renderMemeQueue();}notice('已批量上传 '+ok+' 张表情，白名单立即更新'+(anim?'（其中 '+anim+' 张动画保留第一帧）':''));await memes();});});

let songGeneration=0,selectedSongFile=null,pendingSongLearn=null,lastSongStorageState=null;
const songBlobCache=new Map();

const songErrorLabels={
  invalid_song_title:'歌名格式无效（支持 1～60 字多语种文字、数字与常见标点，不可含 / \\ < > " 等特殊符号）',
  invalid_song_meta:'原唱或标签元数据格式无效',
  invalid_song_pitch:'升降调 Pitch 须为 -24～24 之间的整数',
  invalid_drive_file_id:'Drive 原曲文件 ID 格式无效',
  song_source_required:'请选择 Drive inbox 原曲文件，或勾选「未指定文件时自动检索官方原版」',
  invalid_song_audio:'音频文件无效或不受支持',
  song_title_exists:'歌名已存在，请更换歌名或先删除旧曲目',
  song_already_exists:'相同音频切片已在歌单中',
  song_library_full:'歌单曲目数或总容量已达上限',
  song_drive_not_configured:'Google Drive Hakimi_Songs/ 云盘尚未配置（请检查凭据或目录设置）',
  song_drive_unavailable:'Google Drive 云盘暂时不可达，请稍后重试',
  song_drive_invalid_file:'Drive 原曲文件无效或不在 Hakimi_Songs/ 目录内',
  song_drive_invalid_folder_id:'Drive 歌曲根目录 ID 格式无效（禁止填写 root）',
  song_inbox_upload_too_large:'上传原曲超过 100 MiB 上限',
  song_inbox_upload_timeout:'原曲上传超时，请稍后重试',
  compressed_song_upload_forbidden:'原曲上传不支持压缩编码，请使用原始音频直传',
  drive_context_required:'缺少 Drive 短期授权上下文，请先检查歌单云盘配置',
  source_not_found:'未找到干净官方原曲，可手动上传音频到收件箱或私聊发送原曲',
  colab_unconfigured:'Colab 云端算力 Worker 尚未配置，请先在「Colab 云端算力」检查配置',
  colab_disabled:'Colab 云端算力调度当前已关闭，请先开启后再试',
  colab_worker_unavailable:'Colab Worker 当前离线或不可达，请稍后重试',
  colab_gpu_not_allowed:'RVC 翻唱暂不支持该 GPU 型号（已排除不兼容卡型）',
  colab_daily_job_limit:'今日 Colab 作业次数已达上限',
  colab_daily_cu_budget:'今日 Colab CU 预算已耗尽',
  colab_job_limit_reached:'Colab 排队/并发作业已达上限',
  colab_daily_limit_reached:'今日 Colab 作业次数或 CU 预算已达上限',
  colab_inbox_upload_too_large:'上传原曲超过 100 MiB 上限',
  colab_drive_invalid_file:'原曲文件名无效或不在 Hakimi_Songs/ 目录内',
  colab_drive_not_configured:'Google Drive Hakimi_Songs/ 目录尚未初始化或未配置',
  song_upload_too_large:'上传音频超过 100 MiB 上限',
  song_audio_required:'请先选择有效的音频文件'
};
const songStorageSourceLabels={
  sqlite:'后台 SQLite 覆盖（重启保留）',
  admin:'后台 SQLite 覆盖（重启保留）',
  legacy_sqlite:'旧 Colab SQLite 配置兼容迁移',
  env:'环境变量默认配置',
  legacy_env:'环境变量兼容配置（COLAB_DRIVE_SONGS_FOLDER_ID）',
  default:'默认配置（My Drive 自动查找 Hakimi_Songs）',
  auto:'默认配置（My Drive 自动查找 Hakimi_Songs）',
  none:'未配置'
};
function mapSongError(err){
  const msg=String(err?.message||'操作失败');
  const code=msg.split('（')[0].trim();
  return songErrorLabels[code]||msg;
}
function formatByteSize(bytes){
  const n=Number(bytes||0);
  if(!Number.isFinite(n)||n<=0)return '0 B';
  if(n>=1048576)return (n/1048576).toFixed(2)+' MiB';
  if(n>=1024)return Math.ceil(n/1024)+' KiB';
  return n+' B';
}
function formatInboxTime(v){
  if(!v)return '—';
  if(typeof v==='number')return dateTime(v);
  const parsed=Date.parse(String(v));
  if(Number.isFinite(parsed))return new Date(parsed).toLocaleString('zh-CN',{timeZone:displayZone,hour12:false});
  return String(v);
}
function sanitizeSongTitleStem(filename){
  const stem=String(filename||'').replace(/\.[^.]+$/,'');
  const cleaned=stem.replace(/[^\p{L}\p{M}\p{N}\s_\-'’‘&!?！？,，.。、~～+:：·・—–()（）\[\]【】「」『』@#%*=♡♥★☆♪♫]/gu,' ').replace(/\s+/g,' ').trim();
  return cleaned.slice(0,60).trim();
}
const defaultLearnGpuPolicy={default_gpu:'L4',fallback_gpus:['T4'],allowed_gpus:['L4','T4','A100','H100']};
function renderSongLearnGpuPolicy(policy){
  const sel=$('song-learn-gpu');
  if(!sel)return;
  const prev=sel.value;
  const p=policy&&typeof policy==='object'?policy:defaultLearnGpuPolicy;
  const defaultGpu=String(p.default_gpu||defaultLearnGpuPolicy.default_gpu).trim()||'L4';
  const fallbacks=Array.isArray(p.fallback_gpus)?p.fallback_gpus.map(g=>String(g||'').trim()).filter(Boolean):defaultLearnGpuPolicy.fallback_gpus;
  const rawAllowed=Array.isArray(p.allowed_gpus)&&p.allowed_gpus.length?p.allowed_gpus:defaultLearnGpuPolicy.allowed_gpus;
  const allowed=[];
  for(const item of rawAllowed){
    const gpu=String(item||'').trim();
    if(gpu&&!allowed.includes(gpu))allowed.push(gpu);
  }
  const defaultLabel=fallbacks.length?('默认 '+defaultGpu+'（自动回退 '+fallbacks.join(' / ')+'）'):('默认 '+defaultGpu+'（不自动回退）');
  const opts=[selectOption('',defaultLabel),...allowed.map(gpu=>selectOption(gpu,gpu))];
  sel.replaceChildren(...opts);
  sel.value=prev&&allowed.includes(prev)?prev:'';
}
function clearSongLearnForm(){
  $('song-learn-title').value='';
  $('song-learn-artist').value='';
  $('song-learn-tags').value='';
  $('song-learn-pitch').value='0';
  $('song-learn-inbox').value='';
  $('song-learn-gpu').value='';
  $('song-learn-auto-search').checked=true;
}
function showSongLearnResult(job,title,inboxFilename,pitch){
  const jobId=job?.id||job?.job_id||'—';
  const status=job?.status||'queued';
  const prefGpu=String(job?.preferred_gpu||job?.assigned_gpu||'').trim();
  const box=$('song-learn-result');
  const head=el('div',undefined,'sub-card-head');
  head.append(...[
    el('h3','已提交学歌作业：'+title),
    pill('状态：'+status,'ok'),
    prefGpu?pill('首选 GPU：'+prefGpu,'vertex'):null
  ].filter(Boolean));
  const info=el('p','作业 ID：'+jobId+(prefGpu?' · 首选 GPU：'+prefGpu:'')+(inboxFilename?' · 原曲来源：Drive inbox / '+inboxFilename:' · 自动检索官方原版')+' · 升降调：'+(pitch>0?'+'+pitch:String(pitch)),'muted');
  const actions=el('div',undefined,'row');
  const jumpBtn=el('button','前往 Colab 云端算力查看队列 →','secondary');
  jumpBtn.type='button';
  jumpBtn.dataset.openTab='colab-compute';
  actions.append(jumpBtn);
  box.replaceChildren(head,info,actions);
  box.hidden=false;
}
function prefillSongLearn(item){
  const name=String(item?.name||'');
  const title=sanitizeSongTitleStem(name);
  pendingSongLearn={title,inboxFilename:name,inboxFileId:String(item?.id||''),autoSearch:false};
  if(title)$('song-learn-title').value=title;
  const sel=$('song-learn-inbox');
  if(name){
    if(![...sel.options].some(o=>o.value===name)){
      const opt=selectOption(name,name+(item?.size!=null?' ('+formatByteSize(item.size)+')':''));
      opt.dataset.fileId=String(item?.id||'');
      sel.append(opt);
    }
    sel.value=name;
    $('song-learn-auto-search').checked=false;
  }
}
function clearSongPreviews(revokeAll=false){
  songGeneration++;
  if(revokeAll){
    for(const url of songBlobCache.values())URL.revokeObjectURL(url);
    songBlobCache.clear();
  }
  $('song-list').replaceChildren();
  $('song-storage-pills').replaceChildren();
  text('song-cache-state','');
  text('song-learn-colab-hint','');
  text('song-state','');
}
async function songRequest(path,method='GET',body,extraHeaders={}){
  const signal=sessionRequests.signal;
  const r=await fetch('/admin/api/songs'+path,{method,signal,headers:{Authorization:'Bearer '+token,...(body?{'Content-Type':body.type||'application/octet-stream'}:{}),...extraHeaders},body,cache:'no-store',credentials:'omit',redirect:'error'});
  if(!r.ok){
    let detail='HTTP '+r.status;try{detail=(await r.json()).detail||detail;}catch{}
    if(!signal.aborted&&(r.status===401||r.status===403))logout();
    throw Error(songErrorLabels[detail]||detail);
  }
  return r;
}
async function uploadInboxAudio(file){
  const signal=sessionRequests.signal;
  const contentType=(file.type&&/^audio\/(?:mpeg|mp3|wav|x-wav|wave|ogg|opus|mp4|x-m4a|flac)$/i.test(file.type))?file.type:'application/octet-stream';
  const r=await fetch('/admin/api/songs/inbox',{
    method:'POST',
    signal,
    headers:{
      Authorization:'Bearer '+token,
      'Content-Type':contentType,
      'X-Drive-Filename':encodeURIComponent(file.name)
    },
    body:file,
    cache:'no-store',
    credentials:'omit',
    redirect:'error'
  });
  let data=null;
  try{data=await r.json();}catch{}
  if(signal.aborted)throw new DOMException('请求已取消','AbortError');
  if(!r.ok){
    if(r.status===401||r.status===403)logout();
    const detail=data?.detail||('HTTP '+r.status);
    throw Error(mapSongError({message:detail}));
  }
  if(!data)throw Error('服务器未返回有效 JSON');
  return data;
}
function setSelectedSongFile(file){
  if(!file){selectedSongFile=null;text('song-file-name','未选择音频文件');return;}
  if(!/\.(?:mp3|wav|ogg|opus|m4a|flac)$/i.test(file.name)||file.size>100*1024*1024){
    selectedSongFile=null;text('song-file-name','未选择音频文件');
    throw Error('仅支持 mp3/wav/ogg/opus/m4a/flac，且单文件不超过 100 MiB');
  }
  selectedSongFile=file;
  text('song-file-name',file.name+'（'+(file.size/1048576).toFixed(2)+' MiB）');
  if(!$('song-title').value.trim()){
    const stem=sanitizeSongTitleStem(file.name);
    if(stem)$('song-title').value=stem;
  }
}
function mountSongAudio(slot,url,autoplay=false){
  const audio=document.createElement('audio');
  audio.controls=true;
  audio.autoplay=autoplay;
  audio.src=url;
  slot.replaceChildren(audio);
}
function renderSongStorageDetails(storage,opts={}){
  const st=storage&&typeof storage==='object'?storage:{};
  const configured=Boolean(st.configured);
  const src=String(st.source||'default');
  const folderId=String(st.drive_songs_folder_id||'');
  const folders=st.folders&&typeof st.folders==='object'?st.folders:null;
  const err=String(st.error||'');
  lastSongStorageState={configured,source:src,drive_songs_folder_id:folderId,folders,error:err};

  const srcLabel=songStorageSourceLabels[src]||cfgSourceLabels[src]||src;
  const statusLabel=configured?'Google Drive 已独立接管（不依赖 Colab Worker）':'Google Drive 尚未配置';
  text('song-storage-source',statusLabel+' · 配置来源：'+srcLabel+(err?' · 提示：'+mapSongError({message:err}):''));

  const rows=[
    ['配置来源',srcLabel],
    ['配置歌曲根目录 ID (drive_songs_folder_id)',folderId||'未覆写（跟随默认配置）']
  ];
  if(folders){
    for(const [k,label] of [['root','Hakimi_Songs/ (root)'],['models','models/'],['songs','songs/'],['inbox','inbox/']]){
      rows.push([label,folders[k]||'未就绪']);
    }
  }else{
    rows.push(['Drive Hakimi_Songs/ 目录状态',configured?(err?'目录读取异常：'+mapSongError({message:err}):'凭据已就绪，点击「刷新收件箱」探测目录 ID'):'尚未配置 Google Drive OAuth 凭据']);
  }
  $('song-drive-details').replaceChildren(...rows.map(([k,v])=>{
    const row=el('div');row.append(el('dt',k),el('dd',v));return row;
  }));

  if(!opts.preserveInput){
    $('song-drive-folder-id').value=folderId;
    clearDirty('song-storage-form');
  }
  $('song-storage-save').disabled=false;
  $('song-storage-reset').disabled=false;
}
function renderSongInboxSelect(items,errMsg=''){
  const sel=$('song-learn-inbox');
  const prev=pendingSongLearn?pendingSongLearn.inboxFilename:sel.value;
  const emptyHint=errMsg?'不指定文件（Drive inbox 暂不可读）':'不指定文件（Drive inbox 暂无原曲）';
  const first=selectOption('',items.length?'不指定文件（使用下方自动检索）':emptyHint);
  const opts=[first];
  for(const item of items){
    const name=String(item?.name||'').trim();
    if(!name)continue;
    const label=name+(item.size!=null?' ('+formatByteSize(item.size)+')':'');
    const opt=selectOption(name,label);
    opt.dataset.fileId=String(item.id||'');
    opts.push(opt);
  }
  if(prev&&!opts.some(o=>o.value===prev)){
    const fallback=selectOption(prev,prev+'（已预填）');
    fallback.dataset.fileId=pendingSongLearn?.inboxFileId||'';
    opts.push(fallback);
  }
  sel.replaceChildren(...opts);
  if(prev)sel.value=prev;
  if(pendingSongLearn){
    if(pendingSongLearn.title)$('song-learn-title').value=pendingSongLearn.title;
    $('song-learn-auto-search').checked=Boolean(pendingSongLearn.autoSearch);
    pendingSongLearn=null;
  }
}
function renderSongInboxTable(items,errMsg=''){
  if(errMsg){
    $('song-inbox-table').replaceChildren(el('p','Drive inbox 暂不可读：'+errMsg,'empty-state'));
    return;
  }
  table('song-inbox-table',['文件名','大小','修改时间','操作'],(items||[]).map(item=>{
    const ops=el('div',undefined,'row');
    ops.append(
      button('填入学歌',()=>{
        prefillSongLearn(item);
        pendingSongLearn=null;
        const heading=$('song-learn-heading');
        if(heading){
          heading.tabIndex=-1;
          heading.focus({preventScroll:true});
          heading.scrollIntoView({block:'start'});
        }
        notice('已将「'+(item.name||'')+'」填入下方学歌表单');
      },'secondary'),
      button('直接学歌',async()=>{
        const title=await promptDialog('直接发起 Colab 学歌',{
          message:'原曲文件：'+item.name+'（默认升降调 Pitch = 0）',
          hint:'请确认或修改入库歌名（1～60字，提交后将立即提交 RVC 翻唱作业）：',
          defaultValue:sanitizeSongTitleStem(item.name),
          placeholder:'输入歌名（1～60字）',
          confirmLabel:'立即提交作业'
        });
        if(title===null)return;
        const clean=title.trim();
        if(!clean)throw Error('请填写歌名（1～60字）');
        let job;
        try{
          job=await api('songs/learn','POST',{
            title:clean,
            inbox_filename:item.name,
            inbox_file_id:item.id||'',
            pitch:0,
            auto_search:false
          });
        }catch(err){
          if(err?.name==='AbortError')throw err;
          throw Error(mapSongError(err));
        }
        showSongLearnResult(job,clean,item.name,0);
        notice('已发起学歌作业「'+clean+'」（'+(job?.id||job?.job_id||'queued')+'），可在「Colab 云端算力」查看进度');
      },'primary')
    );
    return [
      item.name||'—',
      item.size!=null?formatByteSize(item.size):'—',
      formatInboxTime(item.modified_time),
      ops
    ];
  }));
}
async function loadSongStorage(strict=false){
  try{
    const st=await api('songs/storage');
    if(!token)return null;
    renderSongStorageDetails(st);
    return st;
  }catch(err){
    if(err?.name==='AbortError')throw err;
    if(strict)throw Error(mapSongError(err));
    return null;
  }
}
async function loadSongInbox(strict=false){
  let items=[],errMsg='';
  try{
    const res=await api('songs/inbox');
    if(!token)return 0;
    items=Array.isArray(res)?res:(Array.isArray(res?.items)?res.items:[]);
    if(res&&res.folders&&typeof res.folders==='object'&&lastSongStorageState&&!lastSongStorageState.folders){
      renderSongStorageDetails({...lastSongStorageState,folders:res.folders},{preserveInput:true});
    }
  }catch(err){
    if(err?.name==='AbortError')throw err;
    errMsg=mapSongError(err);
    if(strict){
      renderSongInboxTable([],errMsg);
      renderSongInboxSelect([],errMsg);
      throw Error(errMsg);
    }
  }
  renderSongInboxTable(items,errMsg);
  renderSongInboxSelect(items,errMsg);
  return items.length;
}
function renderSongStorageBar(d){
  const driveCount=Number(d.drive_backed_count??0);
  const cacheBytes=Number(d.cache_bytes??0);
  const cacheMax=Number(d.cache_max_bytes??30*1048576);
  const cachedCount=Number(d.cached_count??0);
  const driveOk=Boolean(d.drive_configured);
  const colabOk=Boolean(d.colab_configured);
  const ratio=cacheMax>0?cacheBytes/cacheMax:0;
  $('song-storage-pills').replaceChildren(
    pill(driveOk?'Drive 云盘已配置':'Drive 云盘未配置',driveOk?'ok':'muted'),
    pill('Drive 主存 '+driveCount+' 首',driveCount>0?'ok':'muted'),
    pill('本地热缓存 '+formatByteSize(cacheBytes)+' / '+formatByteSize(cacheMax)+'（'+cachedCount+' 首）',ratio>=0.9?'warn':cachedCount>0?'paid':'muted'),
    pill(colabOk?'Colab GPU 算力就绪':'Colab GPU 算力未配置',colabOk?'vertex':'muted')
  );
  text('song-cache-state','Drive 云盘（歌单工坊独立直连）：'+(driveOk?'已配置':'未配置')+' · Drive 主存 '+driveCount+' 首 · 本地 LRU 热缓存占用 '+formatByteSize(cacheBytes)+' / '+formatByteSize(cacheMax)+'（共 '+cachedCount+' 首缓存；纯本地上传曲目永不淘汰）· Colab GPU 算力（仅学歌推理）：'+(colabOk?'已配置':'未配置'));
  text('song-learn-colab-hint',(driveOk?'Drive 歌单主存与原曲收件箱已独立就绪；':'提示：Drive 歌单存储尚未配置，收件箱与云盘归档暂不可用；')+(colabOk?'Colab GPU 算力已就绪，提交后将在云端执行人声分离与 RVC 翻唱推理，完成后自动入库并通知 Owner。':'当前 Colab GPU Worker 尚未配置，仍可正常播放歌单、管理 Drive 收件箱或手动上传翻唱成品；如需自动学歌请在「Colab 云端算力」配置 Worker。'));
}
function songStoragePills(item){
  const badges=[];
  if(item.drive_backed)badges.push(pill('Drive 已存档','ok'));
  if(item.drive_full_backed)badges.push(pill('完整版已备份','vertex'));
  if(item.drive_backed){
    badges.push(item.cached_local?pill('本地热缓存','paid'):pill('云端按需拉取','muted'));
  }else{
    badges.push(pill('仅本地','muted'));
  }
  if(item.source_origin==='colab')badges.push(pill('Colab 翻唱','free'));
  return pillRow(...badges);
}
async function songs(){
  songGeneration++;
  const generation=songGeneration;
  const [songsRes,storageRes]=await Promise.allSettled([api('songs'),loadSongStorage(false),loadSongInbox(false)]);
  if(songsRes.status==='rejected')throw songsRes.reason;
  const d=songsRes.value;
  if(!token||generation!==songGeneration)return;
  if(storageRes.status!=='fulfilled'||!storageRes.value){
    renderSongStorageDetails({
      configured:Boolean(d.drive_configured),
      source:d.drive_config_source||'default',
      drive_songs_folder_id:d.drive_songs_folder_id||'',
      folders:d.drive_folders||null,
      error:d.drive_error||''
    });
  }
  const validIds=new Set(d.items.map(item=>item.id));
  for(const [id,url] of songBlobCache){
    if(!validIds.has(id)){URL.revokeObjectURL(url);songBlobCache.delete(id);}
  }
  renderSongStorageBar(d);
  renderSongLearnGpuPolicy(d.learn_gpu_policy);
  text('song-prompt-count','当前已加载到 Prompt 的有效曲目：'+(d.loaded_prompt_count??0));
  text('song-state',d.items.length+' / '+(d.max_songs??300)+' 首 · '+((d.total_bytes??0)/1048576).toFixed(2)+' / '+((d.max_library_bytes??524288000)/1048576).toFixed(0)+' MiB');
  const listEl=$('song-list');
  listEl.replaceChildren();
  if(!d.items.length){listEl.append(el('p','还没有录入翻唱曲目。可以通过上方「Colab 学一首新歌」或下方本地上传音频切片加入歌单。','muted'));return;}
  for(const item of d.items){
    const card=el('article',undefined,'song-item');
    const slot=el('div',undefined,'song-audio-slot');
    if(songBlobCache.has(item.id)){
      mountSongAudio(slot,songBlobCache.get(item.id),false);
    }else{
      const playBtn=button('试听',async()=>{
        let url=songBlobCache.get(item.id);
        if(!url){
          const response=await songRequest('/'+encodeURIComponent(item.id)+'/audio');
          const blob=await response.blob();
          if(!token)return;
          url=songBlobCache.get(item.id);
          if(!url){
            url=URL.createObjectURL(blob);
            songBlobCache.set(item.id,url);
          }
        }
        if(generation===songGeneration)mountSongAudio(slot,url,true);
      },'secondary');
      slot.append(playBtn);
    }
    const meta=[item.artist?('原唱：'+item.artist):'',item.tags?('标签：'+item.tags):''].filter(Boolean).join(' · ');
    const spec=((item.duration_ms||0)/1000).toFixed(1)+' 秒 · '+Math.ceil((item.byte_size||0)/1024)+' KiB';
    card.append(el('h3',item.title));
    if(meta)card.append(el('small',meta));
    card.append(
      el('small',spec),
      songStoragePills(item),
      slot,
      button('删除',async()=>{
        if(!(await confirmDialog('确认删除翻唱曲目「'+item.title+'」？歌单将立即更新。',{danger:true,confirmLabel:'删除曲目'})))return;
        await api('songs/'+encodeURIComponent(item.id),'DELETE');
        const oldUrl=songBlobCache.get(item.id);
        if(oldUrl){URL.revokeObjectURL(oldUrl);songBlobCache.delete(item.id);}
        notice('曲目已移除，歌单立即更新');
        await songs();
      },'danger')
    );
    listEl.append(card);
  }
}
$('song-refresh').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDiscardTabChanges('songs','刷新歌单')))return;
  await songs();
}));
$('song-cache-evict').addEventListener('click',e=>busy(e.target,async()=>{
  const res=await api('songs/cache/evict','POST');
  notice('已清理本地 LRU 热缓存：释放 '+(res.evicted_count??0)+' 首（'+formatByteSize(res.freed_bytes??0)+'），剩余缓存 '+formatByteSize(res.cache_bytes??0));
  await songs();
}));
for(const refreshBtnId of ['song-refresh-inbox','song-learn-refresh-inbox']){
  $(refreshBtnId).addEventListener('click',e=>busy(e.target,async()=>{
    const count=await loadSongInbox(true);
    notice('已刷新 Drive inbox 收件箱（共 '+count+' 首待学原曲）');
  }));
}
$('song-storage-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('song-storage-save'),async()=>{
    const folderId=$('song-drive-folder-id').value.trim();
    if(folderId.toLowerCase()==='root')throw Error(songErrorLabels.song_drive_invalid_folder_id);
    let next;
    try{next=await api('songs/storage','PUT',{drive_songs_folder_id:folderId});}
    catch(err){if(err?.name==='AbortError')throw err;throw Error(mapSongError(err));}
    if(!token)return;
    renderSongStorageDetails(next);
    await loadSongInbox(false);
    notice('歌单 Drive 目录配置已保存并立即生效');
  });
});
$('song-storage-reset').addEventListener('click',e=>busy(e.target,async()=>{
  if(!(await confirmDialog('移除后台覆盖值，恢复默认 Hakimi_Songs/ 云盘目录配置？',{danger:true,confirmLabel:'恢复默认配置'})))return;
  let next;
  try{next=await api('songs/storage','DELETE');}
  catch(err){if(err?.name==='AbortError')throw err;throw Error(mapSongError(err));}
  if(!token)return;
  renderSongStorageDetails(next);
  await loadSongInbox(false);
  notice('已恢复默认歌单 Drive 目录配置');
}));
$('song-inbox-upload').addEventListener('click',e=>busy(e.target,async()=>{
  const file=$('song-inbox-file').files?.[0];
  if(!file)throw Error('请先选择要上传的原曲音频文件');
  if(file.size<=0)throw Error(songErrorLabels.invalid_song_audio);
  if(!/\.(?:mp3|wav|ogg|opus|m4a|flac)$/i.test(file.name))throw Error('仅支持 mp3/wav/ogg/opus/m4a/flac 格式的原曲音频');
  if(file.size>100*1024*1024)throw Error(songErrorLabels.song_inbox_upload_too_large);
  const res=await uploadInboxAudio(file);
  if(!token)return;
  $('song-inbox-file').value='';
  prefillSongLearn({id:res.file_id,name:res.name,size:res.size});
  await loadSongInbox(false);
  notice('已上传原曲「'+res.name+'」到 Drive inbox，可直接发起 Colab 学歌');
}));
$('song-learn-inbox').addEventListener('change',e=>{
  const fname=e.target.value.trim();
  if(fname&&!$('song-learn-title').value.trim()){
    const stem=sanitizeSongTitleStem(fname);
    if(stem)$('song-learn-title').value=stem;
  }
});
$('song-learn-clear').addEventListener('click',()=>{
  pendingSongLearn=null;
  clearSongLearnForm();
  $('song-learn-result').hidden=true;
  $('song-learn-result').replaceChildren();
});
$('song-learn-form').addEventListener('submit',e=>{
  e.preventDefault();
  busy($('song-learn-submit'),async()=>{
    const title=$('song-learn-title').value.trim();
    if(!title)throw Error('请填写歌名（1～60字）');
    const pitch=Number($('song-learn-pitch').value||'0');
    if(!Number.isInteger(pitch)||pitch<-24||pitch>24)throw Error(songErrorLabels.invalid_song_pitch);
    const sel=$('song-learn-inbox');
    const inboxFilename=sel.value.trim();
    const inboxFileId=sel.selectedOptions?.[0]?.dataset?.fileId||'';
    const autoSearch=$('song-learn-auto-search').checked;
    if(!inboxFilename&&!autoSearch)throw Error(songErrorLabels.song_source_required);
    const chosenGpu=$('song-learn-gpu').value.trim();
    const payload={
      title,
      artist:$('song-learn-artist').value.trim(),
      tags:$('song-learn-tags').value.trim(),
      pitch,
      auto_search:autoSearch
    };
    if(chosenGpu)payload.gpu=chosenGpu;
    if(inboxFilename){
      payload.inbox_filename=inboxFilename;
      if(inboxFileId)payload.inbox_file_id=inboxFileId;
    }
    let job;
    try{job=await api('songs/learn','POST',payload);}
    catch(err){if(err?.name==='AbortError')throw err;throw Error(mapSongError(err));}
    const jobId=job?.id||job?.job_id||'—';
    showSongLearnResult(job,title,inboxFilename,pitch);
    clearSongLearnForm();
    notice('已提交学歌任务「'+title+'」（作业 '+jobId+'），可在「Colab 云端算力」查看作业进度');
    await songs();
  });
});
$('song-file').addEventListener('change',e=>{try{setSelectedSongFile(e.target.files?.[0]||null);}catch(err){e.target.value='';notice(err.message,true);}});
$('song-clear-file').addEventListener('click',()=>{$('song-file').value='';setSelectedSongFile(null);});
$('song-dropzone').addEventListener('click',()=>$('song-file').click());
$('song-dropzone').addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();$('song-file').click();}});
$('song-dropzone').addEventListener('dragover',e=>{e.preventDefault();$('song-dropzone').classList.add('drag');});
$('song-dropzone').addEventListener('dragleave',()=>$('song-dropzone').classList.remove('drag'));
$('song-dropzone').addEventListener('drop',e=>{e.preventDefault();$('song-dropzone').classList.remove('drag');try{setSelectedSongFile(e.dataTransfer?.files?.[0]||null);}catch(err){notice(err.message,true);}});
$('song-upload').addEventListener('submit',e=>{
  e.preventDefault();
  busy(e.submitter,async()=>{
    const file=selectedSongFile||$('song-file').files?.[0];
    if(!file)throw Error('请先选择或拖入音频文件');
    const title=$('song-title').value.trim();
    if(!title)throw Error('请填写歌名（1～60字）');
    const startSec=Number($('song-start').value||'0');
    const durSec=Number($('song-duration').value||'50');
    if(!Number.isFinite(startSec)||startSec<0||startSec>600)throw Error('起始秒数须在 0～600 之间');
    if(!Number.isFinite(durSec)||durSec<5||durSec>58)throw Error('截取时长须在 5～58 秒之间');
    await songRequest('/upload','POST',file,{
      'X-Song-Title':encodeURIComponent(title),
      'X-Song-Artist':encodeURIComponent($('song-artist').value.trim()),
      'X-Song-Tags':encodeURIComponent($('song-tags').value.trim()),
      'X-Song-Filename':encodeURIComponent(file.name),
      'X-Song-Start':String(startSec),
      'X-Song-Duration':String(durSec),
    });
    $('song-file').value='';setSelectedSongFile(null);
    $('song-title').value='';$('song-artist').value='';$('song-tags').value='';
    notice('已上传并切片入库：'+title);
    await songs();
  });
});

trackDirty('bili-policy','小号巡逻设置');
trackDirty('video-watch-policy','原片分析设置');
trackDirty('song-storage-form','歌单云盘目录');

onLogout(()=>{
  clearMemePreviews(true);
  clearMemeQueue();
  clearSongPreviews(true);
  selectedSongFile=null;
  pendingSongLearn=null;
  lastSongStorageState=null;
  clearSongLearnForm();
  $('song-learn-inbox').replaceChildren(selectOption('','不指定文件（使用下方自动检索）'));
  $('song-learn-gpu').replaceChildren(selectOption('','默认 L4（自动回退 T4）'));
  $('song-learn-gpu').value='';
  $('song-learn-result').hidden=true;
  $('song-learn-result').replaceChildren();
  $('song-drive-details').replaceChildren();
  $('song-inbox-table').replaceChildren();
  $('song-drive-folder-id').value='';
  $('song-storage-save').disabled=true;
  $('song-storage-reset').disabled=true;
  text('song-storage-source','');
  text('song-file-name','未选择音频文件');
  text('meme-prompt-count','当前已加载到 Prompt 的有效表情数量：0');
  text('song-prompt-count','当前已加载到 Prompt 的有效曲目：0');
  for(const id of ['bili-state','bili-watch-history','bili-credential-state','video-watch-state'])text(id,'');
  for(const id of ['bili-runs','video-watch-attempts'])$(id).replaceChildren();
});
