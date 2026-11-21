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

let songGeneration=0,selectedSongFile=null;
const songBlobCache=new Map();

function clearSongPreviews(revokeAll=false){
  songGeneration++;
  if(revokeAll){
    for(const url of songBlobCache.values())URL.revokeObjectURL(url);
    songBlobCache.clear();
  }
  $('song-list').replaceChildren();
  text('song-state','');
}
async function songRequest(path,method='GET',body,extraHeaders={}){
  const signal=sessionRequests.signal;
  const r=await fetch('/admin/api/songs'+path,{method,signal,headers:{Authorization:'Bearer '+token,...(body?{'Content-Type':body.type||'application/octet-stream'}:{}),...extraHeaders},body,cache:'no-store',credentials:'omit',redirect:'error'});
  if(!r.ok){
    let detail='HTTP '+r.status;try{detail=(await r.json()).detail||detail;}catch{}
    if(!signal.aborted&&(r.status===401||r.status===403))logout();
    throw Error(detail);
  }
  return r;
}
function setSelectedSongFile(file){
  if(!file){selectedSongFile=null;text('song-file-name','未选择音频文件');return;}
  if(!/\.(?:mp3|wav|ogg|opus|m4a|flac)$/i.test(file.name)||file.size>12*1024*1024){
    selectedSongFile=null;text('song-file-name','未选择音频文件');
    throw Error('仅支持 mp3/wav/ogg/opus/m4a/flac，且单文件不超过 12 MiB');
  }
  selectedSongFile=file;
  text('song-file-name',file.name+'（'+(file.size/1048576).toFixed(2)+' MiB）');
  if(!$('song-title').value.trim()){
    const stem=file.name.replace(/\.[^.]+$/,'').replace(/\s+/g,' ').trim().slice(0,40);
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
async function songs(){
  songGeneration++;
  const generation=songGeneration;
  const d=await api('songs');
  if(!token||generation!==songGeneration)return;
  const validIds=new Set(d.items.map(item=>item.id));
  for(const [id,url] of songBlobCache){
    if(!validIds.has(id)){URL.revokeObjectURL(url);songBlobCache.delete(id);}
  }
  text('song-prompt-count','当前已加载到 Prompt 的有效曲目：'+(d.loaded_prompt_count??0));
  text('song-state',d.items.length+' / '+(d.max_songs??100)+' 首 · '+((d.total_bytes??0)/1048576).toFixed(2)+' / '+((d.max_library_bytes??104857600)/1048576).toFixed(0)+' MiB');
  const listEl=$('song-list');
  listEl.replaceChildren();
  if(!d.items.length){listEl.append(el('p','还没有录入翻唱曲目。上传一段音频切片即可让猫猫学会唱它。','muted'));return;}
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
$('song-refresh').addEventListener('click',e=>busy(e.target,songs));
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
    if(!title)throw Error('请填写歌名（1～40字）');
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

onLogout(()=>{
  clearMemePreviews(true);
  clearMemeQueue();
  clearSongPreviews(true);
  selectedSongFile=null;
  text('song-file-name','未选择音频文件');
  text('meme-prompt-count','当前已加载到 Prompt 的有效表情数量：0');
  text('song-prompt-count','当前已加载到 Prompt 的有效曲目：0');
  for(const id of ['bili-state','bili-watch-history','bili-credential-state','video-watch-state'])text(id,'');
  for(const id of ['bili-runs','video-watch-attempts'])$(id).replaceChildren();
});
