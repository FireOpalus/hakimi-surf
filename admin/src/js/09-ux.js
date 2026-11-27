// ==================== 09 · 体验增强：页内分区导航、吸底保存条、字段提示与说明折叠 ====================
const prefersReducedMotion=()=>Boolean(window.matchMedia?.('(prefers-reduced-motion: reduce)').matches);

/* ---------- 页内分区导航（长页面锚点条；带 data-section-group 的页面切换为二级分组） ---------- */
const SECTION_GROUP_LABELS={cloud:'云资源总览',calls:'调用明细',billing:'余额与对账',budget:'预算策略'};
const sectionNavs=new Map();

function sectionTitle(card){
  const heading=card.matches('details')?card.querySelector(':scope > summary'):card.querySelector('h2');
  if(heading?.dataset.navLabel)return heading.dataset.navLabel;
  return (heading?.textContent||'').trim().replace(/\s*[（(][^（）()]*[）)]\s*$/,'').trim();
}
function sectionCards(panel){
  const cards=[];
  for(const child of panel.children){
    if(child.matches('.card'))cards.push(child);
    else if(child.matches('.grid'))cards.push(...[...child.children].filter(c=>c.matches('.card')));
  }
  return cards.filter(card=>sectionTitle(card));
}
function markSectionChip(state,chip){
  for(const b of state.buttons){
    if(b===chip)b.setAttribute('aria-current','true');else b.removeAttribute('aria-current');
  }
  const bar=state.bar;
  if(chip&&bar.scrollWidth>bar.clientWidth){
    bar.scrollLeft=chip.offsetLeft-(bar.clientWidth-chip.offsetWidth)/2;
  }
}
function showSectionGroup(state,key){
  state.active=key;
  for(const card of state.cards)card.classList.toggle('section-off',card.dataset.sectionGroup!==key);
  for(const b of state.buttons)b.setAttribute('aria-pressed',String(b.dataset.sectionKey===key));
}
function buildSectionNav(panel){
  const cards=sectionCards(panel);
  const grouped=cards.some(card=>card.dataset.sectionGroup);
  if(!grouped&&cards.length<3)return;
  const bar=el('div',undefined,'section-nav'+(grouped?' section-nav-groups':''));
  bar.setAttribute('role','toolbar');
  bar.setAttribute('aria-label',grouped?'切换本页分组':'跳转到本页分区');
  const state={panel,bar,cards,buttons:[],grouped,active:null};
  if(grouped){
    const keys=[...new Set(cards.map(card=>card.dataset.sectionGroup).filter(Boolean))];
    for(const key of keys){
      const chip=el('button',SECTION_GROUP_LABELS[key]||key,'section-chip');
      chip.type='button';chip.dataset.sectionKey=key;
      chip.addEventListener('click',()=>{
        showSectionGroup(state,key);
        window.scrollTo({top:0,behavior:'instant'});
      });
      state.buttons.push(chip);
    }
    bar.append(...state.buttons);
    panel.prepend(bar);
    showSectionGroup(state,keys[0]);
  }else{
    for(const card of cards){
      const chip=el('button',sectionTitle(card),'section-chip');
      chip.type='button';chip.title=chip.textContent;
      chip.addEventListener('click',()=>{
        if(card.matches('details'))card.open=true;
        card.scrollIntoView({behavior:prefersReducedMotion()?'auto':'smooth',block:'start'});
        markSectionChip(state,chip);
      });
      state.buttons.push(chip);
    }
    bar.append(...state.buttons);
    panel.prepend(bar);
    if('IntersectionObserver' in window){
      const visible=new Set();
      const observer=new IntersectionObserver(entries=>{
        for(const entry of entries){
          if(entry.isIntersecting)visible.add(entry.target);else visible.delete(entry.target);
        }
        const first=cards.find(card=>visible.has(card));
        if(first)markSectionChip(state,state.buttons[cards.indexOf(first)]);
      },{rootMargin:'-150px 0px -55% 0px'});
      cards.forEach(card=>observer.observe(card));
    }
  }
  sectionNavs.set(panel.id,state);
}
// 查找器跳到被分组隐藏的标题时，先切换到它所在的分组。
function revealSectionFor(heading){
  const panel=heading?.closest?.('section.tab');
  const state=panel&&sectionNavs.get(panel.id);
  if(!state?.grouped)return;
  const card=heading.closest('[data-section-group]');
  if(card&&card.classList.contains('section-off'))showSectionGroup(state,card.dataset.sectionGroup);
}

/* ---------- 吸底保存条：当前页有未保存表单时浮出，就近保存或放弃 ---------- */
let saveBarSignature='';
function renderSaveBar(){
  const bar=$('save-bar');
  const entries=token?[...dirtyRegistry.values()].filter(entry=>entry.dirty&&entry.tabId===tab):[];
  const signature=tab+'|'+entries.map(entry=>entry.label).join('|');
  if(signature===saveBarSignature&&bar.hidden===!entries.length)return;
  saveBarSignature=signature;
  if(!entries.length){
    bar.hidden=true;document.body.classList.remove('has-save-bar');return;
  }
  text('save-bar-text',entries.length===1
    ?'「'+entries[0].label+'」有未保存的修改'
    :'有 '+entries.length+' 处未保存的修改：'+entries.map(entry=>entry.label).join('、'));
  const discard=el('button','放弃修改','secondary');
  discard.type='button';
  discard.addEventListener('click',async()=>{
    const forms=entries.map(entry=>entry.form);
    if(!(await confirmDiscardTabChanges(tab,'重新载入本页')))return;
    for(const form of forms)if(typeof form.reset==='function')form.reset();
    busy(discard,()=>refreshTab());
  });
  const saves=entries.map(entry=>{
    const submit=entry.form.querySelector('button[type=submit]');
    const b=el('button',entries.length===1?'保存修改':'保存「'+entry.label+'」','primary');
    b.type='button';
    b.addEventListener('click',()=>{
      openEnclosingDetails(entry.form);
      if(submit&&!submit.disabled&&typeof entry.form.requestSubmit==='function'){
        entry.form.requestSubmit(submit);
      }else{
        entry.form.scrollIntoView({block:'center',behavior:prefersReducedMotion()?'auto':'smooth'});
        entry.form.querySelector('input:not([type=hidden]),select,textarea')?.focus({preventScroll:true});
      }
    });
    return b;
  });
  $('save-bar-actions').replaceChildren(discard,...saves);
  bar.hidden=false;document.body.classList.add('has-save-bar');
}
onDirtyChange(renderSaveBar);
onLogout(()=>{saveBarSignature='';renderSaveBar();});

/* ---------- 字段提示：把标签尾部的取值范围、默认值和内部键名移到输入框下方的弱化提示 ---------- */
const HINT_KEY_RE=/^(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)+|[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)$/;
const HINT_TOKEN_RE=/([a-z][a-z0-9]*(?:_[a-z0-9]+)+|[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)/;
let hintSeq=0;
function hintWorthy(inner,toggle){
  const value=inner.trim();
  if(HINT_KEY_RE.test(value))return true;
  if(toggle||value.length<=4)return false;
  return /[a-z]_[a-z]|\d\s*[–~～-]\s*[\d.]|默认|上限|=|留空|必填|最多|最少|，/.test(value);
}
function splitLabelText(raw,toggle){
  let main=raw.replace(/\s+$/,'');
  const hints=[];
  for(let guard=0;guard<3;guard++){
    const match=main.match(/[（(]([^（）()]{1,90})[）)]$/);
    if(match&&toggle){
      // 开关标签：只把开头的内部键名移出，保留括号里的中文解释。
      const keyed=match[1].trim().match(/^([a-z][a-z0-9]*(?:_[a-z0-9]+)*)\s*(?:[，,]\s*(.+))?$/);
      if(keyed&&(keyed[1].includes('_')||keyed[1].length>=3)){
        hints.unshift(keyed[1]);
        main=main.slice(0,match.index)+(keyed[2]?'（'+keyed[2]+'）':'');
        break;
      }
    }
    if(!match||!hintWorthy(match[1],toggle))break;
    hints.unshift(match[1].trim());
    main=main.slice(0,match.index).replace(/\s+$/,'');
  }
  if(!toggle){
    const bare=main.match(/([\u4e00-\u9fff）])\s+([a-z][a-z0-9]*(?:_[a-z0-9]+)*)$/);
    if(bare&&bare[2].length>=3){
      hints.unshift(bare[2]);
      main=main.slice(0,bare.index+bare[1].length);
    }
  }
  return {main,hints};
}
function appendHintContent(hint,parts){
  parts.forEach((part,index)=>{
    if(index)hint.append(' · ');
    for(const piece of part.split(HINT_TOKEN_RE)){
      if(!piece)continue;
      if(HINT_KEY_RE.test(piece)||(parts.length===1&&/^[a-z]{3,}$/.test(piece)))hint.append(el('code',piece));else hint.append(piece);
    }
  });
}
function enhanceLabel(label){
  if(label.dataset.hintEnhanced==='1')return;
  if(label.closest('.login, .finder, .weather-methods, .scope-checks'))return;
  label.dataset.hintEnhanced='1';
  let control=label.querySelector('input,select,textarea');
  const external=!control&&label.htmlFor?document.getElementById(label.htmlFor):null;
  const toggle=Boolean(control&&(control.type==='checkbox'||control.type==='radio'));
  const textNodes=[...label.childNodes].filter(node=>node.nodeType===3&&node.data.trim());
  if(!textNodes.length)return;
  let node;
  if(toggle)node=textNodes[textNodes.length-1];
  else if(control)node=textNodes.filter(n=>n.compareDocumentPosition(control)&Node.DOCUMENT_POSITION_FOLLOWING).pop();
  else node=textNodes[textNodes.length-1];
  if(!node)return;
  const {main,hints}=splitLabelText(node.data,toggle);
  if(!hints.length||!main.trim())return;
  node.data=main;
  label.querySelector(':scope > .field-hint')?.remove();
  const hint=el('small',undefined,'field-hint');
  hint.id='field-hint-'+(++hintSeq);
  hint.setAttribute('aria-hidden','true');
  appendHintContent(hint,hints);
  if(control&&!toggle&&control.parentNode===label)control.after(hint);else label.append(hint);
  control=control||external;
  if(control){
    const ids=new Set((control.getAttribute('aria-describedby')||'').split(/\s+/).filter(Boolean));
    ids.add(hint.id);
    control.setAttribute('aria-describedby',[...ids].join(' '));
  }
}
function enhanceLabels(root){
  if(!root||root.nodeType!==1)return;
  if(root.matches('label'))enhanceLabel(root);
  root.querySelectorAll('label').forEach(enhanceLabel);
}

/* ---------- 说明折叠：长段说明只保留首句，其余收进「展开说明」 ---------- */
function foldLongHelp(root){
  for(const p of root.querySelectorAll('section.tab p.muted:not([id])')){
    if(p.children.length||p.closest('.help-fold, .table, td, .tg-privacy-box'))continue;
    const raw=p.textContent.trim();
    if(raw.length<110)continue;
    const boundary=[...raw.matchAll(/[。；]/g)].map(m=>m.index).find(index=>index>=24);
    if(boundary===undefined)continue;
    const rest=raw.slice(boundary+1).trim();
    if(rest.length<30)continue;
    p.textContent=raw.slice(0,boundary+1);
    const fold=el('details',undefined,'help-fold');
    fold.append(el('summary','展开说明'),el('p',rest,'muted'));
    p.after(fold);
  }
}

/* ---------- 启动 ---------- */
document.querySelectorAll('#app section.tab').forEach(buildSectionNav);
foldLongHelp($('app'));
enhanceLabels($('app'));
if('MutationObserver' in window){
  const labelObserver=new MutationObserver(records=>{
    for(const record of records)for(const node of record.addedNodes)enhanceLabels(node);
  });
  for(const form of document.querySelectorAll('#app form')){
    labelObserver.observe(form,{childList:true,subtree:true});
  }
}
