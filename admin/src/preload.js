// 在 <head> 中同步执行：首帧渲染前确定主题，避免深色偏好用户刷新时闪一下浅色。
(()=>{
  let saved=null;
  try{saved=localStorage.getItem('hakimi-theme');}catch{}
  const dark=saved?saved==='dark':Boolean(window.matchMedia?.('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.theme=dark?'dark':'light';
})();
