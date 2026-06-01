/* ===== NAV scroll state ===== */
(function(){
  var nav=document.getElementById('nav'); if(!nav) return;
  function onScroll(){ nav.classList.toggle('scrolled', window.scrollY>24); }
  onScroll(); window.addEventListener('scroll',onScroll,{passive:true});
})();

/* ===== MOBILE MENU (hamburger) ===== */
(function(){
  var b=document.getElementById('navBurger'), m=document.getElementById('mobileMenu'), c=document.getElementById('mmClose');
  if(!b||!m) return;
  function open(){ m.classList.add('open'); document.body.style.overflow='hidden'; }
  function close(){ m.classList.remove('open'); document.body.style.overflow=''; }
  b.addEventListener('click',open);
  if(c) c.addEventListener('click',close);
  m.querySelectorAll('a').forEach(function(a){ a.addEventListener('click',close); });
  window.addEventListener('keydown',function(e){ if(e.key==='Escape') close(); });
})();

/* ===== Reveal on scroll ===== */
(function(){
  var io=new IntersectionObserver(function(es){
    es.forEach(function(e){ if(e.isIntersecting){ e.target.classList.add('in'); io.unobserve(e.target);} });
  },{threshold:.14,rootMargin:'0px 0px -8% 0px'});
  document.querySelectorAll('.reveal:not(.in)').forEach(function(el){ if(!el.closest('.hero')) io.observe(el); });
})();

/* ===== LOGIN → DASHBOARD TRANSITION ===== */
/* The progress effect no longer plays on page load anywhere. It runs ONLY when the
   login form succeeds, as a transition into the dashboard. Exposed as window.zyEnterDashboard. */
window.zyEnterDashboard = function(dest){
  var ld=document.getElementById('loader'); if(!ld){ window.location.href=dest; return; }
  var pctEl=document.getElementById('ldPct'), progEl=document.getElementById('ldProg'), loadEl=document.getElementById('ldLoad');
  document.body.classList.add('loading');
  ld.style.display='block';
  var C=364.4, done=false;
  if(loadEl){ loadEl.textContent='entering'; }
  var dotT=setInterval(function(){ if(!loadEl) return; var n=(loadEl.textContent.replace('entering','').match(/\./g)||[]).length; loadEl.textContent='entering'+'.'.repeat((n+1)%4); },300);
  var reduce=window.matchMedia('(prefers-reduced-motion:reduce)').matches;
  var startT=performance.now(), DUR=reduce?500:1500;
  function ease(x){ return 1-Math.pow(1-x,3); }
  function go(){ window.location.href=dest; }
  function tick(now){
    var k=Math.min(1,(now-startT)/DUR), v=Math.round(ease(k)*100);
    if(pctEl) pctEl.textContent=v+'%';
    if(progEl) progEl.setAttribute('stroke-dashoffset', C*(1-v/100));
    if(k<1){ requestAnimationFrame(tick); } else if(!done){ finish(); }
  }
  function finish(){
    if(done) return; done=true; clearInterval(dotT);
    if(pctEl) pctEl.textContent='100%'; if(progEl) progEl.setAttribute('stroke-dashoffset',0);
    setTimeout(go,420);
  }
  ld.addEventListener('click',function(){ if(!done){ done=true; clearInterval(dotT); go(); } });
  requestAnimationFrame(tick);
};

/* ===== LIVE WALLPAPER ===== */
(function(){
  var c=document.getElementById('wp'); if(!c) return;
  var ctx=c.getContext('2d');
  var reduce=window.matchMedia('(prefers-reduced-motion:reduce)').matches;
  var W=0,H=0,DPR=Math.min(window.devicePixelRatio||1,2);
  var orbs=[
    {x:.18,y:.30,r:.45,col:[21,101,192],a:.16,sx:.018,sy:.013,p:0},
    {x:.74,y:.24,r:.40,col:[30,136,229],a:.13,sx:.014,sy:.020,p:1.7},
    {x:.55,y:.62,r:.42,col:[46,125,50],a:.11,sx:.020,sy:.012,p:3.1},
    {x:.90,y:.70,r:.34,col:[230,81,0],a:.07,sx:.012,sy:.017,p:4.6}
  ];
  var pts=[];
  var waves=[
    {base:.46, amp:54, len:.0017, sp:.020, col:'rgba(21,101,192,0.065)'},
    {base:.58, amp:78, len:.0012, sp:.015, col:'rgba(30,136,229,0.07)'},
    {base:.70, amp:66, len:.0021, sp:.027, col:'rgba(46,125,50,0.055)'},
    {base:.82, amp:58, len:.0015, sp:.012, col:'rgba(230,81,0,0.035)'}
  ];
  function reset(){
    W=c.clientWidth; H=c.clientHeight;
    c.width=W*DPR; c.height=H*DPR; ctx.setTransform(DPR,0,0,DPR,0,0);
    var n=Math.round(Math.min(64,Math.max(28,W/26)));
    pts=[];
    for(var i=0;i<n;i++){
      pts.push({x:Math.random()*W,y:Math.random()*H*0.82,
        vx:(Math.random()-.5)*.16,vy:(Math.random()-.5)*.16,
        r:Math.random()*1.6+0.8});
    }
  }
  function drawOrbs(t){
    for(var i=0;i<orbs.length;i++){
      var o=orbs[i];
      var cx=(o.x+Math.sin(t*o.sx+o.p)*.05)*W;
      var cy=(o.y+Math.cos(t*o.sy+o.p)*.05)*H;
      var rad=o.r*Math.max(W,H);
      var g=ctx.createRadialGradient(cx,cy,0,cx,cy,rad);
      g.addColorStop(0,'rgba('+o.col[0]+','+o.col[1]+','+o.col[2]+','+o.a+')');
      g.addColorStop(1,'rgba('+o.col[0]+','+o.col[1]+','+o.col[2]+',0)');
      ctx.fillStyle=g; ctx.fillRect(0,0,W,H);
    }
  }
  function drawWaves(t){
    for(var k=0;k<waves.length;k++){
      var w=waves[k];
      ctx.beginPath(); ctx.moveTo(0,H);
      for(var x=0;x<=W;x+=14){
        var y=w.base*H + Math.sin(x*w.len + t*w.sp)*w.amp + Math.sin(x*w.len*2.3 + t*w.sp*0.6)*w.amp*0.32;
        ctx.lineTo(x,y);
      }
      ctx.lineTo(W,H); ctx.closePath();
      var g=ctx.createLinearGradient(0,w.base*H-w.amp,0,H);
      g.addColorStop(0,w.col); g.addColorStop(1,'rgba(255,255,255,0)');
      ctx.fillStyle=g; ctx.fill();
    }
  }
  function drawNet(){
    var maxd=128;
    for(var i=0;i<pts.length;i++){
      var p=pts[i];
      for(var j=i+1;j<pts.length;j++){
        var q=pts[j], dx=p.x-q.x, dy=p.y-q.y, d=Math.sqrt(dx*dx+dy*dy);
        if(d<maxd){
          ctx.strokeStyle='rgba(21,101,192,'+(0.10*(1-d/maxd))+')';
          ctx.lineWidth=1; ctx.beginPath(); ctx.moveTo(p.x,p.y); ctx.lineTo(q.x,q.y); ctx.stroke();
        }
      }
      ctx.fillStyle='rgba(21,101,192,0.42)';
      ctx.beginPath(); ctx.arc(p.x,p.y,p.r,0,6.2832); ctx.fill();
    }
  }
  function step(p){
    p.x+=p.vx; p.y+=p.vy;
    if(p.x<0||p.x>W)p.vx*=-1;
    if(p.y<0||p.y>H*0.84)p.vy*=-1;
  }
  var t=0,raf;
  function frame(){
    ctx.clearRect(0,0,W,H);
    drawOrbs(t); drawWaves(t);
    if(document.body.classList.contains('motion')) for(var i=0;i<pts.length;i++) step(pts[i]);
    drawNet();
    t+=document.body.classList.contains('motion')?1:0;
    raf=requestAnimationFrame(frame);
  }
  function start(){ reset(); cancelAnimationFrame(raf); if(reduce){ ctx.clearRect(0,0,W,H); drawOrbs(0); drawWaves(0); drawNet(); } else { frame(); } }
  window.addEventListener('resize',function(){ clearTimeout(window.__wpz); window.__wpz=setTimeout(start,180); });
  start();
})();
