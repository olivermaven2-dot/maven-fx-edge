const WS_URL="wss://ws.derivws.com/websockets/v3?app_id=1089";
const MIN_SAMPLE=120, MAX_HISTORY=2500;
const PAIRS={OVER:{1:8,2:7,3:6,4:5,5:4,6:3,7:2,8:1},UNDER:{1:8,2:7,3:6,4:5,5:4,6:3,7:2,8:1}};
const state={ws:null,markets:[],market:"",digit:1,direction:"OVER",buffers:new Map(),stream:[],ticks:0,price:null};

const $=id=>document.getElementById(id);
const key=()=>state.market;
function buffer(){if(!state.buffers.has(key()))state.buffers.set(key,[]);return state.buffers.get(key())}
function setStatus(text,cls){$("connection").textContent=text;$("connection").className="status "+cls}
function setText(id,v){$(id).textContent=v}
function mean(a){return a.length?a.reduce((x,y)=>x+y,0)/a.length:0}
function rate(a,p){return a.length?100*a.filter(p).length/a.length:0}
function fmt(n){return Number.isFinite(n)?n.toFixed(1)+"%":"—"}
function eventRate(a,d,dir){return rate(a,x=>dir==="OVER"?x>d:x<d)}
function zScore(p,n){if(!n||p<=0||p>=1)return 0;return (p-.5)/Math.sqrt(.25/n)}
function std(a){if(a.length<2)return 0;const m=mean(a);return Math.sqrt(mean(a.map(x=>(x-m)**2)))}
function transition(a,d,dir){const idx=[];for(let i=0;i<a.length-1;i++)if(a[i]===d)idx.push(a[i+1]);return idx.length?eventRate(idx,d,dir):null}
function stable(a){if(a.length<100)return 0;const half=Math.floor(a.length/2),x=a.slice(-half),y=a.slice(-half*2,-half);return Math.max(0,100-Math.abs(eventRate(x,state.digit,state.direction)-eventRate(y,state.digit,state.direction))*2)}
function regime(a){if(a.length<100)return "INSUFFICIENT";const s=a.slice(-100),l=a.slice(-400);const shift=Math.abs(eventRate(s,state.digit,state.direction)-eventRate(l,state.digit,state.direction));return shift>=8?"SHIFTING":shift>=4?"CHANGING":"STABLE"}
function dataQuality(a){if(a.length<MIN_SAMPLE)return "INSUFFICIENT";const recent=a.slice(-50);const unique=new Set(recent).size;return unique>=6?"GOOD":"NARROW"}
function pairRate(a){const pd=PAIRS[state.direction]?.[state.digit];if(pd===undefined)return null;return eventRate(a,pd,state.direction==="OVER"?"UNDER":"OVER")}
function renderDistribution(a){const counts=Array(10).fill(0);a.forEach(d=>{if(Number.isInteger(d)&&d>=0&&d<=9)counts[d]++});const max=Math.max(...counts,1);$("distribution").innerHTML=counts.map((c,d)=>`<div class="bar-wrap"><b>${fmt(c*100/(a.length||1))}</b><div class="bar" style="height:${Math.max(2,c/max*105)}px"></div><span>${d}</span></div>`).join("")}
function analyze(){
  const a=buffer(), n=a.length, d=state.digit, dir=state.direction;
  setText("signalContract",dir+" "+d);
  if(!n){setText("signalReason","Waiting for live ticks.");return}
  renderDistribution(a);
  const q=dataQuality(a);
  if(n<MIN_SAMPLE){setText("probability",`Sample ${n}/${MIN_SAMPLE}`);setText("paired","Collecting paired evidence…");setText("transitions","Collecting transitions…");setText("regime","INSUFFICIENT");setText("stability","INSUFFICIENT");setText("validation","Need more out-of-sample data.");setSignal("WAIT","Need more live observations.");return}
  const short=a.slice(-100),long=a.slice(-400),train=a.slice(0,Math.floor(n*.7)),test=a.slice(Math.floor(n*.7));
  const p=eventRate(short,d,dir), lp=eventRate(long,d,dir), tp=eventRate(train,d,dir), vp=eventRate(test,d,dir);
  const pr=pairRate(short), tr=transition(a,d,dir), rg=regime(a), st=stable(a);
  const consistency=Math.max(0,100-Math.abs(tp-vp)*2);
  const z=Math.abs(zScore(p/100,short.length));
  const probabilityGate=p>=52 && p>=lp-2;
  const pairGate=pr===null?true:(dir==="OVER"?pr<50:pr<50);
  const transitionGate=tr===null?false:tr>=52;
  const validationGate=vp>=50 && consistency>=80;
  const stabilityGate=st>=70 && rg!=="SHIFTING";
  const contradiction=[probabilityGate,pairGate,transitionGate,validationGate,stabilityGate].filter(Boolean).length;
  let status="WAIT",reason="Evidence is not yet strong enough.";
  if(q==="NARROW"||rg==="SHIFTING")status="AVOID",reason="Recent digit distribution is too narrow or changing rapidly.";
  else if(contradiction>=5 && z>=1.2)status="STRONG SIGNAL",reason="Independent probability, paired, transition, stability and validation gates align.";
  else if(contradiction>=4)status="SIGNAL",reason="Multiple independent gates align, with no major contradiction.";
  else if(contradiction<=1)status="AVOID",reason="The evidence is internally contradictory.";
  setText("probability",`${dir} ${d}: ${fmt(p)} • long window ${fmt(lp)}`);
  setText("paired",pr===null?"No paired contract for this digit.":`Paired side evidence: ${fmt(pr)}`);
  setText("transitions",tr===null?"No recent touches yet.":`After digit ${d}: ${fmt(tr)} for selected side`);
  setText("regime",rg);
  setText("stability",`${st.toFixed(0)}% stability`);
  setText("validation",`Train ${fmt(tp)} • unseen ${fmt(vp)} • consistency ${consistency.toFixed(0)}%`);
  setSignal(status,reason);
}
function setSignal(status,reason){$("signalStatus").textContent=status;$("signalStatus").className="signal-status "+status.toLowerCase().replace(" ","-");$("signalReason").textContent=reason}
function renderStream(){ $("stream").textContent=state.stream.length?state.stream.join(" "):"Waiting for ticks…";setText("tickCount",buffer().length);setText("lastDigit",state.stream.at(-1)??"—");setText("lastPrice",state.price??"—") }
function digitFromQuote(quote,pip){const decimals=Math.max(0,Number(pip)||0);const fixed=Number(quote).toFixed(decimals);const digits=fixed.replace(/\D/g,"");return Number(digits.at(-1))}
function subscribe(symbol){if(!state.ws||state.ws.readyState!==1)return;state.ws.send(JSON.stringify({ticks:symbol,subscribe:1}));}
function connect(){
  setStatus("CONNECTING","connecting");state.ws=new WebSocket(WS_URL);
  state.ws.onopen=()=>{setStatus("LIVE","live");state.ws.send(JSON.stringify({active_symbols:"brief",product_type:"basic"}))};
  state.ws.onmessage=e=>{let m;try{m=JSON.parse(e.data)}catch{return}
    if(m.error){setStatus("ERROR","offline");return}
    if(m.msg_type==="active_symbols"){state.markets=(m.active_symbols||[]).filter(x=>/Volatility|Jump/i.test(x.display_name||"")).sort((a,b)=>(a.display_name||"").localeCompare(b.display_name||""));populateMarkets();return}
    if(m.msg_type==="tick"){const t=m.tick;if(!t)return;const d=digitFromQuote(t.quote,t.pip_size);if(!Number.isInteger(d)||d<0||d>9)return;const a=buffer();a.push(d);if(a.length>MAX_HISTORY)a.splice(0,a.length-MAX_HISTORY);state.price=t.quote;state.stream.push(d);if(state.stream.length>80)state.stream.shift();renderStream();analyze()}
  };
  state.ws.onclose=()=>{setStatus("OFFLINE","offline");setTimeout(connect,3000)};
  state.ws.onerror=()=>setStatus("ERROR","offline");
}
function populateMarkets(){
  const sel=$("market");sel.innerHTML=state.markets.map(x=>`<option value="${x.symbol}">${x.display_name}</option>`).join("");
  if(!state.market&&state.markets[0])state.market=state.markets[0].symbol;
  sel.value=state.market;if(state.market)subscribe(state.market);renderStream();analyze();
}
function selectMarket(v){state.market=v;state.stream=[];renderStream();if(state.ws?.readyState===1)subscribe(v);analyze()}
function init(){
  $("digits").innerHTML=Array.from({length:10},(_,d)=>`<button class="digit ${d===1?"active":""}" data-digit="${d}">${d}</button>`).join("");
  $("digits").addEventListener("click",e=>{const b=e.target.closest("[data-digit]");if(!b)return;state.digit=Number(b.dataset.digit);document.querySelectorAll(".digit").forEach(x=>x.classList.toggle("active",x===b));analyze()});
  document.querySelectorAll(".direction").forEach(b=>b.addEventListener("click",()=>{state.direction=b.dataset.direction;document.querySelectorAll(".direction").forEach(x=>x.classList.toggle("active",x===b));analyze()}));
  $("market").addEventListener("change",e=>selectMarket(e.target.value));
  $("clear").addEventListener("click",()=>{state.stream=[];renderStream()});
  if("serviceWorker" in navigator)navigator.serviceWorker.register("./sw.js").catch(()=>{});
  connect();
}
init();