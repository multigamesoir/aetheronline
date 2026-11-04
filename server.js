// Serveur Aethergard : sert le jeu + relaie les messages entre les 2 joueurs (WebSocket).
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const {WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000, ROOT=__dirname;
const MIME={'.html':'text/html; charset=utf-8','.js':'text/javascript','.mp3':'audio/mpeg','.ogg':'audio/ogg','.png':'image/png','.json':'application/json','.css':'text/css'};

/* ===================== COMPTES & SAUVEGARDES ===================== */
const DATA_DIR=process.env.DATA_DIR||path.join(__dirname,'data');
const UP_URL=process.env.UPSTASH_REDIS_REST_URL,UP_TOKEN=process.env.UPSTASH_REDIS_REST_TOKEN;
let store;
if(UP_URL&&UP_TOKEN){ // stockage persistant gratuit (Upstash Redis)
  const up=async cmd=>{const r=await fetch(UP_URL,{method:'POST',headers:{Authorization:'Bearer '+UP_TOKEN,'Content-Type':'application/json'},body:JSON.stringify(cmd)});const j=await r.json();if(j.error)throw new Error(j.error);return j.result};
  store={kind:'upstash',get:async k=>{const v=await up(['GET','aeg:'+k]);return v?JSON.parse(v):null},set:async(k,v)=>{await up(['SET','aeg:'+k,JSON.stringify(v)])}};
}else{ // fichier local (⚠ effacé par Render gratuit à chaque redémarrage/déploiement, sauf disque persistant)
  const F=path.join(DATA_DIR,'accounts.json');let db={},t=null;
  try{fs.mkdirSync(DATA_DIR,{recursive:true});db=JSON.parse(fs.readFileSync(F,'utf8'))}catch(e){}
  const flush=()=>{t=null;try{fs.writeFileSync(F+'.tmp',JSON.stringify(db));fs.renameSync(F+'.tmp',F)}catch(e){console.error('save db',e.message)}};
  store={kind:'file',get:async k=>db[k]||null,set:async(k,v)=>{db[k]=v;if(!t)t=setTimeout(flush,800)}};
}
console.log('Stockage des comptes :',store.kind);
const sessions=new Map(); // token -> key
const fails=new Map();    // ip -> {n,t}
const hashPw=(pw,salt)=>crypto.scryptSync(String(pw),salt,32).toString('hex');
const okName=n=>/^[A-Za-z0-9_\-]{3,12}$/.test(n);
function ipOf(req){return (req.headers['x-forwarded-for']||req.socket.remoteAddress||'?').toString().split(',')[0].trim()}
function blocked(ip){const f=fails.get(ip);return f&&f.n>=10&&Date.now()-f.t<10*60*1000}
function fail(ip){const f=fails.get(ip)||{n:0,t:0};if(Date.now()-f.t>10*60*1000)f.n=0;f.n++;f.t=Date.now();fails.set(ip,f)}
function readBody(req){return new Promise((res,rej)=>{let b='';req.on('data',c=>{b+=c;if(b.length>300000){rej(new Error('big'));req.destroy()}});req.on('end',()=>res(b));req.on('error',rej)})}
function json(res,code,o){res.writeHead(code,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(o))}
async function api(req,res,u){
  if(req.method!='POST')return json(res,405,{err:'POST seulement'});
  let b;try{b=JSON.parse(await readBody(req))}catch(e){return json(res,400,{err:'Requête invalide'})}
  const ip=ipOf(req);
  try{
    if(u=='/api/register'||u=='/api/login'){
      const name=String(b.name||'').trim(),pw=String(b.pass||'');
      if(!okName(name))return json(res,400,{err:'Pseudo : 3 à 12 caractères (lettres, chiffres, _ ou -).'});
      if(pw.length<4||pw.length>64)return json(res,400,{err:'Mot de passe : 4 caractères minimum.'});
      if(blocked(ip))return json(res,429,{err:'Trop d\'essais. Réessaie dans 10 minutes.'});
      const key=name.toLowerCase();let acc=await store.get(key);
      if(u=='/api/register'){
        if(acc)return json(res,409,{err:'Ce pseudo existe déjà. Utilise SE CONNECTER.'});
        const salt=crypto.randomBytes(16).toString('hex');
        acc={name,salt,hash:hashPw(pw,salt),save:null,created:Date.now()};await store.set(key,acc);
      }else{
        if(!acc){fail(ip);return json(res,404,{err:'Compte introuvable. Clique sur CRÉER LE COMPTE.'})}
        const a=Buffer.from(hashPw(pw,acc.salt),'hex'),h=Buffer.from(acc.hash,'hex');
        if(a.length!=h.length||!crypto.timingSafeEqual(a,h)){fail(ip);return json(res,401,{err:'Mot de passe incorrect.'})}
      }
      const token=crypto.randomBytes(18).toString('hex');sessions.set(token,key);
      return json(res,200,{ok:1,name:acc.name,token,save:acc.save,created:u=='/api/register'});
    }
    if(u=='/api/save'){
      const key=sessions.get(String(b.token||''));if(!key)return json(res,401,{err:'Session expirée'});
      const acc=await store.get(key);if(!acc)return json(res,404,{err:'Compte introuvable'});
      if(!b.save||typeof b.save!='object'||JSON.stringify(b.save).length>120000)return json(res,400,{err:'Sauvegarde invalide'});
      acc.save=b.save;await store.set(key,acc);return json(res,200,{ok:1});
    }
    return json(res,404,{err:'Inconnu'});
  }catch(e){console.error('api',e.message);return json(res,500,{err:'Erreur serveur'})}
}

const server=http.createServer((req,res)=>{
  let u=decodeURIComponent((req.url||'/').split('?')[0]);
  if(u.startsWith('/api/'))return api(req,res,u);
  if(u=='/turn.js'){res.writeHead(200,{'Content-Type':'text/javascript','Cache-Control':'no-store'});
    return res.end("window.AEG_RELAY=(location.protocol=='https:'?'wss://':'ws://')+location.host+'/ws';")}
  if(u=='/healthz'){res.writeHead(200);return res.end('ok')}
  if(u=='/api/status'){return json(res,200,{ok:1,store:store.kind})}
  if(u=='/')u='/index.html';
  const f=path.normalize(path.join(ROOT,u));
  if(!f.startsWith(ROOT)||/server\.js|package|[\\/]data[\\/]/.test(f)){res.writeHead(404);return res.end('404')}
  fs.readFile(f,(e,d)=>{if(e){res.writeHead(404);return res.end('404')}
    res.writeHead(200,{'Content-Type':MIME[path.extname(f)]||'application/octet-stream'});res.end(d)});
});
const wss=new WebSocketServer({server,path:'/ws',maxPayload:1<<20});
const rooms=new Map(); // code -> {host,guest}
function send(ws,o){if(ws&&ws.readyState==1)ws.send(JSON.stringify(o))}
wss.on('connection',ws=>{
  ws.alive=true;
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.k=='ka')return;
    if(m.k=='host'){
      const c=String(m.code||'').toUpperCase().slice(0,8);
      if(!c||rooms.has(c))return send(ws,{k:'err',m:'Code déjà utilisé, réessaie.'});
      rooms.set(c,{host:ws,guest:null});ws.room=c;ws.role='host';send(ws,{k:'ok'});
    }else if(m.k=='join'){
      const c=String(m.code||'').toUpperCase().slice(0,8),r=rooms.get(c);
      if(!r)return send(ws,{k:'err',m:'Room introuvable. Vérifie le code.'});
      if(r.guest)return send(ws,{k:'err',m:'Room pleine.'});
      r.guest=ws;ws.room=c;ws.role='guest';send(ws,{k:'ok'});send(r.host,{k:'peer'});
    }else if(m.k=='d'){
      const r=rooms.get(ws.room);if(!r)return;
      send(ws.role=='host'?r.guest:r.host,{k:'d',d:m.d});
    }
  });
  ws.on('close',()=>{
    const r=rooms.get(ws.room);if(!r)return;
    if(ws.role=='host'){send(r.guest,{k:'bye'});rooms.delete(ws.room);try{r.guest&&r.guest.close()}catch(e){}}
    else{r.guest=null;send(r.host,{k:'bye'})}
  });
});
server.listen(PORT,()=>console.log('Aethergard relay on',PORT));
