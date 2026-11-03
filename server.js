// Serveur Aethergard : sert le jeu + relaie les messages entre les 2 joueurs (WebSocket).
const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000, ROOT=__dirname;
const MIME={'.html':'text/html; charset=utf-8','.js':'text/javascript','.mp3':'audio/mpeg','.ogg':'audio/ogg','.png':'image/png','.json':'application/json','.css':'text/css'};
const server=http.createServer((req,res)=>{
  let u=decodeURIComponent((req.url||'/').split('?')[0]);
  if(u=='/turn.js'){res.writeHead(200,{'Content-Type':'text/javascript','Cache-Control':'no-store'});
    return res.end("window.AEG_RELAY=(location.protocol=='https:'?'wss://':'ws://')+location.host+'/ws';")}
  if(u=='/healthz'){res.writeHead(200);return res.end('ok')}
  if(u=='/')u='/index.html';
  const f=path.normalize(path.join(ROOT,u));
  if(!f.startsWith(ROOT)||/server\.js|package/.test(f)){res.writeHead(404);return res.end('404')}
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
