
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { monitorEventLoopDelay } = require('perf_hooks');

const PORTA = process.env.PORT || 3000;
const JANELA_S = Number(process.env.JANELA_S || 60);          // a "carga" conta os pedidos dos ultimos 60 s
const LIM_ATENCAO = Number(process.env.LIM_ATENCAO || 15);    // unidades na janela: >= 15 -> ATENCAO
const LIM_CRITICO = Number(process.env.LIM_CRITICO || 25);    // unidades na janela: >= 25 -> CRITICO
const IP_ATENCAO = Number(process.env.IP_ATENCAO || 5);       // pedidos de um mesmo IP na janela
const IP_CRITICO = Number(process.env.IP_CRITICO || 10);
const IP_BLOQUEIO = Number(process.env.IP_BLOQUEIO || 10);    // pedidos do mesmo IP/min -> 429 (bloqueado)
const MAX_RETIDO_MB = Number(process.env.MAX_RETIDO_MB || 300); // teto de RAM retida pelo simulador
const RAM_LIMITE_MB = Number(process.env.RAM_LIMITE_MB || 512); // plano gratis do Render = 512 MB
const SIMULAR_CARGA = process.env.SIMULAR_CARGA !== 'false';  // pedido grande gasta CPU/RAM de verdade (para o monitor reagir)

const SUPABASE_URL = (process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SUPABASE_KEY = (process.env.SUPABASE_KEY || '').trim();
const DB_ATIVO = Boolean(SUPABASE_URL && SUPABASE_KEY);
const DB_TIMEOUT_MS = Number(process.env.DB_TIMEOUT_MS || 8000);
const DB_FILA_MAX = Number(process.env.DB_FILA_MAX || 1000);  // pedidos esperando para gravar (se passar, descarta os mais antigos)


const iniciadoEm = Date.now();
let pedidos = [];          // {t, ip, item, quantidade, nivel, simulado}
let reqEventos = [];       // {t, ms, status}
let totalReq = 0, totalPedidos = 0, totalUnidades = 0;
const classes = { '2xx': 0, '4xx': 0, '5xx': 0 };
const historico = [];      // {t, carga, cpu, ram, rps}
let cpuPct = 0, rpsAtual = 0;
let retidoBytes = 0;

const loop = monitorEventLoopDelay({ resolution: 20 });
loop.enable();

const nivelFaixa = (v, atencao, critico) => (v >= critico ? 'CRITICO' : v >= atencao ? 'ATENCAO' : 'OK');
const nivelQtd = (q) => nivelFaixa(q, LIM_ATENCAO, LIM_CRITICO);
const ORDEM = { OK: 0, ATENCAO: 1, CRITICO: 2 };
const pior = (lista) => lista.reduce((a, b) => (ORDEM[b] > ORDEM[a] ? b : a), 'OK');


const db = {
  ok: null,                // null = ainda nao testou | true | false
  ultimoErro: '', ultimoErroEm: 0,
  fila: [],                // pedidos esperando para gravar
  gravados: 0, erros: 0, descartados: 0,
  lat: [],                 // {t, ms} de cada chamada ao banco
  gravando: false,
  ultimoPing: 0, tamanhoMB: null, conexoes: null, linhasPedidos: null, linhasMetricas: null,
};

async function dbFetch(caminho, { method = 'GET', body, prefer } = {}) {
  const headers = { apikey: SUPABASE_KEY, 'Content-Type': 'application/json' };
  if (SUPABASE_KEY.startsWith('eyJ')) headers.Authorization = 'Bearer ' + SUPABASE_KEY; // chave antiga (JWT)
  if (prefer) headers.Prefer = prefer;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), DB_TIMEOUT_MS);
  const t0 = Date.now();
  try {
    const r = await fetch(SUPABASE_URL + '/rest/v1/' + caminho, {
      method, headers, signal: ctrl.signal, body: body === undefined ? undefined : JSON.stringify(body),
    });
    const txt = await r.text();
    if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + txt.slice(0, 140));
    db.ok = true;
    return txt ? JSON.parse(txt) : null;
  } catch (e) {
    db.ok = false; db.erros++; db.ultimoErroEm = Date.now();
    db.ultimoErro = e.name === 'AbortError' ? 'timeout (sem resposta em ' + DB_TIMEOUT_MS / 1000 + ' s)' : String(e.message || e).slice(0, 160);
    throw e;
  } finally {
    clearTimeout(timer);
    db.lat.push({ t: Date.now(), ms: Date.now() - t0 });
    if (db.lat.length > 500) db.lat.splice(0, db.lat.length - 500);
  }
}

function dbLatencia() {
  const corte = Date.now() - JANELA_S * 1000;
  const ms = db.lat.filter((x) => x.t >= corte).map((x) => x.ms).sort((a, b) => a - b);
  return {
    p95: ms.length ? ms[Math.min(ms.length - 1, Math.floor(ms.length * 0.95))] : 0,
    ultima: db.lat.length ? db.lat[db.lat.length - 1].ms : 0,
  };
}

// pedido aceito -> entra na fila; a fila e gravada em lote a cada 2 s (o cliente nao espera o banco)
function enfileirar(p, teste) {
  if (!DB_ATIVO) return;
  db.fila.push({ criado_em: new Date(p.t).toISOString(), ip: p.ip, item: p.item, quantidade: p.quantidade,
    nivel: p.nivel, simulado: p.simulado, teste: teste || '' });
  if (db.fila.length > DB_FILA_MAX) { const sobra = db.fila.length - DB_FILA_MAX; db.fila.splice(0, sobra); db.descartados += sobra; }
}

async function gravarFila() {
  if (!DB_ATIVO || db.gravando || !db.fila.length) return;
  db.gravando = true;
  const lote = db.fila.splice(0, 200);
  try {
    await dbFetch('pedidos', { method: 'POST', body: lote, prefer: 'return=minimal' });
    db.gravados += lote.length;
  } catch {
    db.fila.unshift(...lote);                                  // falhou: devolve para tentar de novo
    if (db.fila.length > DB_FILA_MAX) { const sobra = db.fila.length - DB_FILA_MAX; db.fila.splice(0, sobra); db.descartados += sobra; }
  } finally { db.gravando = false; }
}

let pingN = 0;
async function pingBanco() {
  if (!DB_ATIVO) return;
  try {
    const s = await dbFetch('rpc/stats_banco', { method: 'POST', body: {} });  // tamanho, conexoes, linhas
    db.tamanhoMB = s.tamanho_mb; db.conexoes = s.conexoes;
    db.linhasPedidos = s.linhas_pedidos; db.linhasMetricas = s.linhas_metricas; db.ultimoPing = Date.now();
    const x = snapshot();
    await dbFetch('metricas', { method: 'POST', prefer: 'return=minimal', body: {
      status: x.status.nivel, carga_pct: x.carga.pct, cpu_pct: x.cpu.pct, ram_mb: x.ram.rssMB, ram_pct: x.ram.pct,
      p95_ms: x.requisicoes.p95Ms, rps: x.requisicoes.rps, erros_5xx: x.requisicoes.erros5xx,
      db_p95_ms: x.banco.p95Ms, db_fila: x.banco.fila } });
    if (++pingN % 360 === 0) await dbFetch('rpc/limpar_antigos', { method: 'POST', body: { dias: 7 } }); // ~1x por hora
  } catch { /* o erro ja ficou registrado em db.ultimoErro */ }
}

if (DB_ATIVO) {
  setInterval(gravarFila, 2000).unref();
  setInterval(pingBanco, 10000).unref();
  setTimeout(pingBanco, 1000).unref();
  console.log('Banco: Supabase ativo (' + SUPABASE_URL + ')');
} else console.log('Banco: desativado (defina SUPABASE_URL e SUPABASE_KEY para gravar os dados)');

// ================= AMOSTRAGEM (a cada 2 s): CPU, RAM, req/s =================
let cpuAnt = process.cpuUsage();
let tAnt = process.hrtime.bigint();
let totalAnt = 0;

function unidadesNaJanela() {
  const corte = Date.now() - JANELA_S * 1000;
  return pedidos.reduce((s, p) => (p.t >= corte ? s + p.quantidade : s), 0);
}

setInterval(() => {
  const agora = process.hrtime.bigint();
  const cpu = process.cpuUsage(cpuAnt);
  cpuPct = Math.min(100, ((cpu.user + cpu.system) / (Number(agora - tAnt) / 1e3)) * 100);
  cpuAnt = process.cpuUsage(); tAnt = agora;
  rpsAtual = (totalReq - totalAnt) / 2; totalAnt = totalReq;

  historico.push({
    t: Date.now(),
    carga: +((unidadesNaJanela() / LIM_CRITICO) * 100).toFixed(1),
    cpu: +cpuPct.toFixed(1),
    ram: +(process.memoryUsage().rss / 1048576).toFixed(1),
    rps: +rpsAtual.toFixed(1),
  });
  if (historico.length > 90) historico.shift();

  const corte = Date.now() - 10 * 60 * 1000; // guarda 10 min de pedidos
  pedidos = pedidos.filter((p) => p.t >= corte).slice(-2000);
}, 2000).unref();

// ================= SIMULA TRABALHO (CPU, RAM e latencia reagem ao pedido) =================
const dorme = (ms) => new Promise((r) => setTimeout(r, ms));

// teste: '' (normal) | 'cpu' | 'ram' | 'lento' | 'erro'
async function simularTrabalho(q, teste) {
  if (!SIMULAR_CARGA) return;

  // CPU: trava o processo (sobe CPU e event loop)
  const msCpu = teste === 'cpu' ? Math.min(2500, q * 40) : Math.min(400, q * 6);
  const fim = Date.now() + msCpu;
  while (Date.now() < fim) Math.sqrt(Math.random());

  // RAM: segura memoria por 30 s
  const mb = teste === 'ram' ? Math.max(40, q) : q * 0.5;
  const bytes = Math.floor(mb * 1048576);
  if (retidoBytes + bytes <= MAX_RETIDO_MB * 1048576) {
    let buf = Buffer.alloc(bytes, 1);
    retidoBytes += bytes;
    setTimeout(() => { buf = null; retidoBytes -= bytes; }, 30_000).unref();
  }

  // LATENCIA: demora para responder (sobe o p95)
  const msEspera = teste === 'lento' ? Math.min(4000, q * 100) : Math.min(1500, q * 20);
  await dorme(msEspera);
}

// ================= SNAPSHOT (o que o /monitor mostra) =================
function snapshot() {
  const agora = Date.now();
  const corte = agora - JANELA_S * 1000;
  const jan = pedidos.filter((p) => p.t >= corte);
  const unidades = jan.reduce((s, p) => s + p.quantidade, 0);

  const dist = { OK: 0, ATENCAO: 0, CRITICO: 0 };
  jan.forEach((p) => dist[p.nivel]++);
  const pct = {};
  for (const k of Object.keys(dist)) pct[k] = jan.length ? +((dist[k] / jan.length) * 100).toFixed(1) : 0;

  const mapa = new Map();
  for (const p of jan) {
    const x = mapa.get(p.ip) || { ip: p.ip, pedidos: 0, unidades: 0, ultimo: 0, simulado: p.simulado };
    x.pedidos++; x.unidades += p.quantidade; x.ultimo = Math.max(x.ultimo, p.t);
    mapa.set(p.ip, x);
  }
  const porIp = [...mapa.values()]
    .sort((a, b) => b.pedidos - a.pedidos || b.unidades - a.unidades)
    .map((x) => ({ ip: x.ip, pedidos: x.pedidos, unidades: x.unidades, simulado: x.simulado,
      ultimoHaSeg: Math.round((agora - x.ultimo) / 1000), nivel: nivelFaixa(x.pedidos, IP_ATENCAO, IP_CRITICO) }));

  reqEventos = reqEventos.filter((e) => e.t >= corte);
  const ms = reqEventos.map((e) => e.ms).sort((a, b) => a - b);
  const p95 = ms.length ? ms[Math.min(ms.length - 1, Math.floor(ms.length * 0.95))] : 0;
  const erros5xx = reqEventos.filter((e) => e.status >= 500).length;

  const mem = process.memoryUsage();
  const ramPct = (mem.rss / (RAM_LIMITE_MB * 1048576)) * 100;
  const cargaPct = (unidades / LIM_CRITICO) * 100;
  const maxIp = porIp.length ? porIp[0].pedidos : 0;

  const dbLat = dbLatencia();
  const nivelBanco = !DB_ATIVO ? 'OK' : pior([
    db.ok === false ? 'CRITICO' : 'OK',
    nivelFaixa(dbLat.p95, 300, 800),
    nivelFaixa(db.fila.length, 100, 500),
  ]);

  const nivel = {
    carga: nivelQtd(unidades),
    cpu: nivelFaixa(cpuPct, 70, 90),
    ram: nivelFaixa(ramPct, 70, 90),
    latencia: nivelFaixa(p95, 500, 1000),
    erros: nivelFaixa(erros5xx, 1, 5),
    ip: nivelFaixa(maxIp, IP_ATENCAO, IP_CRITICO),
    banco: nivelBanco,
  };
  const motivos = [];
  const txt = { ATENCAO: 'ATENÇÃO', CRITICO: 'CRÍTICO' };
  const add = (k, texto) => { if (nivel[k] !== 'OK') motivos.push(texto + ' → ' + txt[nivel[k]]); };
  add('carga', 'Carga de pedidos: ' + unidades + ' un. no último minuto (' + cargaPct.toFixed(0) + '%)');
  add('cpu', 'CPU em ' + cpuPct.toFixed(0) + '%');
  add('ram', 'Memória em ' + ramPct.toFixed(0) + '% do limite');
  add('latencia', 'Latência p95 de ' + p95.toFixed(0) + ' ms');
  add('erros', erros5xx + ' erro(s) 5xx no último minuto');
  add('ip', 'IP ' + (porIp[0] ? porIp[0].ip : '') + ' fez ' + maxIp + ' pedidos no último minuto');
  add('banco', db.ok === false
    ? 'Banco de dados com problema: ' + db.ultimoErro
    : 'Banco de dados lento/atrasado (p95 ' + dbLat.p95.toFixed(0) + ' ms, fila ' + db.fila.length + ')');

  return {
    uptimeSeg: Math.floor((agora - iniciadoEm) / 1000),
    janelaSeg: JANELA_S,
    status: { nivel: pior(Object.values(nivel)), motivos },
    niveis: nivel,
    limites: { atencao: LIM_ATENCAO, critico: LIM_CRITICO },
    carga: { unidades, pct: +cargaPct.toFixed(1) },
    pedidos: {
      total: totalPedidos, unidadesTotal: totalUnidades,
      janela: { qtd: jan.length, unidades, ipsDistintos: mapa.size, distribuicao: dist, pct },
      porIp: porIp.slice(0, 10),
      ultimos: pedidos.slice(-12).reverse().map((p) => ({
        haSeg: Math.round((agora - p.t) / 1000), ip: p.ip, item: p.item, quantidade: p.quantidade, nivel: p.nivel, simulado: p.simulado })),
    },
    requisicoes: { total: totalReq, rps: +rpsAtual.toFixed(1), p95Ms: +p95.toFixed(1), erros5xx, classes },
    cpu: { pct: +cpuPct.toFixed(1), cores: os.cpus().length, load1: +os.loadavg()[0].toFixed(2) },
    ram: { rssMB: +(mem.rss / 1048576).toFixed(1), heapMB: +(mem.heapUsed / 1048576).toFixed(1),
      limiteMB: RAM_LIMITE_MB, pct: +ramPct.toFixed(1),
      maquinaTotalMB: Math.round(os.totalmem() / 1048576), maquinaLivreMB: Math.round(os.freemem() / 1048576) },
    banco: {
      configurado: DB_ATIVO, ok: db.ok, p95Ms: +dbLat.p95.toFixed(1), ultimaMs: dbLat.ultima,
      fila: db.fila.length, gravados: db.gravados, erros: db.erros, descartados: db.descartados,
      ultimoErro: db.ultimoErro, ultimoErroHaSeg: db.ultimoErroEm ? Math.round((agora - db.ultimoErroEm) / 1000) : null,
      tamanhoMB: db.tamanhoMB, conexoes: db.conexoes, linhasPedidos: db.linhasPedidos, linhasMetricas: db.linhasMetricas,
      pingHaSeg: db.ultimoPing ? Math.round((agora - db.ultimoPing) / 1000) : null,
    },
    eventLoopMs: Number.isFinite(loop.mean) ? +(loop.mean / 1e6).toFixed(1) : 0,
    historico,
  };
}

// ================= PAGINA /monitor (HTML dentro do proprio server.js) =================
const PAGINA_MONITOR = String.raw`<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Monitor ao vivo</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--txt:#1c1e21;--mut:#6b7280;--bd:#e5e7eb;--ok:#16a34a;--at:#d97706;--cr:#dc2626}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#181b21;--txt:#e8eaed;--mut:#9aa0a6;--bd:#2a2e36;--ok:#4ade80;--at:#fbbf24;--cr:#f87171}}
*{box-sizing:border-box}body{margin:0;font-family:system-ui,sans-serif;background:var(--bg);color:var(--txt);padding:16px;max-width:900px;margin:0 auto}
.top{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:12px}
h1{font-size:20px;margin:0}h2{font-size:15px;margin:20px 0 10px}.sub{color:var(--mut);font-size:12px}
a,button.link{color:#2563eb;text-decoration:none;font-size:14px;background:none;border:0;cursor:pointer;padding:0}
#banner{border-radius:12px;padding:16px 20px;color:#fff;background:var(--ok)}
#banner.ATENCAO{background:var(--at)}#banner.CRITICO{background:var(--cr)}
#banner .t{font-size:24px;font-weight:700}#banner ul{margin:8px 0 0;padding-left:18px;font-size:14px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px}
.lbl{font-size:12px;color:var(--mut)}.val{font-size:24px;font-weight:600;margin:4px 0 8px}
.barra{height:8px;background:var(--bd);border-radius:4px;overflow:hidden}.barra i{display:block;height:100%;border-radius:4px;transition:width .4s}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:7px 6px;border-bottom:1px solid var(--bd)}th{color:var(--mut);font-weight:500}
.wrap{overflow-x:auto}.tag{padding:2px 10px;border-radius:99px;font-size:12px;font-weight:600;color:#fff}
.OK{background:var(--ok)}.ATENCAO{background:var(--at)}.CRITICO{background:var(--cr)}
.sim{font-size:11px;color:var(--mut);border:1px solid var(--bd);border-radius:4px;padding:0 4px;margin-left:4px}
canvas{width:100%;height:130px;display:block}
</style></head><body>
<div class="top"><div><h1>📡 Monitor ao vivo</h1><div class="sub" id="info">Conectando... (no plano gratuito do Render o servidor pode levar ~1 min para acordar)</div></div>
<div><a href="/pedido">Fazer pedido →</a> &nbsp; <button class="link" onclick="zerar()">Zerar dados</button></div></div>

<div id="banner"><div class="t" id="bTitulo">...</div><ul id="bMotivos"></ul></div>

<h2>Situação agora</h2>
<div class="grid" id="cards"></div>

<h2>Carga de pedidos (%)</h2>
<div class="card"><canvas id="gCarga"></canvas></div>

<h2>Pedidos por IP (último minuto)</h2>
<div class="card wrap"><table><thead><tr><th>IP</th><th>Pedidos</th><th>Unidades</th><th>Nível</th></tr></thead><tbody id="ips"></tbody></table></div>

<h2>Últimos pedidos</h2>
<div class="card wrap"><table><thead><tr><th>Quando</th><th>IP</th><th>Item</th><th>Qtd</th><th>Nível</th></tr></thead><tbody id="ultimos"></tbody></table></div>

<p class="sub" id="reqs"></p>

<script>
var $ = function(id){return document.getElementById(id)};
var esc = function(s){return String(s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})};
var NOME = {OK:'OK',ATENCAO:'Atenção',CRITICO:'Crítico'};
var COR = {OK:'var(--ok)',ATENCAO:'var(--at)',CRITICO:'var(--cr)'};
var TITULO = {OK:'✅ SISTEMA OK',ATENCAO:'⚠️ SISTEMA EM ATENÇÃO',CRITICO:'🚨 SISTEMA CRÍTICO'};
var css = function(v){return getComputedStyle(document.documentElement).getPropertyValue(v).trim()};

function card(titulo, valor, pct, nivel, sub){
  return '<div class="card" style="border-top:4px solid '+COR[nivel]+'"><div class="lbl">'+titulo+'</div><div class="val">'+valor+
    '</div><div class="barra"><i style="width:'+Math.min(100,pct)+'%;background:'+COR[nivel]+'"></i></div><div class="sub" style="margin-top:6px">'+sub+'</div></div>';
}
function grafico(id, dados, max, cor){
  var c=$(id), d=window.devicePixelRatio||1, w=c.clientWidth, h=c.clientHeight;
  c.width=w*d; c.height=h*d; var g=c.getContext('2d'); g.scale(d,d);
  g.strokeStyle=css('--bd'); g.fillStyle=css('--mut'); g.font='11px system-ui'; g.lineWidth=1;
  for(var i=0;i<=3;i++){var y=10+((h-20)*i)/3; g.beginPath(); g.moveTo(34,y); g.lineTo(w,y); g.stroke(); g.fillText((max*(1-i/3)).toFixed(0),0,y+4);}
  g.strokeStyle=css(cor); g.lineWidth=2; g.beginPath();
  dados.forEach(function(v,i){var x=34+((w-34)*i)/Math.max(1,dados.length-1), y=10+(h-20)*(1-Math.min(v,max)/max); if(i){g.lineTo(x,y)}else{g.moveTo(x,y)}});
  g.stroke();
}
async function zerar(){ if(confirm('Apagar pedidos e métricas da memória? (os dados gravados no banco são mantidos)')){ await fetch('/api/reset',{method:'POST'}); atualizar(); } }

function cardBanco(b, nivel){
  if(!b.configurado) return '<div class="card"><div class="lbl">Banco de dados</div><div class="val">desativado</div><div class="sub">defina SUPABASE_URL e SUPABASE_KEY</div></div>';
  var valor = b.ok===false ? 'OFFLINE' : b.ok===null ? 'testando...' : b.p95Ms+' ms';
  var sub = b.ok===false ? esc(b.ultimoErro) : (b.linhasPedidos===null?'?':b.linhasPedidos)+' pedidos gravados · fila '+b.fila+(b.descartados?' · perdidos '+b.descartados:'');
  return card('Banco (latência p95)', valor, b.ok===false?100:b.p95Ms/8, nivel, sub);
}

async function atualizar(){
  try{
    var s = await (await fetch('/api/monitor')).json();
    var h = s.historico, n = s.niveis;
    $('info').textContent = 'Ao vivo · ligado há '+Math.floor(s.uptimeSeg/60)+' min '+(s.uptimeSeg%60)+' s';
    $('banner').className = s.status.nivel; $('bTitulo').textContent = TITULO[s.status.nivel];
    $('bMotivos').innerHTML = s.status.motivos.map(function(m){return '<li>'+esc(m)+'</li>'}).join('') || '<li>Tudo funcionando normalmente.</li>';

    $('cards').innerHTML =
      card('Carga de pedidos', s.carga.pct.toFixed(0)+'%', s.carga.pct, n.carga, s.carga.unidades+' un. no último minuto') +
      card('CPU', s.cpu.pct.toFixed(0)+'%', s.cpu.pct, n.cpu, s.cpu.cores+' núcleo(s)') +
      card('Memória RAM', s.ram.rssMB+' MB', s.ram.pct, n.ram, s.ram.pct.toFixed(0)+'% de '+s.ram.limiteMB+' MB') +
      cardBanco(s.banco, n.banco);

    grafico('gCarga', h.map(function(x){return x.carga}), Math.max(100, Math.max.apply(null,h.map(function(x){return x.carga}))), '--at');

    $('ips').innerHTML = s.pedidos.porIp.map(function(x){
      return '<tr><td>'+esc(x.ip)+(x.simulado?'<span class="sim">simulado</span>':'')+'</td><td>'+x.pedidos+'</td><td>'+x.unidades+'</td><td><span class="tag '+x.nivel+'">'+NOME[x.nivel]+'</span></td></tr>';
    }).join('') || '<tr><td colspan="4">Nenhum pedido no último minuto.</td></tr>';

    $('ultimos').innerHTML = s.pedidos.ultimos.map(function(x){
      return '<tr><td>há '+x.haSeg+' s</td><td>'+esc(x.ip)+(x.simulado?'<span class="sim">simulado</span>':'')+'</td><td>'+esc(x.item)+'</td><td><b>'+x.quantidade+'</b></td><td><span class="tag '+x.nivel+'">'+NOME[x.nivel]+'</span></td></tr>';
    }).join('') || '<tr><td colspan="5">Nenhum pedido ainda. Abra /pedido no celular.</td></tr>';

    var r = s.requisicoes, c = r.classes;
    $('reqs').textContent = 'Requisições: '+r.total+' no total · '+r.rps+'/s · sucesso '+c['2xx']+' · erro do cliente '+c['4xx']+' · erro do servidor '+c['5xx']+' · latência p95 '+r.p95Ms+' ms';
  }catch(e){ $('info').textContent = 'Sem conexão com o servidor... tentando de novo'; }
}
atualizar(); setInterval(atualizar, 1000); window.addEventListener('resize', atualizar);
</script></body></html>`;

// ================= HTTP =================
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
const SEM_METRICA = new Set(['/api/monitor', '/api/banco/pedidos', '/health', '/api/reset']); // nao contam como "requisicoes"

function ipDoCliente(req) {
  const xf = req.headers['x-forwarded-for']; // o Render coloca o IP real do visitante aqui
  const ip = xf ? xf.split(',')[0].trim() : req.socket.remoteAddress || 'desconhecido';
  return ip.replace(/^::ffff:/, '');
}
const ipValido = (s) => typeof s === 'string' && /^(\d{1,3}\.){3}\d{1,3}$/.test(s) && s.split('.').every((n) => Number(n) <= 255);

function lerBody(req) {
  return new Promise((ok) => {
    let txt = '';
    req.on('data', (c) => { txt += c; if (txt.length > 10_000) req.destroy(); });
    req.on('end', () => { try { ok(JSON.parse(txt || '{}')); } catch { ok({}); } });
  });
}

async function criarPedido(body, req) {
  const item = String(body.item || 'Carro').trim().slice(0, 30) || 'Carro';
  const quantidade = Number(body.quantidade);
  if (!Number.isInteger(quantidade) || quantidade < 1 || quantidade > 500)
    return { status: 400, dados: { erro: 'quantidade deve ser um numero inteiro de 1 a 500' } };

  const teste = ['cpu', 'ram', 'lento', 'erro'].includes(body.teste) ? body.teste : '';
  const simulado = ipValido(body.ipSimulado);            // para simular muitos IPs diferentes
  const ip = simulado ? body.ipSimulado : ipDoCliente(req);

  // 429 (4xx): mesmo IP mandando demais
  const corte = Date.now() - JANELA_S * 1000;
  const doIp = pedidos.filter((p) => p.ip === ip && p.t >= corte).length;
  if (doIp >= IP_BLOQUEIO)
    return { status: 429, dados: { erro: 'IP ' + ip + ' bloqueado: ' + doIp + ' pedidos no último minuto' } };

  // 503 (5xx): sistema critico recusa parte dos pedidos
  if (unidadesNaJanela() >= LIM_CRITICO && Math.random() < 0.3)
    return { status: 503, dados: { erro: 'servidor sobrecarregado, tente de novo' } };

  // 500 (5xx): erro forcado pelo teste
  if (teste === 'erro') {
    await dorme(200);
    return { status: 500, dados: { erro: 'falha simulada (teste de erro)' } };
  }

  const nivel = nivelQtd(quantidade);
  const registro = { t: Date.now(), ip, item, quantidade, nivel, simulado };
  pedidos.push(registro);
  enfileirar(registro, teste);                           // grava no Supabase em segundo plano
  totalPedidos++; totalUnidades += quantidade;
  await simularTrabalho(quantidade, teste);

  const unidades = unidadesNaJanela();
  return { status: 201, dados: {
    ok: true, id: totalPedidos, ip, simulado, item, quantidade, nivelPedido: nivel,
    carga: { unidades, pct: +((unidades / LIM_CRITICO) * 100).toFixed(1) },
    statusSistema: snapshot().status.nivel,
  } };
}

http.createServer(async (req, res) => {
  const caminho = new URL(req.url, 'http://x').pathname;
  const ini = process.hrtime.bigint();

  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }

  // paginas
  if (req.method === 'GET' && caminho === '/monitor') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(PAGINA_MONITOR);
  }
  if (req.method === 'GET' && (caminho === '/' || caminho === '/pedido')) {
    try {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(path.join(__dirname, 'pedido.html')));
    } catch {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end('Coloque o arquivo pedido.html na mesma pasta do server.js');
    }
  }

  // API
  let status = 200, dados;
  try {
    if (req.method === 'GET' && caminho === '/api/monitor') dados = snapshot();
    else if (req.method === 'GET' && caminho === '/health') dados = { status: 'ok' };
    else if (req.method === 'GET' && caminho === '/api/banco/pedidos') {
      if (!DB_ATIVO) dados = { linhas: [], erro: 'Banco não configurado (SUPABASE_URL / SUPABASE_KEY).' };
      else {
        try { dados = { linhas: await dbFetch('pedidos?select=id,criado_em,ip,item,quantidade,nivel,simulado&order=id.desc&limit=10') }; }
        catch { dados = { linhas: [], erro: 'Erro ao ler o banco: ' + db.ultimoErro }; }
      }
    }
    else if (req.method === 'POST' && caminho === '/api/pedido') ({ status, dados } = await criarPedido(await lerBody(req), req));
    else if (req.method === 'POST' && caminho === '/api/reset') {
      pedidos = []; reqEventos = []; historico.length = 0;
      totalReq = totalPedidos = totalUnidades = 0; classes['2xx'] = classes['4xx'] = classes['5xx'] = 0;
      dados = { ok: true };
    } else { status = 404; dados = { erro: 'rota nao encontrada' }; }
  } catch (e) {
    status = 500; dados = { erro: 'erro interno' };
    console.error('erro:', e.message);
  }

  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', ...CORS });
  res.end(JSON.stringify(dados));

  if (!SEM_METRICA.has(caminho)) {
    const ms = Number(process.hrtime.bigint() - ini) / 1e6;
    totalReq++;
    classes[`${Math.floor(status / 100)}xx`] = (classes[`${Math.floor(status / 100)}xx`] || 0) + 1;
    reqEventos.push({ t: Date.now(), ms, status });
    console.log(JSON.stringify({ hora: new Date().toISOString(), metodo: req.method, rota: caminho, status, ms: Math.round(ms), ip: ipDoCliente(req) }));
  }
}).listen(PORTA, '0.0.0.0', () => console.log('Servidor na porta ' + PORTA + ' -> /pedido e /monitor'));
