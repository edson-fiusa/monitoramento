const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { monitorEventLoopDelay } = require('perf_hooks');
const { createClient } = require('@supabase/supabase-js');

const PORTA = process.env.PORT || 3000;
const JANELA_S = Number(process.env.JANELA_S || 60);
const LIM_ATENCAO = Number(process.env.LIM_ATENCAO || 11);
const LIM_CRITICO = Number(process.env.LIM_CRITICO || 20);
const IP_ATENCAO = Number(process.env.IP_ATENCAO || 5);
const IP_CRITICO = Number(process.env.IP_CRITICO || 10);
const IP_BLOQUEIO = Number(process.env.IP_BLOQUEIO || 15);
const MAX_RETIDO_MB = Number(process.env.MAX_RETIDO_MB || 300);
const RAM_LIMITE_MB = Number(process.env.RAM_LIMITE_MB || 512);
const SIMULAR_CARGA = process.env.SIMULAR_CARGA !== 'false';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Faltam SUPABASE_URL e SUPABASE_KEY (ou SUPABASE_ANON_KEY) nas variaveis de ambiente.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

let dbEscritas = 0, dbLeituras = 0;
let msUltimaEscrita = 0, msUltimaLeitura = 0;
let dbTamanhoMB = 0, dbPaginas = 0, dbPaginasLivres = 0, dbWalMB = 0;

const iniciadoEm = Date.now();
let reqEventos = [];
let totalReq = 0, totalPedidos = 0, totalUnidades = 0;
const classes = { '2xx': 0, '4xx': 0, '5xx': 0 };
const historico = [];
let cpuPct = 0, rpsAtual = 0;
let retidoBytes = 0;

const loop = monitorEventLoopDelay({ resolution: 20 });
loop.enable();

const nivelFaixa = (v, atencao, critico) => (v >= critico ? 'CRITICO' : v >= atencao ? 'ATENCAO' : 'OK');
const nivelQtd = (q) => nivelFaixa(q, LIM_ATENCAO, LIM_CRITICO);
const ORDEM = { OK: 0, ATENCAO: 1, CRITICO: 2 };
const pior = (lista) => lista.reduce((a, b) => (ORDEM[b] > ORDEM[a] ? b : a), 'OK');

async function inserirPedido(t, ip, item, quantidade, nivel, simulado) {
  const ini = process.hrtime.bigint();
  const { error } = await supabase.from('pedidos').insert({
    t: new Date(t).toISOString(),
    ip, item, quantidade, nivel, simulado,
  });
  msUltimaEscrita = Number(process.hrtime.bigint() - ini) / 1e6;
  dbEscritas++;
  if (error) throw new Error('supabase insert pedido: ' + error.message);
}

async function unidadesNaJanela() {
  const ini = process.hrtime.bigint();
  const { data, error } = await supabase.rpc('unidades_na_janela', { segundos: JANELA_S });
  msUltimaLeitura = Number(process.hrtime.bigint() - ini) / 1e6;
  dbLeituras++;
  if (error) throw new Error('supabase unidades_na_janela: ' + error.message);
  return Number(data || 0);
}

async function contarPedidosIp(ip) {
  const { data, error } = await supabase.rpc('contar_pedidos_ip', { p_ip: ip, segundos: JANELA_S });
  dbLeituras++;
  if (error) throw new Error('supabase contar_pedidos_ip: ' + error.message);
  return Number(data || 0);
}

async function pedidosPorIp() {
  const ini = process.hrtime.bigint();
  const { data, error } = await supabase.rpc('pedidos_por_ip_janela', { segundos: JANELA_S });
  msUltimaLeitura = Number(process.hrtime.bigint() - ini) / 1e6;
  dbLeituras++;
  if (error) throw new Error('supabase pedidos_por_ip_janela: ' + error.message);
  return data || [];
}

async function metricasDb() {
  const { data, error } = await supabase.rpc('metricas_banco');
  dbLeituras++;
  if (error) return { tamanho_mb: 0, paginas: 0, paginas_livres: 0, wal_mb: 0 };
  return Array.isArray(data) ? data[0] : data;
}

async function inserirMetrica(t) {
  const ini = process.hrtime.bigint();
  const { error } = await supabase.from('db_metricas').insert({
    t: new Date(t).toISOString(),
    tamanho_mb: dbTamanhoMB,
    paginas: dbPaginas,
    paginas_livres: dbPaginasLivres,
    wal_mb: dbWalMB,
    escritas: dbEscritas,
    leituras: dbLeituras,
    ms_ultima_escrita: +msUltimaEscrita.toFixed(3),
    ms_ultima_leitura: +msUltimaLeitura.toFixed(3),
  });
  msUltimaEscrita = Number(process.hrtime.bigint() - ini) / 1e6;
  dbEscritas++;
  if (error) console.error('erro metrica:', error.message);
}

async function limparAntigos() {
  const { error } = await supabase.rpc('limpar_pedidos_antigos');
  dbLeituras++;
  if (error) console.error('erro limpeza:', error.message);
}

let cpuAnt = process.cpuUsage();
let tAnt = process.hrtime.bigint();
let totalAnt = 0;
let unidadesCache = 0;

setInterval(async () => {
  const agora = process.hrtime.bigint();
  const cpu = process.cpuUsage(cpuAnt);
  cpuPct = Math.min(100, ((cpu.user + cpu.system) / (Number(agora - tAnt) / 1e3)) * 100);
  cpuAnt = process.cpuUsage(); tAnt = agora;
  rpsAtual = (totalReq - totalAnt) / 2; totalAnt = totalReq;

  try {
    const m = await metricasDb();
    if (m) {
      dbTamanhoMB = Number(m.tamanho_mb || 0);
      dbPaginas = Number(m.paginas || 0);
      dbPaginasLivres = Number(m.paginas_livres || 0);
      dbWalMB = Number(m.wal_mb || 0);
    }
    unidadesCache = await unidadesNaJanela();
  } catch (e) {
    console.error('amostragem:', e.message);
  }

  historico.push({
    t: Date.now(),
    carga: +((unidadesCache / LIM_CRITICO) * 100).toFixed(1),
    cpu: +cpuPct.toFixed(1),
    ram: +(process.memoryUsage().rss / 1048576).toFixed(1),
    rps: +rpsAtual.toFixed(1),
  });
  if (historico.length > 90) historico.shift();

  try {
    await inserirMetrica(Date.now());
    await limparAntigos();
  } catch (e) {
    console.error('metricas:', e.message);
  }
}, 2000).unref();

const dorme = (ms) => new Promise((r) => setTimeout(r, ms));

async function simularTrabalho(q, teste) {
  if (!SIMULAR_CARGA) return;
  const msCpu = teste === 'cpu' ? Math.min(2500, q * 40) : Math.min(400, q * 6);
  const fim = Date.now() + msCpu;
  while (Date.now() < fim) Math.sqrt(Math.random());
  const mb = teste === 'ram' ? Math.max(40, q) : q * 0.5;
  const bytes = Math.floor(mb * 1048576);
  if (retidoBytes + bytes <= MAX_RETIDO_MB * 1048576) {
    let buf = Buffer.alloc(bytes, 1);
    retidoBytes += bytes;
    setTimeout(() => { buf = null; retidoBytes -= bytes; }, 30_000).unref();
  }
  const msEspera = teste === 'lento' ? Math.min(4000, q * 100) : Math.min(1500, q * 20);
  await dorme(msEspera);
}

async function snapshot() {
  const agora = Date.now();
  const corte = new Date(agora - JANELA_S * 1000).toISOString();

  const { data: jan, error: errJan } = await supabase
    .from('pedidos').select('t,ip,item,quantidade,nivel,simulado')
    .gte('t', corte).order('t', { ascending: false }).limit(2000);
  dbLeituras++;
  if (errJan) throw new Error('supabase pedidos janela: ' + errJan.message);

  const unidades = (jan || []).reduce((s, p) => s + p.quantidade, 0);
  const dist = { OK: 0, ATENCAO: 0, CRITICO: 0 };
  (jan || []).forEach((p) => dist[p.nivel]++);
  const pct = {};
  for (const k of Object.keys(dist)) pct[k] = jan.length ? +((dist[k] / jan.length) * 100).toFixed(1) : 0;

  const porIpBruto = await pedidosPorIp();
  const porIp = porIpBruto.map((x) => ({
    ip: x.ip, pedidos: Number(x.pedidos), unidades: Number(x.unidades), simulado: !!x.simulado,
    ultimoHaSeg: Math.round((agora - new Date(x.ultimo).getTime()) / 1000),
    nivel: nivelFaixa(Number(x.pedidos), IP_ATENCAO, IP_CRITICO),
  }));

  reqEventos = reqEventos.filter((e) => e.t >= agora - JANELA_S * 1000);
  const ms = reqEventos.map((e) => e.ms).sort((a, b) => a - b);
  const p95 = ms.length ? ms[Math.min(ms.length - 1, Math.floor(ms.length * 0.95))] : 0;
  const erros5xx = reqEventos.filter((e) => e.status >= 500).length;

  const mem = process.memoryUsage();
  const ramPct = (mem.rss / (RAM_LIMITE_MB * 1048576)) * 100;
  const cargaPct = (unidades / LIM_CRITICO) * 100;
  const maxIp = porIp.length ? porIp[0].pedidos : 0;

  const nivelDb = {
    tamanho: nivelFaixa(dbTamanhoMB, 200, 400),
    escrita: nivelFaixa(msUltimaEscrita, 20, 50),
    leitura: nivelFaixa(msUltimaLeitura, 20, 50),
    wal: nivelFaixa(dbWalMB, 50, 100),
  };

  const nivel = {
    carga: nivelQtd(unidades),
    cpu: nivelFaixa(cpuPct, 70, 90),
    ram: nivelFaixa(ramPct, 70, 90),
    latencia: nivelFaixa(p95, 500, 1000),
    erros: nivelFaixa(erros5xx, 1, 5),
    ip: nivelFaixa(maxIp, IP_ATENCAO, IP_CRITICO),
    db: pior(Object.values(nivelDb)),
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
  add('db', 'Banco: ' + dbTamanhoMB + ' MB, escrita ' + msUltimaEscrita.toFixed(1) + ' ms, leitura ' + msUltimaLeitura.toFixed(1) + ' ms');

  const { data: ultimosRaw } = await supabase
    .from('pedidos').select('t,ip,item,quantidade,nivel,simulado')
    .order('t', { ascending: false }).limit(12);
  dbLeituras++;
  const ultimos = (ultimosRaw || []).map((p) => ({
    haSeg: Math.round((agora - new Date(p.t).getTime()) / 1000),
    ip: p.ip, item: p.item, quantidade: p.quantidade, nivel: p.nivel, simulado: !!p.simulado,
  }));

  const { count: totalNoBanco } = await supabase
    .from('pedidos').select('*', { count: 'exact', head: true });
  dbLeituras++;

  const { data: metricasDbRaw } = await supabase
    .from('db_metricas').select('*').order('t', { ascending: false }).limit(90);
  dbLeituras++;

  return {
    uptimeSeg: Math.floor((agora - iniciadoEm) / 1000),
    janelaSeg: JANELA_S,
    status: { nivel: pior(Object.values(nivel)), motivos },
    niveis: nivel,
    limites: { atencao: LIM_ATENCAO, critico: LIM_CRITICO },
    carga: { unidades, pct: +cargaPct.toFixed(1) },
    pedidos: {
      total: totalPedidos, unidadesTotal: totalUnidades, noBanco: totalNoBanco || 0,
      janela: { qtd: jan.length, unidades, ipsDistintos: porIpBruto.length, distribuicao: dist, pct },
      porIp,
      ultimos,
    },
    requisicoes: { total: totalReq, rps: +rpsAtual.toFixed(1), p95Ms: +p95.toFixed(1), erros5xx, classes },
    cpu: { pct: +cpuPct.toFixed(1), cores: os.cpus().length, load1: +os.loadavg()[0].toFixed(2) },
    ram: { rssMB: +(mem.rss / 1048576).toFixed(1), heapMB: +(mem.heapUsed / 1048576).toFixed(1),
      limiteMB: RAM_LIMITE_MB, pct: +ramPct.toFixed(1),
      maquinaTotalMB: Math.round(os.totalmem() / 1048576), maquinaLivreMB: Math.round(os.freemem() / 1048576) },
    eventLoopMs: Number.isFinite(loop.mean) ? +(loop.mean / 1e6).toFixed(1) : 0,
    banco: {
      provedor: 'supabase',
      tamanhoMB: dbTamanhoMB, paginas: dbPaginas, paginasLivres: dbPaginasLivres, walMB: dbWalMB,
      escritas: dbEscritas, leituras: dbLeituras,
      msUltimaEscrita: +msUltimaEscrita.toFixed(3), msUltimaLeitura: +msUltimaLeitura.toFixed(3),
      niveis: nivelDb,
      historico: (metricasDbRaw || []).reverse().map((m) => ({
        t: new Date(m.t).getTime(), tamanhoMB: Number(m.tamanho_mb), paginas: Number(m.paginas),
        paginasLivres: Number(m.paginas_livres), walMB: Number(m.wal_mb),
        escritas: Number(m.escritas), leituras: Number(m.leituras),
        msEscrita: Number(m.ms_ultima_escrita), msLeitura: Number(m.ms_ultima_leitura),
      })),
    },
    historico,
  };
}

const PAGINA_MONITOR = String.raw`<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Monitor ao vivo</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--txt:#1c1e21;--mut:#6b7280;--bd:#e5e7eb;--ok:#16a34a;--at:#d97706;--cr:#dc2626;--a:#2563eb}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#181b21;--txt:#e8eaed;--mut:#9aa0a6;--bd:#2a2e36;--ok:#4ade80;--at:#fbbf24;--cr:#f87171;--a:#60a5fa}}
*{box-sizing:border-box}body{margin:0;font-family:system-ui,sans-serif;background:var(--bg);color:var(--txt);padding:16px}
.top{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:12px}
h1{font-size:20px;margin:0}h2{font-size:15px;margin:20px 0 10px}.sub{color:var(--mut);font-size:12px}
a,button.link{color:var(--a);text-decoration:none;font-size:14px;background:none;border:0;cursor:pointer;padding:0}
#banner{border-radius:12px;padding:16px 20px;color:#fff;background:var(--ok)}
#banner.ATENCAO{background:var(--at)}#banner.CRITICO{background:var(--cr)}
#banner .t{font-size:24px;font-weight:700}#banner ul{margin:8px 0 0;padding-left:18px;font-size:14px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(155px,1fr))}
.charts{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px}
.lbl{font-size:12px;color:var(--mut)}.val{font-size:24px;font-weight:600;margin:4px 0 8px}
.barra{height:8px;background:var(--bd);border-radius:4px;overflow:hidden}.barra i{display:block;height:100%;border-radius:4px;transition:width .4s}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:7px 6px;border-bottom:1px solid var(--bd)}th{color:var(--mut);font-weight:500}
.wrap{overflow-x:auto}.tag{padding:2px 10px;border-radius:99px;font-size:12px;font-weight:600;color:#fff}
.OK{background:var(--ok)}.ATENCAO{background:var(--at)}.CRITICO{background:var(--cr)}
.sim{font-size:11px;color:var(--mut);border:1px solid var(--bd);border-radius:4px;padding:0 4px;margin-left:4px}
canvas{width:100%;height:130px;display:block}
.dist{display:flex;height:26px;border-radius:6px;overflow:hidden;background:var(--bd)}.dist div{display:flex;align-items:center;justify-content:center;color:#fff;font-size:12px;font-weight:600;transition:width .4s}
.leg{display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;font-size:13px}
</style></head><body>
<div class="top"><div><h1>📡 Monitor ao vivo</h1><div class="sub" id="info">Conectando...</div></div>
<div><a href="/pedido">Fazer pedido →</a> &nbsp; <button class="link" onclick="zerar()">Zerar dados</button></div></div>

<div id="banner"><div class="t" id="bTitulo">...</div><ul id="bMotivos"></ul></div>

<h2>Situação agora</h2>
<div class="grid" id="cards"></div>

<h2>Pedidos no último minuto: % bom / atenção / crítico</h2>
<div class="card"><div class="dist" id="dist"></div><div class="leg" id="legenda"></div></div>

<h2>Gráficos (últimos ~3 min)</h2>
<div class="charts">
<div class="card"><div class="lbl">Carga de pedidos (%)</div><canvas id="gCarga"></canvas></div>
<div class="card"><div class="lbl">CPU (%)</div><canvas id="gCpu"></canvas></div>
<div class="card"><div class="lbl">Memória RAM (MB)</div><canvas id="gRam"></canvas></div>
</div>

<h2>Banco de dados (Supabase)</h2>
<div class="grid" id="dbCards"></div>
<div class="charts" style="margin-top:12px">
<div class="card"><div class="lbl">Tamanho do banco (MB)</div><canvas id="gDbTam"></canvas></div>
<div class="card"><div class="lbl">Latência de escrita (ms)</div><canvas id="gDbEscrita"></canvas></div>
<div class="card"><div class="lbl">Latência de leitura (ms)</div><canvas id="gDbLeitura"></canvas></div>
</div>

<h2>Pedidos por IP (último minuto)</h2>
<div class="card wrap"><table><thead><tr><th>IP</th><th>Pedidos</th><th>Unidades</th><th>Último</th><th>Nível</th></tr></thead><tbody id="ips"></tbody></table></div>

<h2>Últimos pedidos</h2>
<div class="card wrap"><table><thead><tr><th>Quando</th><th>IP</th><th>Item</th><th>Qtd</th><th>Nível</th></tr></thead><tbody id="ultimos"></tbody></table></div>

<h2>Requisições</h2>
<div class="grid" id="reqs"></div>

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
function simples(titulo, valor, sub){
  return '<div class="card"><div class="lbl">'+titulo+'</div><div class="val">'+valor+'</div><div class="sub">'+sub+'</div></div>';
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
async function zerar(){ if(confirm('Apagar pedidos e métricas?')){ await fetch('/api/reset',{method:'POST'}); atualizar(); } }
async function atualizar(){
  try{
    var s = await (await fetch('/api/monitor')).json();
    var h = s.historico;
    $('info').textContent = 'Ao vivo · atualiza a cada 1 s · ligado há '+Math.floor(s.uptimeSeg/60)+' min '+(s.uptimeSeg%60)+' s · janela de '+s.janelaSeg+' s';
    $('banner').className = s.status.nivel; $('bTitulo').textContent = TITULO[s.status.nivel];
    $('bMotivos').innerHTML = s.status.motivos.map(function(m){return '<li>'+esc(m)+'</li>'}).join('') || '<li>Tudo funcionando normalmente.</li>';
    var n = s.niveis;
    $('cards').innerHTML =
      card('Carga de pedidos', s.carga.pct.toFixed(0)+'%', s.carga.pct, n.carga, s.carga.unidades+' un. no último minuto (atenção ≥ '+s.limites.atencao+', crítico ≥ '+s.limites.critico+')') +
      card('CPU', s.cpu.pct.toFixed(0)+'%', s.cpu.pct, n.cpu, s.cpu.cores+' núcleo(s) · load '+s.cpu.load1) +
      card('Memória RAM', s.ram.rssMB+' MB', s.ram.pct, n.ram, s.ram.pct.toFixed(0)+'% de '+s.ram.limiteMB+' MB · heap '+s.ram.heapMB+' MB') +
      card('Latência p95', s.requisicoes.p95Ms+' ms', s.requisicoes.p95Ms/10, n.latencia, 'event loop: '+s.eventLoopMs+' ms') +
      card('Erros 5xx', s.requisicoes.erros5xx, s.requisicoes.erros5xx*20, n.erros, 'no último minuto') +
      card('IP mais ativo', s.pedidos.porIp.length ? s.pedidos.porIp[0].pedidos+' pedidos' : '0', s.pedidos.porIp.length ? s.pedidos.porIp[0].pedidos*10 : 0, n.ip, s.pedidos.janela.ipsDistintos+' IP(s) diferentes');
    var j = s.pedidos.janela, p = j.pct;
    $('dist').innerHTML = j.qtd ? ['OK','ATENCAO','CRITICO'].map(function(k){
      return p[k] ? '<div style="width:'+p[k]+'%;background:'+COR[k]+'">'+(p[k]>=8 ? p[k].toFixed(0)+'%' : '')+'</div>' : '';}).join('')
      : '<div style="width:100%;color:var(--mut)">sem pedidos no último minuto</div>';
    $('legenda').innerHTML = ['OK','ATENCAO','CRITICO'].map(function(k){
      return '<span><span class="tag '+k+'">'+NOME[k]+'</span> '+p[k].toFixed(1)+'% ('+j.distribuicao[k]+')</span>';}).join('') +
      '<span class="sub">Total: '+j.qtd+' pedidos · '+j.unidades+' unidades</span>';
    grafico('gCarga', h.map(function(x){return x.carga}), Math.max(100, Math.max.apply(null,h.map(function(x){return x.carga}))), '--at');
    grafico('gCpu', h.map(function(x){return x.cpu}), 100, '--a');
    grafico('gRam', h.map(function(x){return x.ram}), s.ram.limiteMB, '--ok');
    var b = s.banco, bh = b.historico;
    $('dbCards').innerHTML =
      card('Tamanho do banco', b.tamanhoMB+' MB', b.tamanhoMB/4, b.niveis.tamanho, b.paginas+' páginas · '+b.paginasLivres+' livres') +
      card('Escrita (última)', b.msUltimaEscrita+' ms', b.msUltimaEscrita*2, b.niveis.escrita, b.escritas+' escritas desde que ligou') +
      card('Leitura (última)', b.msUltimaLeitura+' ms', b.msUltimaLeitura*2, b.niveis.leitura, b.leituras+' leituras desde que ligou') +
      card('WAL', b.walMB+' MB', b.walMB, b.niveis.wal, 'journal do Postgres') +
      simples('Pedidos no banco', s.pedidos.noBanco, 'persistidos no Supabase');
    grafico('gDbTam', bh.map(function(x){return x.tamanhoMB}), Math.max(10, Math.max.apply(null,bh.map(function(x){return x.tamanhoMB}))), '--a');
    grafico('gDbEscrita', bh.map(function(x){return x.msEscrita}), Math.max(10, Math.max.apply(null,bh.map(function(x){return x.msEscrita}))), '--at');
    grafico('gDbLeitura', bh.map(function(x){return x.msLeitura}), Math.max(10, Math.max.apply(null,bh.map(function(x){return x.msLeitura}))), '--ok');
    $('ips').innerHTML = s.pedidos.porIp.map(function(x){
      return '<tr><td>'+esc(x.ip)+(x.simulado?'<span class="sim">simulado</span>':'')+'</td><td>'+x.pedidos+'</td><td>'+x.unidades+'</td><td>há '+x.ultimoHaSeg+' s</td><td><span class="tag '+x.nivel+'">'+NOME[x.nivel]+'</span></td></tr>';
    }).join('') || '<tr><td colspan="5">Nenhum pedido no último minuto.</td></tr>';
    $('ultimos').innerHTML = s.pedidos.ultimos.map(function(x){
      return '<tr><td>há '+x.haSeg+' s</td><td>'+esc(x.ip)+(x.simulado?'<span class="sim">simulado</span>':'')+'</td><td>'+esc(x.item)+'</td><td><b>'+x.quantidade+'</b></td><td><span class="tag '+x.nivel+'">'+NOME[x.nivel]+'</span></td></tr>';
    }).join('') || '<tr><td colspan="5">Nenhum pedido ainda. Abra /pedido no celular.</td></tr>';
    var c = s.requisicoes.classes;
    $('reqs').innerHTML = simples('Total de requisições', s.requisicoes.total, 'desde que ligou') + simples('Requisições/s', s.requisicoes.rps, 'média dos últimos 2 s') +
      simples('2xx (sucesso)', c['2xx'], 'respostas boas') + simples('4xx (erro do cliente)', c['4xx'], 'pedido inválido') + simples('5xx (erro do servidor)', c['5xx'], 'falhas') +
      simples('Pedidos / unidades', s.pedidos.total+' / '+s.pedidos.unidadesTotal, 'desde que ligou');
  }catch(e){ $('info').textContent = 'Sem conexão com o servidor... tentando de novo'; }
}
atualizar(); setInterval(atualizar, 1000); window.addEventListener('resize', atualizar);
</script></body></html>`;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
const SEM_METRICA = new Set(['/api/monitor', '/health', '/api/reset']);

function ipDoCliente(req) {
  const xf = req.headers['x-forwarded-for'];
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
  const simulado = ipValido(body.ipSimulado);
  const ip = simulado ? body.ipSimulado : ipDoCliente(req);

  const doIp = await contarPedidosIp(ip);
  if (doIp >= IP_BLOQUEIO)
    return { status: 429, dados: { erro: 'IP ' + ip + ' bloqueado: ' + doIp + ' pedidos no último minuto' } };

  if ((await unidadesNaJanela()) >= LIM_CRITICO && Math.random() < 0.3)
    return { status: 503, dados: { erro: 'servidor sobrecarregado, tente de novo' } };

  if (teste === 'erro') {
    await dorme(200);
    return { status: 500, dados: { erro: 'falha simulada (teste de erro)' } };
  }

  const nivel = nivelQtd(quantidade);
  const t = Date.now();
  await inserirPedido(t, ip, item, quantidade, nivel, simulado);
  totalPedidos++; totalUnidades += quantidade;
  await simularTrabalho(quantidade, teste);

  const unidades = await unidadesNaJanela();
  return { status: 201, dados: {
    ok: true, id: totalPedidos, ip, simulado, item, quantidade, nivelPedido: nivel,
    carga: { unidades, pct: +((unidades / LIM_CRITICO) * 100).toFixed(1) },
    statusSistema: (await snapshot()).status.nivel,
  } };
}

http.createServer(async (req, res) => {
  const caminho = new URL(req.url, 'http://x').pathname;
  const ini = process.hrtime.bigint();

  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }

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

  let status = 200, dados;
  try {
    if (req.method === 'GET' && caminho === '/api/monitor') dados = await snapshot();
    else if (req.method === 'GET' && caminho === '/health') dados = { status: 'ok' };
    else if (req.method === 'POST' && caminho === '/api/pedido') ({ status, dados } = await criarPedido(await lerBody(req), req));
    else if (req.method === 'POST' && caminho === '/api/reset') {
      await supabase.from('pedidos').delete().neq('id', 0);
      await supabase.from('db_metricas').delete().neq('id', 0);
      historico.length = 0;
      totalReq = totalPedidos = totalUnidades = 0; classes['2xx'] = classes['4xx'] = classes['5xx'] = 0;
      dbEscritas = dbLeituras = 0; msUltimaEscrita = msUltimaLeitura = 0;
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
