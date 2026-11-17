// monitor.js - tudo de monitoramento: LOGS (com mascaramento), METRICAS e ALERTAS

const JANELA_MS = 60_000; // os alertas olham o ultimo minuto
const LIMITES = { p95Ms: 500, erros5xx: 3, rssMB: 150, dbLentas: 2 };

const inicio = Date.now();
const total = { req: 0, erros5xx: 0, db: 0, dbErros: 0, dbLentas: 0 };
const porRota = {};      // "GET /api/users/:id" -> contadores
const porConsulta = {};  // "listar_usuarios"    -> contadores
let reqEventos = [];     // {t, ms, erro}  (ultimo minuto)
let dbEventos = [];      // {t, lenta}     (ultimo minuto)
let banco = { ok: true, pingMs: 0 };

// ---------- 1) LOGS com mascaramento de dados sensiveis ----------
const CAMPOS_SENSIVEIS = /senha|password|token|cpf|cartao|card|authorization/i;

function mascararTexto(s) {
  return s
    .replace(/([\w.+-])[\w.+-]*@([\w-]+\.[\w.-]+)/g, '$1***@$2') // e-mail -> a***@site.com
    .replace(/\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b/g, '***.***.***-**'); // CPF
}
function mascarar(valor, chave = '') {
  if (CAMPOS_SENSIVEIS.test(chave)) return '[MASCARADO]';
  if (typeof valor === 'string') return mascararTexto(valor);
  if (valor && typeof valor === 'object')
    return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, mascarar(v, k)]));
  return valor;
}
function log(nivel, mensagem, campos = {}) {
  console.log(JSON.stringify({ hora: new Date().toISOString(), nivel, mensagem, ...mascarar(campos) }));
}

// ---------- 2) METRICAS ----------
// CARDINALIDADE: 'rota' deve ser o padrao (/api/users/:id) e 'nome' um nome fixo
// da consulta. Nunca use URL real, id, e-mail ou SQL completo como identificador.
function registrarRequisicao(metodo, rota, status, ms) {
  const erro = status >= 500;
  total.req++;
  if (erro) total.erros5xx++;
  const r = (porRota[`${metodo} ${rota}`] ||= { qtd: 0, erros: 0, somaMs: 0, maxMs: 0 });
  r.qtd++; r.somaMs += ms; r.maxMs = Math.max(r.maxMs, ms);
  if (erro) r.erros++;
  reqEventos.push({ t: Date.now(), ms, erro });
}

function registrarConsulta(nome, ms, erro, lenta) {
  total.db++;
  if (erro) total.dbErros++;
  if (lenta) total.dbLentas++;
  const q = (porConsulta[nome] ||= { qtd: 0, erros: 0, lentas: 0, somaMs: 0, maxMs: 0 });
  q.qtd++; q.somaMs += ms; q.maxMs = Math.max(q.maxMs, ms);
  if (erro) q.erros++;
  if (lenta) q.lentas++;
  dbEventos.push({ t: Date.now(), lenta });
}

const definirBanco = (ok, pingMs) => { banco = { ok, pingMs: Number(pingMs.toFixed(2)) }; };

// ---------- 3) ALERTAS ----------
const estadoAlertas = {};

function avaliarAlertas() {
  const corte = Date.now() - JANELA_MS;
  reqEventos = reqEventos.filter((e) => e.t >= corte);
  dbEventos = dbEventos.filter((e) => e.t >= corte);

  const ordenadas = reqEventos.map((e) => e.ms).sort((a, b) => a - b);
  const p95 = ordenadas.length ? ordenadas[Math.min(ordenadas.length - 1, Math.floor(ordenadas.length * 0.95))] : 0;
  const erros = reqEventos.filter((e) => e.erro).length;
  const lentas = dbEventos.filter((e) => e.lenta).length;
  const rssMB = process.memoryUsage().rss / 1048576;

  const regras = [
    { nome: 'latencia_alta',     texto: `p95 > ${LIMITES.p95Ms} ms (ultimo minuto)`,       ativo: p95 > LIMITES.p95Ms },
    { nome: 'muitos_erros_5xx',  texto: `${LIMITES.erros5xx} ou mais erros 5xx no ultimo minuto`, ativo: erros >= LIMITES.erros5xx },
    { nome: 'memoria_alta',      texto: `memoria RSS > ${LIMITES.rssMB} MB`,               ativo: rssMB > LIMITES.rssMB },
    { nome: 'banco_lento',       texto: `${LIMITES.dbLentas} ou mais consultas lentas no ultimo minuto`, ativo: lentas >= LIMITES.dbLentas },
    { nome: 'banco_indisponivel', texto: 'ping no banco falhou',                           ativo: !banco.ok },
  ];
  for (const r of regras) {
    if (r.ativo !== !!estadoAlertas[r.nome]) {
      estadoAlertas[r.nome] = r.ativo;
      log(r.ativo ? 'error' : 'info', r.ativo ? 'ALERTA_DISPARADO' : 'ALERTA_RESOLVIDO', { alerta: r.nome, regra: r.texto });
    }
  }
  return { regras, p95, erros, lentas, rssMB };
}
setInterval(avaliarAlertas, 2000).unref();

// ---------- resumo (GET /monitor) ----------
const lista = (obj, chave) => Object.entries(obj).map(([nome, v]) => ({
  [chave]: nome, qtd: v.qtd, erros: v.erros, ...(v.lentas !== undefined && { lentas: v.lentas }),
  mediaMs: +(v.somaMs / v.qtd).toFixed(1), maxMs: +v.maxMs.toFixed(1),
}));

function resumo() {
  const a = avaliarAlertas();
  const mem = process.memoryUsage();
  return {
    uptimeSegundos: Math.floor((Date.now() - inicio) / 1000),
    memoria: { rssMB: +(mem.rss / 1048576).toFixed(1), heapUsadoMB: +(mem.heapUsed / 1048576).toFixed(1) },
    requisicoes: {
      total: total.req, erros5xx: total.erros5xx,
      ultimoMinuto: { qtd: reqEventos.length, erros5xx: a.erros, p95Ms: +a.p95.toFixed(1) },
      porRota: lista(porRota, 'rota'),
    },
    banco: {
      ok: banco.ok, pingMs: banco.pingMs,
      consultas: total.db, erros: total.dbErros, lentas: total.dbLentas,
      porConsulta: lista(porConsulta, 'consulta'),
    },
    alertas: a.regras.map((r) => ({ alerta: r.nome, regra: r.texto, disparado: r.ativo })),
  };
}

module.exports = { log, registrarRequisicao, registrarConsulta, definirBanco, resumo };