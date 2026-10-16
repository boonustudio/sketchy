'use strict';
/* Sketchy server: Express (static files) + ws (realtime game) + MongoDB Atlas (persistence).
   The server is authoritative: it owns words, timers, scoring and guess checking.
   Live rooms are kept in memory; parties and finished games are saved to MongoDB. */
try { require('dotenv').config(); } catch (_) {}
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const { MongoClient } = require('mongodb');
const WORDS = require('./words');

const PORT = +process.env.PORT || 3000;
const MAX_PLAYERS = 12;
const GRACE_MS = 45000;

/* ---------- utils ---------- */
const now = () => Date.now();
const rnd = (n) => Math.floor(Math.random() * n);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const shuffle = (a) => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = rnd(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const norm = (s) => String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '');
const cap = (w) => w.charAt(0).toUpperCase() + w.slice(1);
function editDist1(a, b) {
  if (a === b) return false;
  const la = a.length, lb = b.length;
  if (Math.abs(la - lb) > 1) return false;
  let i = 0; while (i < la && i < lb && a[i] === b[i]) i++;
  if (la === lb) return a.slice(i + 1) === b.slice(i + 1);
  return la > lb ? a.slice(i + 1) === b.slice(i) : b.slice(i + 1) === a.slice(i);
}
function hashStr(s) { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; }
function seeded(seed) { let a = seed || 1; return () => { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }
function hintCount(frac, letters) { if (frac < .3) return 0; return clamp(1 + Math.floor((frac - .3) / .2), 0, Math.max(1, Math.floor(letters * .4))); }
function blanksText(word, k, seed) {
  const idx = []; for (let i = 0; i < word.length; i++) if (/[a-z0-9]/i.test(word[i])) idx.push(i);
  const r = seeded(hashStr(word + seed)), order = idx.slice();
  for (let i = order.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [order[i], order[j]] = [order[j], order[i]]; }
  const show = new Set(order.slice(0, k)); const out = [];
  for (let i = 0; i < word.length; i++) { const ch = word[i]; out.push(/[a-z0-9]/i.test(ch) ? (show.has(i) ? ch.toUpperCase() : '_') : (ch === ' ' ? ' ' : ch)); }
  return out.join(' ');
}
function pick3(used) {
  const u = new Set((used || []).map(norm));
  const out = [], tries = new Set();
  while (out.length < 3 && tries.size < WORDS.length) {
    const w = WORDS[rnd(WORDS.length)]; tries.add(w);
    if (!u.has(norm(w)) && !out.includes(w)) out.push(w);
  }
  while (out.length < 3) out.push(WORDS[rnd(WORDS.length)]);
  return out;
}
const ORDER_PTS = [100, 80, 60, 40, 30, 20];
const cleanName = (n) => String(n || '').replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 14);
const validUid = (u) => typeof u === 'string' && /^[A-Za-z0-9_-]{8,40}$/.test(u);
const log = (...a) => console.log(new Date().toISOString(), ...a);

/* ---------- MongoDB ---------- */
const col = { parties: null, games: null };
async function initMongo() {
  const uri = process.env.MONGODB_URI;
  if (!uri) { console.warn('MONGODB_URI not set: running without persistence (games still work).'); return; }
  try {
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000 });
    await client.connect();
    const db = client.db(process.env.MONGODB_DB || 'sketchy');
    col.parties = db.collection('parties');
    col.games = db.collection('games');
    await col.parties.createIndex({ code: 1 }, { unique: true });
    await col.parties.createIndex({ updatedAt: 1 }, { expireAfterSeconds: 86400 }); // parties expire after 24h idle
    await col.games.createIndex({ finishedAt: -1 });
    log('MongoDB Atlas connected, db =', db.databaseName);
  } catch (e) {
    console.error('MongoDB connection failed (continuing without persistence):', e.message);
    col.parties = col.games = null;
  }
}
function persist(r) {
  if (!col.parties) return;
  col.parties.updateOne({ code: r.code }, {
    $set: { host: r.host, phase: r.phase, set: r.set, round: r.round, updatedAt: new Date(),
      players: [...r.players.values()].filter((p) => !p.bot).map((p) => ({ id: p.id, name: p.name, score: p.score })) },
    $setOnInsert: { code: r.code, createdAt: new Date() },
  }, { upsert: true }).catch((e) => console.error('persist failed:', e.message));
}
function saveGame(r) {
  if (!col.games) return;
  const players = [...r.players.values()].sort((a, b) => b.score - a.score).map((p) => ({ name: p.name, score: p.score, bot: !!p.bot }));
  col.games.insertOne({ code: r.code, finishedAt: new Date(), rounds: r.set.rounds, timePerTurn: r.set.time, players, winner: players[0] ? players[0].name : null })
    .catch((e) => console.error('saveGame failed:', e.message));
}

/* ---------- rooms ---------- */
const rooms = new Map();
const defaultSet = () => ({ time: 80, rounds: 1, hints: true });
function newRoom(code, host, set) {
  return { code, host, phase: 'lobby', gen: 0, round: 0, turn: 0, order: [], drawer: '', words: [], word: '', hint: '',
    set: set || defaultSet(), tStart: 0, tEnd: 0, pickEnd: 0, resEnd: 0, used: [], res: null,
    players: new Map(), strokes: [], map: {}, kick: null, banned: new Set(), plan: null, nextScribble: 0, scribbles: 0, condAt: 0, emptySince: 0 };
}
const tk = (r) => `${r.gen}.${r.round}.${r.turn}`;
const alive = (p) => !!p && (p.bot || (p.ws && p.ws.readyState === 1) || now() - p.seen < GRACE_MS);
const humans = (r) => [...r.players.values()].filter((p) => !p.bot);
const bots = (r) => [...r.players.values()].filter((p) => p.bot);
const plist = (r) => [...r.players.values()].sort((a, b) => a.joined - b.joined || (a.id < b.id ? -1 : 1));
const sendTo = (p, o) => { if (p && p.ws && p.ws.readyState === 1) { try { p.ws.send(JSON.stringify(o)); } catch (_) {} } };
const toAll = (r, o, except) => { for (const p of humans(r)) if (p.id !== except) sendTo(p, o); };
const knows = (r, p) => r.phase === 'draw' && (p.id === r.drawer || p.gKey === tk(r));

function kickNeeded(r) {
  const k = r.kick; if (!k) return 1;
  const el = humans(r).filter((p) => alive(p) && p.id !== k.target);
  return Math.max(1, Math.floor(el.length / 2) + 1);
}
function view(r, p) {
  const k = r.kick, key = tk(r), known = knows(r, p), inDraw = r.phase === 'draw';
  return {
    t: 'state', now: now(), code: r.code, phase: r.phase, host: r.host, you: p.id, gen: r.gen, round: r.round, turn: r.turn, tk: key,
    drawer: r.drawer, set: r.set, tStart: r.tStart, tEnd: r.tEnd, pickEnd: r.pickEnd, resEnd: r.resEnd,
    words: r.phase === 'pick' && p.id === r.drawer ? r.words : [],
    word: known ? r.word : '', wordLen: inDraw ? r.word.replace(/[^a-z0-9]/gi, '').length : 0, hint: inDraw ? r.hint : '',
    res: r.res,
    players: plist(r).map((q) => ({ id: q.id, name: q.name, score: q.score, bot: !!q.bot, alive: alive(q), got: inDraw && q.gKey === key, drawing: inDraw && q.id === r.drawer })),
    kick: k ? { target: k.target, targetName: k.targetName, voters: k.voters.length, voted: k.voters.includes(p.id), startAt: k.startAt, needed: kickNeeded(r) } : null,
  };
}
const push = (r) => { for (const p of humans(r)) if (p.ws) sendTo(p, view(r, p)); };
const sys = (r, txt) => toAll(r, { t: 'feed', kind: 'sys', txt });

function genCode() { const a = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; let s = ''; for (let i = 0; i < 5; i++) s += a[rnd(a.length)]; return s; }
function addPlayer(r, id, name, ws) {
  const p = { id, name, bot: false, score: 0, ws, seen: now(), joined: now(), gKey: '', gPts: 0, gAt: 0 };
  r.players.set(id, p); return p;
}
function deleteRoom(r) {
  rooms.delete(r.code);
  if (col.parties) col.parties.deleteOne({ code: r.code }).catch(() => {});
}
function removePlayer(r, id, opts = {}) {
  const p = r.players.get(id); if (!p) return;
  r.players.delete(id);
  if (opts.ban) r.banned.add(id);
  if (p.ws) { if (opts.msg) sendTo(p, { t: 'kicked', msg: opts.msg }); if (p.ws.ctx) p.ws.ctx.code = null; p.ws = null; }
  if (!humans(r).length) { deleteRoom(r); return; }
  if (r.host === id) { const nx = humans(r).filter(alive).sort((a, b) => a.joined - b.joined)[0] || humans(r)[0]; if (nx) r.host = nx.id; }
  persist(r); push(r);
}

/* ---------- game flow ---------- */
function guessPts(r, rank) {
  if (r.tEnd) { const tot = r.tEnd - r.tStart, left = clamp((r.tEnd - now()) / tot, 0, 1); return Math.round(20 + 80 * left) + (rank === 0 ? 25 : 0); }
  return ORDER_PTS[Math.min(rank, ORDER_PTS.length - 1)];
}
const gotCount = (r) => [...r.players.values()].filter((p) => p.gKey === tk(r)).length;
function hintFor(r) {
  const letters = r.word.replace(/[^a-z0-9]/gi, '').length, total = r.tEnd ? r.tEnd - r.tStart : 90000;
  const frac = clamp((now() - r.tStart) / total, 0, 1);
  return blanksText(r.word, r.set.hints ? hintCount(frac, letters) : 0, tk(r));
}
function finish(r) {
  r.phase = 'final'; r.drawer = ''; r.word = ''; r.words = []; r.strokes = []; r.map = {};
  saveGame(r); persist(r); push(r);
}
function nextTurn(r, turn) {
  const N = r.order.length, total = N * r.set.rounds;
  while (turn < total) { const p = r.players.get(r.order[turn % N]); if (p && alive(p)) break; turn++; }
  const aliveN = [...r.players.values()].filter(alive).length;
  if (!N || turn >= total || aliveN < 2) return finish(r);
  const drawer = r.order[turn % N], bot = drawer.startsWith('bot_');
  r.phase = 'pick'; r.round = Math.floor(turn / N) + 1; r.turn = turn; r.drawer = drawer;
  r.words = pick3(r.used); r.word = ''; r.hint = ''; r.res = null; r.tStart = 0; r.tEnd = 0;
  r.pickEnd = now() + (bot ? 1800 : 20000); r.strokes = []; r.map = {}; r.plan = null; r.condAt = 0;
  push(r);
}
function startGame(r) {
  const ids = [...r.players.values()].filter(alive).map((p) => p.id);
  if (ids.length < 2) return false;
  r.order = shuffle(ids); r.gen++; r.used = [];
  for (const p of r.players.values()) { p.score = 0; p.gKey = ''; p.gPts = 0; p.gAt = 0; }
  r.round = 0; r.turn = 0;
  nextTurn(r, 0); persist(r); return true;
}
function pickWord(r, i) {
  if (r.phase !== 'pick') return;
  const w = r.words[i] || r.words[0]; if (!w) return;
  const t = now();
  r.phase = 'draw'; r.word = w; r.words = []; r.used.push(w); r.tStart = t; r.tEnd = r.set.time ? t + r.set.time * 1000 : 0;
  r.strokes = []; r.map = {}; r.plan = null; r.scribbles = 0; r.nextScribble = 0; r.condAt = 0; r.hint = hintFor(r);
  push(r);
}
function endTurn(r) {
  if (r.phase !== 'draw') return;
  const key = tk(r);
  const got = [...r.players.values()].filter((p) => p.gKey === key).sort((a, b) => a.gAt - b.gAt);
  const dp = 50 * got.length;
  for (const p of got) p.score += p.gPts || 0;
  const d = r.players.get(r.drawer); if (d && dp) d.score += dp;
  r.res = { word: r.word, got: got.map((p) => ({ id: p.id, name: p.name, pts: p.gPts || 0 })), dp, drawer: r.drawer };
  r.phase = 'result'; r.resEnd = now() + 7000;
  persist(r); push(r);
}
function award(r, p) {
  const pts = guessPts(r, gotCount(r));
  p.gKey = tk(r); p.gPts = pts; p.gAt = now();
  sendTo(p, { t: 'feed', kind: 'ok', txt: `You guessed it! +${pts}` });
  toAll(r, { t: 'feed', kind: 'ok', txt: `${p.name} guessed it!` }, p.id);
  push(r);
}
function guess(r, p, txt) {
  txt = String(txt || '').trim().slice(0, 40); if (!txt) return;
  if (r.phase !== 'draw' || p.id === r.drawer || p.gKey === tk(r)) return chat(r, p, txt);
  const a = norm(txt), w = norm(r.word);
  if (a === w) return award(r, p);
  if (w.length > 3 && editDist1(a, w)) return sendTo(p, { t: 'feed', kind: 'near', txt: 'So close! Try again.' });
  toAll(r, { t: 'feed', kind: 'g', n: p.name, txt });
}
function chat(r, p, txt) {
  txt = String(txt || '').trim().slice(0, 200); if (!txt) return;
  const known = knows(r, p);
  if (known && norm(r.word).length > 1 && norm(txt).includes(norm(r.word))) txt = txt.replace(/./g, '*');
  const m = { t: 'chat', from: p.id, n: p.name, txt };
  if (known) { for (const q of humans(r)) if (knows(r, q)) sendTo(q, m); } // drawer + people who already guessed chat privately
  else toAll(r, m);
}

/* ---------- drawing ops (validated + mirrored so late joiners get the picture) ---------- */
function applyOp(r, m) {
  if (!m || typeof m !== 'object') return null;
  if (m.k === 'x') { r.strokes = []; r.map = {}; return { k: 'x' }; }
  if (m.k === 'u') { const s = r.strokes.pop(); if (s) delete r.map[s.id]; return { k: 'u' }; }
  if (m.k !== 's') return null;
  const int = (v, a, b) => Math.min(b, Math.max(a, Math.round(+v) || 0));
  const id = int(m.id, 0, 5000), st = int(m.st, 0, 20000);
  const p = Array.isArray(m.p) ? m.p.slice(0, 400).map((v) => int(v, 0, 1000)) : [];
  if (p.length % 2) p.pop();
  let s = r.map[id];
  if (!s) {
    if (r.strokes.length >= 3000) return null;
    s = { id, c: int(m.c, 0, 9), s: int(m.s, 0, 2), e: m.e ? 1 : 0, f: m.f ? 1 : 0, p: [], end: 0 };
    r.strokes.push(s); r.map[id] = s;
  }
  if (st > s.p.length / 2 || s.p.length > 40000) return null;
  s.p.length = st * 2; for (const v of p) s.p.push(v); s.end = m.end ? 1 : 0;
  return { k: 's', id, c: s.c, s: s.s, e: s.e, f: s.f, st, p, end: s.end };
}

/* ---------- demo bots ---------- */
const BOTS = [{ id: 'bot_milo', name: 'Milo' }, { id: 'bot_zara', name: 'Zara' }];
function setBots(r, on) {
  for (const b of BOTS) {
    if (on && !r.players.has(b.id)) r.players.set(b.id, { id: b.id, name: b.name, bot: true, score: 0, ws: null, seen: now(), joined: now() + (b.id === 'bot_milo' ? 1 : 2), gKey: '', gPts: 0, gAt: 0 });
    if (!on) r.players.delete(b.id);
  }
}
function botsTick(r) {
  if (r.phase !== 'draw') return;
  const key = tk(r), t = now();
  if (!r.plan || r.plan.tk !== key) {
    const total = r.tEnd ? r.tEnd - r.tStart : 60000, items = [];
    for (const b of bots(r)) {
      if (b.id === r.drawer) continue;
      if (Math.random() < .8) items.push({ id: b.id, kind: 'wrong', at: r.tStart + total * (.1 + Math.random() * .4) });
      if (Math.random() < .65) items.push({ id: b.id, kind: 'ok', at: r.tStart + total * (.2 + Math.random() * .6) });
    }
    r.plan = { tk: key, items };
  }
  for (const it of r.plan.items) {
    if (it.done || t < it.at) continue; it.done = true;
    const b = r.players.get(it.id); if (!b) continue;
    if (it.kind === 'ok') { if (b.gKey !== key) award(r, b); }
    else { const w = WORDS[rnd(WORDS.length)] || 'hmm'; if (norm(w) !== norm(r.word)) toAll(r, { t: 'feed', kind: 'g', n: b.name, txt: w }); }
  }
  if (r.drawer.startsWith('bot_') && t >= r.nextScribble && r.scribbles < 16) {
    r.nextScribble = t + 600 + rnd(700); r.scribbles++;
    let x = 150 + rnd(700), y = 150 + rnd(700); const p = [x, y];
    for (let i = 0; i < 11; i++) { x = clamp(x + rnd(220) - 110, 20, 980); y = clamp(y + rnd(220) - 110, 20, 980); p.push(x, y); }
    const op = applyOp(r, { k: 's', id: r.strokes.length, c: rnd(10), s: rnd(3), e: 0, f: 0, st: 0, p, end: 1 });
    if (op) toAll(r, { t: 'stroke', op });
  }
}

/* ---------- votekick ---------- */
function resolveKick(r) {
  const k = r.kick; if (!k) return;
  const target = r.players.get(k.target);
  if (!target) { r.kick = null; return push(r); }
  const elig = new Set(humans(r).filter((p) => alive(p) && p.id !== k.target).map((p) => p.id));
  const yes = k.voters.filter((v) => elig.has(v)).length;
  if (yes >= kickNeeded(r)) {
    r.kick = null; sys(r, `${k.targetName} was voted out.`);
    removePlayer(r, k.target, { ban: true, msg: 'You were voted out of the party.' });
  } else if (now() - k.startAt > 30000) { r.kick = null; push(r); }
}

/* ---------- 500ms game loop ---------- */
const drawerGone = (r) => { const d = r.players.get(r.drawer); return !d || !alive(d); };
function guessersDone(r) {
  const gs = [...r.players.values()].filter((p) => p.id !== r.drawer && alive(p));
  return gs.length > 0 && gs.every((p) => p.gKey === tk(r));
}
function tick(r) {
  const t = now(), hum = humans(r);
  if (!hum.some(alive)) { if (!r.emptySince) r.emptySince = t; else if (t - r.emptySince > 180000) return deleteRoom(r); } else r.emptySince = 0;
  if (r.phase === 'lobby') {
    let ch = false;
    for (const p of hum) if (!alive(p) && t - p.seen > 60000) { r.players.delete(p.id); ch = true; }
    if (ch) { if (!r.players.has(r.host)) { const nx = humans(r)[0]; if (nx) r.host = nx.id; } push(r); }
  }
  const h = r.players.get(r.host);
  if (!h || !alive(h)) { const nx = humans(r).filter(alive).sort((a, b) => a.joined - b.joined)[0]; if (nx && nx.id !== r.host) { r.host = nx.id; push(r); } }
  if (r.kick) resolveKick(r);
  botsTick(r);
  if (r.phase === 'pick') {
    if (t >= r.pickEnd || (drawerGone(r) && t > r.pickEnd - 18000)) pickWord(r, 0);
  } else if (r.phase === 'draw') {
    if (r.tEnd && t >= r.tEnd) return endTurn(r);
    if (guessersDone(r) || drawerGone(r)) { if (!r.condAt) r.condAt = t; else if (t - r.condAt >= 400) return endTurn(r); } else r.condAt = 0;
    const hn = hintFor(r); if (hn !== r.hint) { r.hint = hn; push(r); }
  } else if (r.phase === 'result') {
    if (t >= r.resEnd) nextTurn(r, r.turn + 1);
  }
}
setInterval(() => { for (const r of [...rooms.values()]) { try { tick(r); } catch (e) { console.error('tick error', e); } } }, 500);

/* ---------- HTTP + WebSocket ---------- */
const app = express();
app.disable('x-powered-by');
app.get('/healthz', (_q, res) => res.json({ ok: true, rooms: rooms.size, mongo: !!col.parties }));
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });

const err = (ws, msg, extra) => { try { ws.send(JSON.stringify(Object.assign({ t: 'error', msg }, extra || {}))); } catch (_) {} };

async function restoreRoom(code, uid) { // after a server restart the host can reopen the party from MongoDB
  if (!col.parties) return null;
  try {
    const d = await col.parties.findOne({ code });
    if (!d || d.host !== uid) return null;
    const r = newRoom(code, uid, Object.assign(defaultSet(), d.set || {}));
    rooms.set(code, r); return r;
  } catch (_) { return null; }
}
function attach(ws, r, id, name) {
  let p = r.players.get(id);
  if (p) {
    if (p.ws && p.ws !== ws) { const old = p.ws; p.ws = null; try { old.ctx.code = null; old.close(); } catch (_) {} }
    p.ws = ws; p.seen = now(); if (name) p.name = name;
  } else {
    if (humans(r).length >= MAX_PLAYERS) return null;
    p = addPlayer(r, id, name, ws);
  }
  ws.ctx = { code: r.code, id };
  return p;
}
function afterJoin(r, p) {
  sendTo(p, { t: 'joined', code: r.code });
  push(r);
  if (r.phase === 'draw' || r.phase === 'result') sendTo(p, { t: 'canvas', strokes: r.strokes });
}

wss.on('connection', (ws) => {
  ws.ctx = { code: null, id: null }; ws.alive = true; ws.cnt = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', async (raw) => {
    if (++ws.cnt > 150) return; // flood guard (reset every second)
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    try { await handle(ws, m); } catch (e) { console.error('handler error', m.t, e); }
  });
  ws.on('close', () => {
    const r = rooms.get(ws.ctx.code); if (!r) return;
    const p = r.players.get(ws.ctx.id);
    if (p && p.ws === ws) { p.ws = null; p.seen = now(); push(r); }
  });
  ws.on('error', () => {});
});
setInterval(() => { for (const ws of wss.clients) ws.cnt = 0; }, 1000);
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (_) {} } }, 25000);

async function handle(ws, m) {
  if (m.t === 'create') {
    const name = cleanName(m.name), uid = m.uid;
    if (!name) return err(ws, 'Enter your name first.');
    if (!validUid(uid)) return err(ws, 'Bad player id. Reload the page.');
    if (ws.ctx.code) leaveCurrent(ws);
    let code = ''; for (let i = 0; i < 20; i++) { const c = genCode(); if (!rooms.has(c)) { code = c; break; } }
    if (!code) return err(ws, 'Could not make a party code. Try again.');
    const r = newRoom(code, uid); rooms.set(code, r);
    const p = attach(ws, r, uid, name); persist(r); return afterJoin(r, p);
  }
  if (m.t === 'join') {
    const name = cleanName(m.name), uid = m.uid;
    const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!name) return err(ws, 'Enter your name first.');
    if (!validUid(uid)) return err(ws, 'Bad player id. Reload the page.');
    if (code.length < 4) return err(ws, 'Enter the party code.');
    let r = rooms.get(code) || await restoreRoom(code, uid);
    if (!r) return err(ws, m.re ? 'That party is gone.' : 'No party with that code. Check it and try again.', { gone: !!m.re });
    if (r.banned.has(uid)) return err(ws, 'You were removed from that party.', { gone: !!m.re });
    if (ws.ctx.code && ws.ctx.code !== code) leaveCurrent(ws);
    const p = attach(ws, r, uid, name);
    if (!p) return err(ws, 'That party is full.');
    if (!r.players.has(r.host) || !alive(r.players.get(r.host))) r.host = humans(r).filter(alive).sort((a, b) => a.joined - b.joined)[0].id;
    return afterJoin(r, p);
  }
  const r = rooms.get(ws.ctx.code); if (!r) return;
  const p = r.players.get(ws.ctx.id); if (!p) return;
  p.seen = now();
  const host = r.host === p.id;

  switch (m.t) {
    case 'leave': return leaveCurrent(ws);
    case 'settings': {
      if (!host || r.phase !== 'lobby' || !m.set) return;
      const s = m.set;
      r.set = { time: [0, 60, 80, 120].includes(+s.time) ? +s.time : 80, rounds: [1, 2, 3, 5].includes(+s.rounds) ? +s.rounds : 1, hints: !!s.hints };
      persist(r); return push(r);
    }
    case 'bots': if (!host || r.phase !== 'lobby') return; setBots(r, !!m.on); return push(r);
    case 'start':
      if (!host || (r.phase !== 'lobby' && r.phase !== 'final')) return;
      if (!startGame(r)) err(ws, 'You need at least 2 players to start.'); return;
    case 'lobby': if (host && r.phase === 'final') { r.phase = 'lobby'; r.drawer = ''; r.word = ''; r.words = []; r.res = null; r.round = 0; r.turn = 0; persist(r); push(r); } return;
    case 'pick': if (r.phase === 'pick' && r.drawer === p.id) pickWord(r, clamp(+m.i || 0, 0, 2)); return;
    case 'endturn': if (r.phase === 'draw' && r.drawer === p.id) endTurn(r); return;
    case 'skip': if (host && r.phase === 'result') nextTurn(r, r.turn + 1); return;
    case 'stroke': {
      if (r.phase !== 'draw' || r.drawer !== p.id) return;
      const op = applyOp(r, m.op); if (op) toAll(r, { t: 'stroke', op }, p.id); return;
    }
    case 'guess': return guess(r, p, m.txt);
    case 'chat': return chat(r, p, m.txt);
    case 'kick': {
      const tg = r.players.get(m.target);
      if (!tg || tg.bot || tg.id === p.id) return;
      if (r.kick && now() - r.kick.startAt < 30000) return err(ws, 'A vote is already in progress.');
      r.kick = { target: tg.id, targetName: tg.name, voters: [p.id], startAt: now() };
      toAll(r, { t: 'feed', kind: 'sys', txt: `${p.name} started a vote to kick ${tg.name}.` });
      resolveKick(r); return push(r);
    }
    case 'vote': {
      const k = r.kick; if (!k || p.id === k.target) return;
      k.voters = k.voters.filter((v) => v !== p.id); if (m.yes) k.voters.push(p.id);
      resolveKick(r); return push(r);
    }
  }
}
function leaveCurrent(ws) {
  const r = rooms.get(ws.ctx.code); const id = ws.ctx.id;
  ws.ctx.code = null;
  if (!r || !r.players.has(id)) return;
  const p = r.players.get(id); if (p && p.ws === ws) p.ws = null;
  removePlayer(r, id);
}

initMongo().finally(() => {
  server.listen(PORT, () => log(`Sketchy running on http://localhost:${PORT}`));
});
process.on('unhandledRejection', (e) => console.error('unhandledRejection', e));
