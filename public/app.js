'use strict';
/*
 * Tapple Online — multiplayer via MQTT público (WebSocket seguro).
 *
 * Arquitetura:
 *  - Quem cria a sala é o ANFITRIÃO. O aparelho dele é o "juiz": recebe as ações
 *    dos jogadores (topic .../action), valida e publica o estado completo do jogo
 *    (topic .../state, mensagem *retained*, então quem entra/recarrega recebe na hora).
 *  - Presença de cada jogador: .../p/<id> retained "1"/"0" (com last-will "0").
 *  - Relógio: o anfitrião manda .../clock com seu horário; os outros estimam a
 *    diferença para mostrar o cronômetro igual em todos.
 *  - Se o anfitrião sumir, qualquer jogador pode "assumir como anfitrião".
 */
(() => {
  const BROKERS = [
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'wss://test.mosquitto.org:8081/mqtt',
  ];
  const ROOT = 'tapple-br-mp/v1';
  const ALPHABET = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ'];
  const DEFAULT_OFF = 'KQWXYZ';
  const TIME_OPTIONS = [5, 8, 10, 12, 15, 20, 30];
  const MAX_PLAYERS = 12;
  const GRACE = 400;        // tolerância de rede (ms) depois que o tempo zera
  const COUNTDOWN = 3000;   // contagem regressiva antes da primeira jogada
  const HOST_LOST_MS = 8000;

  const qs = new URLSearchParams(location.search);
  const BROKER_OVERRIDE = qs.get('broker');
  const $app = document.getElementById('app');
  const $toast = document.getElementById('toast');

  // ---------------------------------------------------------------- utilidades
  const store = {
    get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* sem storage */ } },
  };
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const clone = (o) => JSON.parse(JSON.stringify(o));
  const randInt = (n) => crypto.getRandomValues(new Uint32Array(1))[0] % n;
  const randStr = (n, abc) => Array.from({ length: n }, () => abc[randInt(abc.length)]).join('');
  const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 16);
  const cleanTheme = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 60);
  const shuffle = (arr) => { for (let i = arr.length - 1; i > 0; i--) { const j = randInt(i + 1); [arr[i], arr[j]] = [arr[j], arr[i]]; } return arr; };

  const me = {
    id: store.get('tapple.id') || (() => { const id = randStr(12, 'abcdefghijkmnpqrstuvwxyz23456789'); store.set('tapple.id', id); return id; })(),
    name: store.get('tapple.name', ''),
  };

  const app = {
    room: null, brokerIdx: 0, client: null, T: null,
    connected: false, connectedAt: 0, lastError: '',
    state: null, presence: {},
    clockSamples: [], clockHost: null, offset: 0, lastClockSent: 0,
    lastEvN: null, lastTurnKey: null, lastBeep: null, timeUpKey: null,
    pending: null, joinSentAt: 0, wasIn: false, kicked: false,
    flashLetter: null, preDoneKey: null, lastHostLost: false,
    rolling: false, muted: store.get('tapple.muted', false), wakeLock: null,
    waitingSince: 0, recoverTried: false, popTheme: false,
  };

  // ---------------------------------------------------------------- som / vibração
  let audioCtx = null;
  function beep(freq, dur = 0.08, type = 'sine', vol = 0.18, when = 0) {
    if (app.muted) return;
    try {
      audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
      if (audioCtx.state === 'suspended') audioCtx.resume();
      const t = audioCtx.currentTime + when;
      const o = audioCtx.createOscillator(); const g = audioCtx.createGain();
      o.type = type; o.frequency.setValueAtTime(freq, t);
      g.gain.setValueAtTime(vol, t); g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
      o.connect(g).connect(audioCtx.destination); o.start(t); o.stop(t + dur + 0.02);
    } catch { /* sem áudio */ }
  }
  const sfx = {
    key: () => beep(880, 0.07, 'triangle', 0.25),
    turn: () => { beep(660, 0.1, 'sine', 0.2); beep(990, 0.14, 'sine', 0.2, 0.1); },
    tick: () => beep(1200, 0.05, 'square', 0.08),
    out: () => { beep(220, 0.25, 'sawtooth', 0.15); beep(150, 0.35, 'sawtooth', 0.15, 0.18); },
    win: () => [523, 659, 784, 1046].forEach((f, i) => beep(f, 0.18, 'triangle', 0.2, i * 0.12)),
    start: () => [440, 554, 659].forEach((f, i) => beep(f, 0.12, 'triangle', 0.18, i * 0.1)),
  };
  const vibrate = (p) => { try { navigator.vibrate && navigator.vibrate(p); } catch { /* ok */ } };

  function toast(msg) {
    const d = document.createElement('div'); d.textContent = msg; $toast.appendChild(d);
    setTimeout(() => d.remove(), 3300);
    while ($toast.children.length > 3) $toast.firstChild.remove();
  }

  async function keepAwake() {
    try {
      if ('wakeLock' in navigator && !app.wakeLock && document.visibilityState === 'visible') {
        app.wakeLock = await navigator.wakeLock.request('screen');
        app.wakeLock.addEventListener('release', () => { app.wakeLock = null; });
      }
    } catch { /* sem wake lock */ }
  }

  // ---------------------------------------------------------------- estado do jogo (regras)
  function newState(room) {
    return {
      room, v: 0, hostId: me.id, phase: 'lobby',
      players: [{ id: me.id, name: me.name }],
      gone: {}, scores: { [me.id]: 0 },
      settings: { turnSecs: 10, letters: ALPHABET.filter((l) => !DEFAULT_OFF.includes(l)).join(''), endMode: 'end' },
      theme: null, usedThemes: [], roundNo: 0, starter: 0, round: null,
      evN: 0, ev: [],
    };
  }

  const nameOf = (s, id) => (s.players.find((p) => p.id === id) || {}).name || s.gone[id] || '?';
  const secsMs = (s) => s.settings.turnSecs * 1000;

  function nextAlive(r, fromId) {
    const n = r.order.length; const idx = r.order.indexOf(fromId);
    for (let i = 1; i <= n; i++) {
      const c = r.order[(idx + i) % n];
      if (c !== fromId && r.alive.includes(c)) return c;
    }
    return null;
  }

  function setTurn(s, id, now) {
    const r = s.round;
    r.turn = id; r.seq++;
    r.deadline = Math.max(now, r.startAt) + secsMs(s);
    r.left = secsMs(s);
  }

  function endRound(s, winners, why) {
    const r = s.round;
    winners.forEach((w) => { s.scores[w] = (s.scores[w] || 0) + 1; });
    r.winners = winners; r.why = why; r.contest = null; r.paused = false;
    s.phase = 'roundEnd';
    s._ev.push({ k: 'end', ids: winners, why });
  }

  function eliminate(s, id, reason, now) {
    const r = s.round;
    if (!r || !r.alive.includes(id)) return false;
    const wasTurn = r.turn === id;
    const next = wasTurn ? nextAlive(r, id) : null;
    r.alive = r.alive.filter((x) => x !== id);
    r.out.push({ id, reason });
    s._ev.push({ k: reason, id });
    if (r.alive.length <= 1) { endRound(s, r.alive.slice(), 'last'); return true; }
    if (wasTurn) setTurn(s, next, now);
    return true;
  }

  function startRound(s, now) {
    const ids = s.players.map((p) => p.id);
    const start = s.starter % ids.length;
    const order = ids.slice(start).concat(ids.slice(0, start));
    s.starter = start + 1;
    s.roundNo++;
    if (!s.usedThemes.includes(s.theme)) s.usedThemes.push(s.theme);
    if (s.usedThemes.length > 80) s.usedThemes = s.usedThemes.slice(-80);
    s.round = {
      no: s.roundNo, theme: s.theme, order, alive: order.slice(), out: [],
      letters: s.settings.letters, used: {}, history: [], resets: 0,
      turn: order[0], seq: 1, startAt: now + COUNTDOWN,
      deadline: now + COUNTDOWN + secsMs(s), left: secsMs(s),
      paused: false, contest: null, winners: null, why: null,
    };
    s.phase = 'playing';
    s._ev.push({ k: 'start', theme: s.theme, no: s.roundNo });
  }

  // Aplica uma ação ao estado (somente no anfitrião). Retorna true se mudou algo.
  function reduce(s, a, now) {
    const isHost = a.from === s.hostId;
    const r = s.round;
    const player = s.players.find((p) => p.id === a.from);
    switch (a.t) {
      case 'join': {
        const name = cleanName(a.name);
        if (!name) return false;
        if (player) { if (player.name === name) return false; player.name = name; return true; }
        if (s.players.length >= MAX_PLAYERS) return false;
        s.players.push({ id: a.from, name });
        if (!(a.from in s.scores)) s.scores[a.from] = 0;
        delete s.gone[a.from];
        s._ev.push({ k: 'join', id: a.from });
        return true;
      }
      case 'kick': {
        if (!isHost || a.id === s.hostId) return false;
        const p = s.players.find((x) => x.id === a.id);
        if (!p) return false;
        if (s.phase === 'playing') eliminate(s, a.id, 'kick', now);
        s.gone[a.id] = p.name;
        s.players = s.players.filter((x) => x.id !== a.id);
        s._ev.push({ k: 'kicked', id: a.id });
        return true;
      }
      case 'move': {
        if (!isHost || s.phase === 'playing') return false;
        const i = s.players.findIndex((p) => p.id === a.id); const j = i + (a.dir > 0 ? 1 : -1);
        if (i < 0 || j < 0 || j >= s.players.length) return false;
        [s.players[i], s.players[j]] = [s.players[j], s.players[i]];
        s.starter = 0;
        return true;
      }
      case 'shuffle': {
        if (!isHost || s.phase === 'playing' || s.players.length < 2) return false;
        shuffle(s.players); s.starter = 0;
        s._ev.push({ k: 'shuffled' });
        return true;
      }
      case 'settings': {
        if (!isHost || s.phase === 'playing') return false;
        const st = { ...s.settings };
        if (TIME_OPTIONS.includes(a.turnSecs)) st.turnSecs = a.turnSecs;
        if (typeof a.letters === 'string') {
          const L = ALPHABET.filter((l) => a.letters.includes(l)).join('');
          if (L.length >= 2) st.letters = L;
        }
        if (a.endMode === 'end' || a.endMode === 'reset') st.endMode = a.endMode;
        s.settings = st;
        return true;
      }
      case 'theme': {
        if (!isHost || s.phase === 'playing') return false;
        const t = cleanTheme(a.theme);
        if (!t) return false;
        s.theme = t;
        if (s.phase === 'roundEnd') { s.phase = 'lobby'; s.round = null; }
        s._ev.push({ k: 'theme', theme: t });
        return true;
      }
      case 'start': {
        if (!isHost || s.phase !== 'lobby' || !s.theme || s.players.length < 2) return false;
        startRound(s, now);
        return true;
      }
      case 'letter': {
        if (s.phase !== 'playing' || r.paused || r.contest) return false;
        if (a.from !== r.turn || a.seq !== r.seq) return false;
        if (now < r.startAt || now > r.deadline + GRACE) return false;
        const L = String(a.letter || '');
        if (L.length !== 1 || !r.letters.includes(L) || r.used[L]) return false;
        r.used[L] = a.from;
        r.history.push({ l: L, by: a.from });
        s._ev.push({ k: 'letter', id: a.from, l: L });
        if ([...r.letters].every((x) => r.used[x])) {
          if (s.settings.endMode === 'reset') {
            r.used = {}; r.resets++;
            s._ev.push({ k: 'reset' });
          } else {
            endRound(s, r.alive.slice(), 'letters');
            return true;
          }
        }
        setTurn(s, nextAlive(r, a.from) || a.from, now);
        return true;
      }
      case 'giveup': {
        if (s.phase !== 'playing' || r.contest || a.from !== r.turn || a.seq !== r.seq) return false;
        return eliminate(s, a.from, 'giveup', now);
      }
      case 'skip': {
        if (!isHost || s.phase !== 'playing' || r.contest) return false;
        return eliminate(s, r.turn, 'skip', now);
      }
      case 'pause': {
        if (!isHost || s.phase !== 'playing' || r.paused) return false;
        r.paused = true; r.left = Math.max(0, Math.min(secsMs(s), r.deadline - Math.max(now, r.startAt)));
        s._ev.push({ k: 'pause' });
        return true;
      }
      case 'resume': {
        if (!isHost || s.phase !== 'playing' || !r.paused || r.contest) return false;
        r.paused = false; r.startAt = Math.min(r.startAt, now);
        r.deadline = now + Math.max(r.left, 3000);
        s._ev.push({ k: 'resume' });
        return true;
      }
      case 'contest': {
        if (s.phase !== 'playing' || r.contest || !player) return false;
        const idx = r.history.length - 1;
        if (idx < 0 || r.history[idx].void || r.history[idx].ok) return false;
        if (!r.paused) { r.paused = true; r.left = Math.max(0, Math.min(secsMs(s), r.deadline - Math.max(now, r.startAt))); }
        r.contest = { by: a.from, idx };
        s._ev.push({ k: 'contest', id: a.from, of: r.history[idx].by, l: r.history[idx].l });
        return true;
      }
      case 'resolve': {
        if (!isHost || s.phase !== 'playing' || !r.contest) return false;
        const h = r.history[r.contest.idx];
        r.contest = null;
        if (a.accept) {
          h.void = true; delete r.used[h.l];
          s._ev.push({ k: 'voided', id: h.by, l: h.l });
          eliminate(s, h.by, 'contestOut', now);
        } else {
          h.ok = true;
          s._ev.push({ k: 'upheld', id: h.by, l: h.l });
        }
        if (s.phase === 'playing') {
          r.paused = false; r.startAt = Math.min(r.startAt, now);
          r.deadline = now + Math.max(r.left, 3000);
        }
        return true;
      }
      case 'next': {
        if (!isHost || s.phase !== 'roundEnd') return false;
        s.phase = 'lobby'; s.round = null; s.theme = null;
        return true;
      }
      case 'abort': {
        if (!isHost || s.phase !== 'playing') return false;
        endRound(s, [], 'abort');
        return true;
      }
      case 'resetScores': {
        if (!isHost || s.phase === 'playing') return false;
        Object.keys(s.scores).forEach((k) => { s.scores[k] = 0; });
        s.roundNo = 0;
        return true;
      }
      default: return false;
    }
  }

  // ---------------------------------------------------------------- rede
  const amHost = () => !!app.state && app.state.hostId === me.id;
  const hostNow = () => Date.now() + (amHost() ? 0 : app.offset);

  function publish(topic, obj, retain = false) {
    if (!app.client) return;
    app.client.publish(topic, typeof obj === 'string' ? obj : JSON.stringify(obj), { qos: 1, retain });
  }

  function commit(s) {
    s.v = Math.max(app.state ? app.state.v : 0, s.v || 0) + 1;
    s.sentAt = Date.now();
    if (s._ev && s._ev.length) { s.evN = (s.evN || 0) + 1; s.ev = s._ev; }
    delete s._ev;
    app.state = s;
    store.set('tapple.state.' + s.room, s);
    publish(app.T.state, s, true);
    onNewState();
  }

  function handleAction(a) {
    if (!amHost() || !a || typeof a.t !== 'string' || typeof a.from !== 'string' || a.from.length > 40) return;
    const s = clone(app.state); s._ev = [];
    if (reduce(s, a, Date.now())) commit(s);
  }

  function dispatch(a) {
    a.from = me.id;
    if (amHost()) handleAction(a);
    else publish(app.T.action, a);
  }

  function validState(s) {
    return s && typeof s === 'object' && s.room === app.room && typeof s.v === 'number' &&
      typeof s.hostId === 'string' && Array.isArray(s.players) && s.settings &&
      ['lobby', 'playing', 'roundEnd'].includes(s.phase) &&
      (s.phase === 'lobby' || (s.round && Array.isArray(s.round.order)));
  }

  function onState(s, retained) {
    if (!validState(s)) return;
    const cur = app.state;
    if (cur && s.v < cur.v && amHost()) { publish(app.T.state, cur, true); return; }
    if (cur && (s.v < cur.v || (s.v === cur.v && s.hostId <= cur.hostId))) return;
    if (cur && cur.hostId !== s.hostId) { app.clockSamples = []; app.offset = 0; }
    if (!retained && s.hostId !== me.id && typeof s.sentAt === 'number') sampleClock(s.sentAt);
    app.state = s;
    if (amHost()) store.set('tapple.state.' + s.room, s);
    onNewState();
  }

  function sampleClock(t) {
    app.clockSamples.push(t - Date.now());
    if (app.clockSamples.length > 15) app.clockSamples.shift();
    // amostra = offset real - latência; o maior valor é o de menor latência
    app.offset = Math.max(...app.clockSamples);
  }

  function onMessage(topic, payload, packet) {
    const text = payload.toString();
    if (topic.startsWith(app.T.pres)) {
      const id = topic.slice(app.T.pres.length);
      app.presence[id] = { on: text === '1', at: Date.now() };
      scheduleRender();
      return;
    }
    let msg; try { msg = JSON.parse(text); } catch { return; }
    if (topic === app.T.state) onState(msg, packet.retain);
    else if (topic === app.T.action) handleAction(msg);
    else if (topic === app.T.clock) {
      if (!packet.retain && app.state && msg && msg.h === app.state.hostId && msg.h !== me.id && typeof msg.t === 'number') sampleClock(msg.t);
    }
  }

  function connect(room, brokerIdx) {
    app.room = room; app.brokerIdx = brokerIdx;
    const base = `${ROOT}/${room}`;
    app.T = { state: base + '/state', action: base + '/action', clock: base + '/clock', pres: base + '/p/' };
    const url = BROKER_OVERRIDE || BROKERS[brokerIdx] || BROKERS[0];
    app.waitingSince = Date.now();
    const client = mqtt.connect(url, {
      clientId: 'tp_' + me.id + '_' + randStr(4, 'abcdefghijklmnopqrstuvwxyz'),
      clean: true, keepalive: 20, reconnectPeriod: 2000, connectTimeout: 10000,
      will: { topic: app.T.pres + me.id, payload: '0', qos: 1, retain: true },
    });
    app.client = client;
    client.on('connect', () => {
      app.connected = true; app.connectedAt = Date.now(); app.lastError = '';
      client.subscribe([app.T.state, app.T.action, app.T.clock, app.T.pres + '+'], { qos: 1 });
      publish(app.T.pres + me.id, '1', true);
      if (amHost()) publish(app.T.state, app.state, true); // garante o estado no servidor
      render();
    });
    client.on('close', () => { if (app.connected) { app.connected = false; render(); } });
    client.on('error', (e) => { app.lastError = String((e && e.message) || e); });
    client.on('message', onMessage);
  }

  // ---------------------------------------------------------------- reações a mudanças
  function onNewState() {
    const s = app.state;
    const inNow = s.players.some((p) => p.id === me.id);
    if (app.wasIn && !inNow && !amHost()) app.kicked = true;
    app.wasIn = inNow;
    if (!inNow && me.name && !app.kicked && app.connected && Date.now() - app.joinSentAt > 2500) {
      app.joinSentAt = Date.now(); dispatch({ t: 'join', name: me.name });
    }

    if (app.lastEvN === null) app.lastEvN = s.evN; // primeira carga: não toca eventos antigos
    else if (s.evN > app.lastEvN) { app.lastEvN = s.evN; (s.ev || []).forEach(playEvent); }

    if (s.phase === 'playing') {
      const r = s.round;
      const key = r.no + ':' + r.seq;
      if (r.turn === me.id && app.lastTurnKey !== key && !r.paused) {
        app.lastTurnKey = key; sfx.turn(); vibrate([80, 40, 80]);
      }
      if (app.pending && app.pending.seq !== r.seq) app.pending = null;
      keepAwake();
    } else {
      app.pending = null;
    }
    render();
  }

  function playEvent(e) {
    const s = app.state; const n = (id) => (id === me.id ? 'Você' : nameOf(s, id));
    switch (e.k) {
      case 'join': if (e.id !== me.id) toast(`${n(e.id)} entrou na sala`); break;
      case 'kicked': if (e.id !== me.id) toast(`${n(e.id)} foi removido`); break;
      case 'shuffled': toast('🔀 Ordem embaralhada'); break;
      case 'theme': app.popTheme = true; sfx.key(); break;
      case 'start': sfx.start(); vibrate(60); break;
      case 'letter': app.flashLetter = e.l; if (e.id !== me.id) sfx.key(); break;
      case 'timeout': sfx.out(); vibrate(e.id === me.id ? [300, 80, 300] : 120); toast(`⏰ Tempo esgotado! ${n(e.id)} ${e.id === me.id ? 'saiu' : 'saiu da rodada'}`); break;
      case 'giveup': sfx.out(); toast(`🏳️ ${n(e.id)} ${e.id === me.id ? 'desistiu' : 'desistiu da rodada'}`); break;
      case 'skip': sfx.out(); toast(`⏭ ${n(e.id)} foi eliminado pelo anfitrião`); break;
      case 'contest': vibrate(100); toast(`⚖️ ${n(e.id)} contestou a letra ${e.l} de ${nameOf(s, e.of)}`); break;
      case 'voided': sfx.out(); toast(`❌ Letra ${e.l} anulada — ${n(e.id)} saiu`); break;
      case 'upheld': toast(`✅ A letra ${e.l} vale!`); break;
      case 'reset': toast('🔄 Acabaram as letras! Alfabeto liberado de novo'); break;
      case 'pause': toast('⏸ Jogo pausado'); break;
      case 'resume': toast('▶️ Valendo!'); break;
      case 'host': toast(`👑 ${n(e.id)} agora é o anfitrião`); break;
      case 'end': if (e.why !== 'abort') { sfx.win(); vibrate([60, 60, 60, 60, 200]); } break;
      default: break;
    }
  }

  // Laço do anfitrião: tempo esgotado + relógio
  setInterval(() => {
    if (!amHost() || !app.state) return;
    const s = app.state; const now = Date.now();
    if (s.phase === 'playing' && !s.round.paused && !s.round.contest && now > s.round.deadline + GRACE) {
      const c = clone(s); c._ev = [];
      if (eliminate(c, c.round.turn, 'timeout', now)) commit(c);
    }
    if (app.connected && now - app.lastClockSent > 2000) {
      app.lastClockSent = now;
      publish(app.T.clock, { h: me.id, t: now });
    }
  }, 150);

  // Laço de manutenção: entrar na sala, heartbeat de presença, recuperação do anfitrião
  setInterval(() => {
    if (!app.room || !app.connected) return;
    const now = Date.now(); const s = app.state;
    if (now - (app.lastPresence || 0) > 10000) { app.lastPresence = now; publish(app.T.pres + me.id, '1', true); }
    if (!s && !app.recoverTried && now - app.connectedAt > 2500) {
      app.recoverTried = true;
      const saved = store.get('tapple.state.' + app.room, null);
      if (saved && saved.hostId === me.id && validState(saved)) { app.state = null; commit(saved); return; }
    }
    if (s && me.name && !app.kicked && !s.players.some((p) => p.id === me.id) && now - app.joinSentAt > 2500) {
      app.joinSentAt = now; dispatch({ t: 'join', name: me.name });
    }
    const lost = hostLost();
    const inside = s && !app.kicked && s.players.some((p) => p.id === me.id);
    if (!inside || lost !== app.lastHostLost) { app.lastHostLost = lost; scheduleRender(); }
  }, 1000);

  function hostLost() {
    const s = app.state;
    if (!s || amHost() || !app.connected) return false;
    const p = app.presence[s.hostId];
    if (p) return !p.on && Date.now() - p.at > HOST_LOST_MS;
    return Date.now() - app.connectedAt > 15000;
  }

  function takeOver() {
    const s = clone(app.state); const now = Date.now();
    s.hostId = me.id; s._ev = [{ k: 'host', id: me.id }];
    if (!s.players.some((p) => p.id === me.id)) s.players.push({ id: me.id, name: me.name });
    if (s.phase === 'playing') {
      const r = s.round;
      r.paused = true; r.left = secsMs(s); r.startAt = Math.min(r.startAt, now);
    }
    commit(s);
  }

  // ---------------------------------------------------------------- telas
  function shareUrl() {
    const u = new URL(location.href);
    u.hash = app.room + (app.brokerIdx ? '-' + app.brokerIdx : '');
    return u.toString();
  }

  function viewHome() {
    return `
      <div class="logo"><div class="word">TAPPLE</div><div class="tag">online</div></div>
      <p class="sub">Sorteie um tema, aperte a letra da sua palavra e passe a vez antes do tempo acabar!</p>
      <div class="card stack">
        <div><label for="nm">Seu nome</label><input id="nm" maxlength="16" autocomplete="nickname" placeholder="Ex.: Camila" value="${esc(me.name)}"></div>
        <button class="btn primary block" data-act="create">🎲 Criar sala</button>
      </div>
      <div class="or">— ou entre numa sala existente —</div>
      <div class="card">
        <label for="code">Código da sala</label>
        <div class="row"><input id="code" class="code-input grow" maxlength="7" autocomplete="off" placeholder="ABCDE"><button class="btn" data-act="joincode">Entrar</button></div>
      </div>
      ${howTo()}
    `;
  }

  function howTo() {
    return `
      <details class="howto">
        <summary>Como jogar</summary>
        <ol>
          <li>Uma pessoa cria a sala e manda o link no grupo. Cada um abre e coloca o nome.</li>
          <li>O anfitrião organiza a ordem dos turnos e sorteia o tema.</li>
          <li>Na sua vez, fale em voz alta uma palavra do tema e aperte a letra inicial dela. A letra trava para todo mundo.</li>
          <li>Não conseguiu antes do tempo acabar? Está fora da rodada. Quem sobrar por último ganha o ponto.</li>
          <li>Achou que a palavra não vale? Aperte <b>Contestar</b> e o anfitrião decide.</li>
        </ol>
      </details>`;
  }

  function viewName() {
    return `
      <div class="logo"><div class="word">TAPPLE</div><div class="tag">online</div></div>
      <div class="card stack">
        <p class="center">Você foi convidado para a sala <b class="roomcode">${esc(app.room)}</b></p>
        <div><label for="nm">Seu nome</label><input id="nm" maxlength="16" autocomplete="nickname" placeholder="Ex.: Erick" value="${esc(me.name)}"></div>
        <button class="btn primary block" data-act="setname">Entrar no jogo</button>
        <button class="btn ghost block small" data-act="home">Voltar ao início</button>
      </div>
      ${howTo()}`;
  }

  function viewWaiting() {
    const s = app.state;
    if (app.kicked) {
      return `<div class="card center stack"><p>Você foi removido da sala <b>${esc(app.room)}</b>.</p>
        <button class="btn primary block" data-act="rejoin">Entrar de novo</button>
        <button class="btn ghost block" data-act="home">Voltar ao início</button></div>`;
    }
    let msg = 'Conectando ao servidor…';
    let extra = '';
    if (app.connected && !s) {
      msg = 'Procurando a sala…';
      if (Date.now() - app.connectedAt > 5000) {
        msg = `Não achei a sala <b>${esc(app.room)}</b>. Confira o código, ou talvez o anfitrião ainda esteja abrindo.`;
        extra = `<button class="btn yellow block" data-act="claim">Criar sala com este código</button>`;
      }
    } else if (app.connected && s && s.players.length >= MAX_PLAYERS) {
      msg = 'A sala está cheia.';
    } else if (app.connected && s) {
      msg = 'Entrando na sala…';
    } else if (!app.connected && Date.now() - app.waitingSince > 10000) {
      msg = 'Não está conseguindo conectar. Verifique sua internet.';
      extra = BROKER_OVERRIDE ? '' : `<button class="btn block" data-act="otherbroker">Tentar outro servidor</button>`;
    }
    return `
      <div class="spinner"></div>
      <p class="center">${msg}</p>
      <div class="stack">${extra}<button class="btn ghost block" data-act="home">Voltar ao início</button></div>`;
  }

  function banners() {
    if (!app.room) return '';
    let h = '';
    if (app.client && !app.connected && app.state) h += `<div class="banner err">📡 Sem conexão — reconectando…</div>`;
    if (hostLost()) {
      h += `<div class="banner warn"><span>👑 ${esc(nameOf(app.state, app.state.hostId))} (anfitrião) está offline.</span>
        <button class="btn small yellow" data-act="takeover">Assumir</button></div>`;
    }
    return h;
  }

  function topbar() {
    const s = app.state;
    return `
      <div class="topbar">
        <div><div class="muted small">Sala</div><div class="roomcode">${esc(app.room)}</div></div>
        <div class="row">
          <button class="btn icon ghost" data-act="mute" title="Som">${app.muted ? '🔇' : '🔊'}</button>
          <button class="btn small" data-act="share">📤 Convidar</button>
        </div>
      </div>
      ${s.roundNo ? `<div class="spacer"></div>` : ''}`;
  }

  function onlineDot(id) {
    const p = app.presence[id];
    const on = id === me.id ? app.connected : (p ? p.on : false);
    return `<span class="dot ${on ? '' : 'off'}" title="${on ? 'online' : 'offline'}"></span>`;
  }

  function viewLobby() {
    const s = app.state; const host = amHost();
    const list = s.players.map((p, i) => `
      <li>
        <span class="pos">${i + 1}</span>
        ${onlineDot(p.id)}
        <span class="name">${esc(p.name)} <span class="tags">${p.id === me.id ? '(você)' : ''} ${p.id === s.hostId ? '👑' : ''}</span></span>
        ${s.roundNo ? `<span class="score">${s.scores[p.id] || 0}</span>` : ''}
        ${host ? `
          <button class="btn icon ghost" data-act="move" data-id="${esc(p.id)}" data-dir="-1" ${i === 0 ? 'disabled' : ''} aria-label="Subir">▲</button>
          <button class="btn icon ghost" data-act="move" data-id="${esc(p.id)}" data-dir="1" ${i === s.players.length - 1 ? 'disabled' : ''} aria-label="Descer">▼</button>
          ${p.id !== me.id ? `<button class="btn icon ghost" data-act="kick" data-id="${esc(p.id)}" aria-label="Remover">✕</button>` : '<span style="width:36px"></span>'}
        ` : ''}
      </li>`).join('');

    const order = s.players.map((p) => esc(p.name));
    const starterIdx = s.starter % Math.max(1, s.players.length);
    const orderTxt = s.players.length > 1
      ? order.slice(starterIdx).concat(order.slice(0, starterIdx)).join(' → ') + ' → …'
      : '';

    const themeBox = `
      <div class="theme-box ${s.theme ? '' : 'empty'} ${app.popTheme ? 'pop' : ''}">
        <div class="t" id="themeName">${s.theme ? esc(s.theme) : (host ? 'Sorteie um tema' : 'Aguardando o tema…')}</div>
      </div>`;

    const st = s.settings;
    const settingsHost = `
      <div class="card">
        <h3>Configurações</h3>
        <label>Tempo por jogada</label>
        <div class="chips">${TIME_OPTIONS.map((t) => `<button class="chip ${st.turnSecs === t ? 'on' : ''}" data-act="secs" data-v="${t}">${t}s</button>`).join('')}</div>
        <div class="spacer"></div>
        <label>Letras no jogo <span class="muted">(${st.letters.length}) — toque para ligar/desligar</span></label>
        <div class="chips">${ALPHABET.map((l) => `<button class="chip letter ${st.letters.includes(l) ? 'on' : ''}" data-act="toggleLetter" data-v="${l}">${l}</button>`).join('')}</div>
        <div class="spacer"></div>
        <label>Quando todas as letras forem usadas</label>
        <label class="radio"><input type="radio" name="endMode" data-act="endMode" value="end" ${st.endMode === 'end' ? 'checked' : ''}><span>Fim da rodada — <b>todos que sobraram</b> ganham ponto</span></label>
        <label class="radio"><input type="radio" name="endMode" data-act="endMode" value="reset" ${st.endMode === 'reset' ? 'checked' : ''}><span>Libera o alfabeto de novo e continua até sobrar um</span></label>
        ${s.roundNo ? `<div class="spacer"></div><button class="btn small ghost" data-act="resetScores">Zerar placar</button>` : ''}
      </div>`;
    const settingsGuest = `<p class="muted small center">⏱ ${st.turnSecs}s por jogada · ${st.letters.length} letras</p>`;

    const canStart = s.theme && s.players.length >= 2;
    const footer = host
      ? `<div class="spacer"></div><button class="btn primary block" data-act="start" ${canStart ? '' : 'disabled'}>▶️ Começar rodada ${s.roundNo + 1}</button>
         ${!canStart ? `<p class="muted small center">${s.players.length < 2 ? 'Precisa de pelo menos 2 jogadores.' : 'Sorteie ou escreva um tema.'}</p>` : ''}`
      : `<div class="spacer"></div><p class="center muted">Aguardando o anfitrião começar…</p>`;

    return `
      ${topbar()}
      <div class="card">
        <h3><span>Jogadores (${s.players.length})</span>${host && s.players.length > 1 ? '<button class="btn small ghost" data-act="shuffle">🔀 Embaralhar</button>' : ''}</h3>
        <ul class="players">${list}</ul>
        ${orderTxt ? `<p class="muted small">Ordem da próxima rodada: ${orderTxt}</p>` : ''}
        ${host && s.players.length < 2 ? `<p class="muted small">Mande o link no grupo para os amigos entrarem 👉 <b>Convidar</b></p>` : ''}
        <button class="btn small ghost" data-act="rename">✏️ Trocar meu nome</button>
      </div>
      <div class="card">
        <h3>Tema ${s.roundNo ? `· rodada ${s.roundNo + 1}` : ''}</h3>
        ${themeBox}
        ${host ? `
          <div class="spacer"></div>
          <button class="btn yellow block" data-act="roll" ${app.rolling ? 'disabled' : ''}>🎲 Sortear tema</button>
          <div class="spacer"></div>
          <div class="row"><input id="customTheme" class="grow" maxlength="60" placeholder="…ou escreva um tema"><button class="btn" data-act="customTheme">Usar</button></div>
        ` : ''}
      </div>
      ${host ? settingsHost : settingsGuest}
      ${footer}
    `;
  }

  function ringLayout(n) {
    // posições (em %) das letras em volta do círculo
    const keyPct = Math.min(15, (Math.PI * 100 / n) * 0.82);
    const R = 50 - keyPct / 2 - 1;
    const keys = [];
    for (let i = 0; i < n; i++) {
      const ang = (i / n) * Math.PI * 2 - Math.PI / 2;
      keys.push({ x: 50 + R * Math.cos(ang), y: 50 + R * Math.sin(ang) });
    }
    return { keyPct, core: (R - keyPct / 2 - 3) * 2, keys };
  }

  function viewPlaying() {
    const s = app.state; const r = s.round; const host = amHost();
    const myTurn = r.turn === me.id;
    const imAlive = r.alive.includes(me.id);
    const inRound = r.order.includes(me.id);
    const letters = [...r.letters];
    const L = ringLayout(letters.length);
    const last = r.history.length ? r.history[r.history.length - 1] : null;
    const canPress = myTurn && !r.paused && !r.contest && !app.pending && hostNow() >= r.startAt;

    const keys = letters.map((l, i) => {
      const used = r.used[l]; const pend = app.pending && app.pending.l === l;
      const cls = pend ? 'pending' : used ? (last && last.l === l && !last.void ? (app.flashLetter === l ? 'used last flash' : 'used last') : 'used') : (canPress ? 'ready' : '');
      const by = used ? `<span class="by">${esc(nameOf(s, used))}</span>` : '';
      return `<button class="key ${cls}" data-act="letter" data-v="${l}" ${used || !canPress ? 'disabled' : ''}
        style="left:${L.keys[i].x}%;top:${L.keys[i].y}%;width:${L.keyPct}%;height:${L.keyPct}%;font-size:min(${L.keyPct * 0.42}vw, ${L.keyPct * 0.42 * 4.4}px)">${l}${by}</button>`;
    }).join('');

    const queue = r.order.map((id) => {
      const out = !r.alive.includes(id);
      return `<span class="qchip ${id === r.turn && !out ? 'turn' : ''} ${out ? 'out' : ''} ${id === me.id ? 'me' : ''}">${esc(nameOf(s, id))}</span>`;
    }).join('');

    let status = myTurn ? 'SUA VEZ!' : `Vez de ${esc(nameOf(s, r.turn))}`;
    if (!inRound) status = 'Você entra na próxima rodada 👀';
    else if (!imAlive) status = `Você saiu · vez de ${esc(nameOf(s, r.turn))}`;

    let actions = '';
    if (myTurn) actions += `<button class="btn ghost" data-act="giveup" ${r.paused || r.contest ? 'disabled' : ''}>🏳️ Não sei, desisto</button>`;
    if (last && !last.void && !last.ok && !r.contest && s.players.some((p) => p.id === me.id) && last.by !== me.id) {
      actions += `<button class="btn ghost" data-act="contest">⚖️ Contestar ${esc(last.l)} de ${esc(nameOf(s, last.by))}</button>`;
    }
    const hostbar = host ? `
      <div class="hostbar">
        ${r.paused ? `<button class="btn small green" data-act="resume" ${r.contest ? 'disabled' : ''}>▶️ Continuar</button>` : `<button class="btn small" data-act="pause">⏸ Pausar</button>`}
        <button class="btn small ghost" data-act="skip" ${r.contest ? 'disabled' : ''}>⏭ Eliminar ${esc(nameOf(s, r.turn))}</button>
        <button class="btn small ghost" data-act="abort">⏹ Encerrar rodada</button>
      </div>` : '';

    let overlay = '';
    if (r.contest) {
      const h = r.history[r.contest.idx];
      overlay = `
        <div class="overlay"><div class="box">
          <div class="big">${esc(h.l)}</div>
          <p><b>${esc(nameOf(s, r.contest.by))}</b> contestou a palavra de <b>${esc(nameOf(s, h.by))}</b> com a letra <b>${esc(h.l)}</b>.</p>
          ${host ? `
            <button class="btn primary block" data-act="resolve" data-v="1">❌ Não vale — ${esc(nameOf(s, h.by))} sai</button>
            <button class="btn green block" data-act="resolve" data-v="0">✅ Vale! Continua o jogo</button>`
          : `<p class="muted">O anfitrião vai decidir…</p>`}
        </div></div>`;
    } else if (r.paused) {
      overlay = `
        <div class="overlay"><div class="box">
          <div class="big">⏸</div><p>Jogo pausado</p>
          ${host ? `<button class="btn green block" data-act="resume">▶️ Continuar</button>` : '<p class="muted">Aguardando o anfitrião…</p>'}
        </div></div>`;
    }

    return `
      ${topbar()}
      <div class="theme-head"><div class="lbl">Rodada ${r.no} · Tema</div><div class="t">${esc(r.theme)}</div></div>
      <div class="queue">${queue}</div>
      <div class="status ${myTurn && !r.paused ? 'mine' : ''}">${status}</div>
      <div class="ring">
        ${keys}
        <div class="core" id="core" style="width:${L.core}%;height:${L.core}%">
          <svg viewBox="0 0 100 100"><circle class="track" cx="50" cy="50" r="46" fill="none" stroke-width="6"/>
            <circle id="tbar" class="bar" cx="50" cy="50" r="46" fill="none" stroke-width="6" stroke-dasharray="289" stroke-dashoffset="0"/></svg>
          <div class="inner"><div class="num" id="tnum">${s.settings.turnSecs}</div><div class="who" id="twho">${myTurn ? 'você' : esc(nameOf(s, r.turn))}</div></div>
        </div>
      </div>
      ${r.resets ? `<p class="center muted small">🔄 Alfabeto liberado ${r.resets}x</p>` : ''}
      ${actions ? `<div class="actions">${actions}</div>` : ''}
      ${hostbar}
      <div id="countdown"></div>
      ${overlay}
    `;
  }

  function viewRoundEnd() {
    const s = app.state; const r = s.round; const host = amHost();
    const w = r.winners || [];
    let title;
    if (r.why === 'abort') title = 'Rodada encerrada pelo anfitrião';
    else if (!w.length) title = 'Ninguém sobrou!';
    else if (w.length === 1) title = w[0] === me.id ? 'Você venceu a rodada!' : `${esc(nameOf(s, w[0]))} venceu a rodada!`;
    else title = `Acabaram as letras! Ponto para ${w.map((id) => esc(nameOf(s, id))).join(', ')}`;
    const ranking = s.players.slice().sort((a, b) => (s.scores[b.id] || 0) - (s.scores[a.id] || 0));
    const seq = r.history.map((h) => `<span class="${h.void ? 'void' : ''}"><b>${esc(h.l)}</b>${esc(nameOf(s, h.by))}</span>`).join('');
    return `
      ${topbar()}
      <div class="winner">
        <div class="trophy">${r.why === 'abort' ? '⏹' : '🏆'}</div>
        <div class="t">${title}</div>
        <p class="muted">Tema: ${esc(r.theme)}</p>
      </div>
      <div class="card">
        <h3>Placar</h3>
        <ul class="players">${ranking.map((p, i) => `
          <li><span class="pos">${i + 1}</span>${onlineDot(p.id)}<span class="name">${esc(p.name)} <span class="tags">${p.id === me.id ? '(você)' : ''}</span></span><span class="score">${s.scores[p.id] || 0}</span></li>`).join('')}
        </ul>
      </div>
      ${seq ? `<div class="card"><h3>Letras jogadas</h3><div class="seq">${seq}</div></div>` : ''}
      <div class="spacer"></div>
      ${host
        ? `<button class="btn primary block" data-act="next">🎲 Próxima rodada</button>`
        : `<p class="center muted">Aguardando o anfitrião iniciar a próxima rodada…</p>`}
    `;
  }

  // ---------------------------------------------------------------- render
  let renderQueued = false;
  function scheduleRender() {
    if (renderQueued) return;
    renderQueued = true;
    requestAnimationFrame(() => { renderQueued = false; render(); });
  }

  function render() {
    const ae = document.activeElement;
    const focus = ae && ae.id && ae.tagName === 'INPUT' ? { id: ae.id, start: ae.selectionStart, end: ae.selectionEnd } : null;
    const drafts = {};
    $app.querySelectorAll('input[id]').forEach((el) => { if (el.type !== 'radio') drafts[el.id] = el.value; });

    let html;
    const s = app.state;
    if (!app.room) html = viewHome();
    else if (!me.name) html = viewName();
    else if (!s || app.kicked || !s.players.some((p) => p.id === me.id)) html = viewWaiting();
    else if (s.phase === 'lobby') html = viewLobby();
    else if (s.phase === 'playing') html = viewPlaying();
    else html = viewRoundEnd();

    $app.innerHTML = banners() + html;
    app.popTheme = false; app.flashLetter = null;

    Object.entries(drafts).forEach(([id, v]) => { const el = document.getElementById(id); if (el && id !== 'nm') el.value = v; else if (el && v) el.value = v; });
    if (focus) {
      const el = document.getElementById(focus.id);
      if (el) { el.focus(); try { el.setSelectionRange(focus.start, focus.end); } catch { /* ok */ } }
    }
    updateTimer();
  }

  // Cronômetro (roda a cada frame, só mexe em poucos elementos)
  function updateTimer() {
    const s = app.state;
    if (!s || s.phase !== 'playing') return;
    const r = s.round;
    const $num = document.getElementById('tnum'); const $bar = document.getElementById('tbar');
    const $core = document.getElementById('core'); const $cd = document.getElementById('countdown');
    if (!$num) return;
    const total = secsMs(s);
    const now = hostNow();
    let left = r.paused || r.contest ? r.left : r.deadline - now;
    const pre = !r.paused && !r.contest ? r.startAt - now : 0;
    left = Math.max(0, Math.min(total, left));
    const secs = Math.ceil(left / 1000);
    $num.textContent = String(secs);
    $bar.setAttribute('stroke-dashoffset', String(289 * (1 - left / total)));
    $core.classList.toggle('danger', secs <= 3 && !r.paused);

    if ($cd) {
      const html = pre > 0
        ? `<div class="overlay"><div class="box"><p class="muted">Rodada ${r.no} · Tema</p><h2>${esc(r.theme)}</h2><p>Começa com <b>${esc(nameOf(s, r.turn))}</b></p><div class="big">${Math.ceil(pre / 1000)}</div></div></div>`
        : '';
      if ($cd.innerHTML !== html) $cd.innerHTML = html;
    }

    if (r.turn === me.id && !r.paused && !r.contest && pre <= 0) {
      const key = r.no + ':' + r.seq + ':' + secs;
      if (secs <= 3 && secs >= 1 && app.lastBeep !== key) { app.lastBeep = key; sfx.tick(); vibrate(40); }
    }
    // fim da contagem regressiva: re-renderiza para liberar as teclas
    const preKey = r.no + ':' + r.seq;
    if (pre <= 0 && app.preDoneKey !== preKey) { app.preDoneKey = preKey; if (r.turn === me.id) scheduleRender(); }
    // tempo acabou: trava as teclas até o anfitrião confirmar
    const upKey = r.no + ':' + r.seq;
    if (!r.paused && !r.contest && left <= 0 && app.timeUpKey !== upKey) {
      app.timeUpKey = upKey;
      document.querySelectorAll('.key.ready').forEach((k) => { k.disabled = true; k.classList.remove('ready'); });
    }
  }
  (function frame() { updateTimer(); requestAnimationFrame(frame); })();

  // ---------------------------------------------------------------- ações da interface
  function readName() {
    const el = document.getElementById('nm');
    const n = cleanName(el ? el.value : me.name);
    if (!n) { toast('Digite seu nome 🙂'); el && el.focus(); return null; }
    me.name = n; store.set('tapple.name', n);
    return n;
  }

  function parseHash() {
    const m = /^#?([A-Za-z]{4,6})(?:-(\d))?$/.exec(location.hash.trim());
    return m ? { room: m[1].toUpperCase(), broker: Number(m[2] || 0) } : null;
  }

  function enterRoom(room, brokerIdx) {
    history.replaceState(null, '', '#' + room + (brokerIdx ? '-' + brokerIdx : ''));
    connect(room, brokerIdx);
    render();
  }

  async function share() {
    const url = shareUrl();
    const text = `Bora jogar Tapple! Sala ${app.room}`;
    try {
      if (navigator.share) { await navigator.share({ title: 'Tapple Online', text, url }); return; }
    } catch { /* cancelado */ }
    try { await navigator.clipboard.writeText(url); toast('🔗 Link copiado! Cole no grupo.'); } catch { prompt('Copie o link:', url); }
  }

  function rollTheme() {
    const s = app.state;
    let pool = (window.TEMAS || []).filter((t) => !s.usedThemes.includes(t) && t !== s.theme);
    if (!pool.length) pool = (window.TEMAS || ['Animais']).slice();
    const final = pool[randInt(pool.length)];
    app.rolling = true; render();
    let i = 0;
    const iv = setInterval(() => {
      const el = document.getElementById('themeName');
      if (el) { el.textContent = pool[randInt(pool.length)]; el.parentElement.classList.remove('empty'); }
      sfx.tick();
      if (++i >= 12) { clearInterval(iv); app.rolling = false; dispatch({ t: 'theme', theme: final }); }
    }, 75);
  }

  const handlers = {
    create() {
      if (!readName()) return;
      const room = randStr(5, 'ABCDEFGHJKLMNPQRSTUVWXYZ');
      enterRoom(room, 0);
      app.state = null; commit(newState(room));
    },
    joincode() {
      if (!readName()) return;
      const code = (document.getElementById('code').value || '').trim().toUpperCase();
      const m = /^([A-Z]{4,6})(?:-(\d))?$/.exec(code);
      if (!m) { toast('Código inválido'); return; }
      enterRoom(m[1], Number(m[2] || 0));
    },
    setname() { if (readName()) { app.joinSentAt = 0; render(); } },
    home() { location.href = location.pathname + location.search; },
    rejoin() { app.kicked = false; app.joinSentAt = 0; render(); },
    claim() { app.state = null; const s = newState(app.room); commit(s); },
    otherbroker() { location.hash = app.room + '-' + ((app.brokerIdx + 1) % BROKERS.length); location.reload(); },
    takeover() { if (confirm('Assumir como anfitrião? Você passa a controlar o jogo.')) takeOver(); },
    mute() { app.muted = !app.muted; store.set('tapple.muted', app.muted); render(); },
    share,
    rename() {
      const n = cleanName(prompt('Seu nome:', me.name) || '');
      if (!n) return;
      me.name = n; store.set('tapple.name', n);
      dispatch({ t: 'join', name: n });
    },
    move(el) { dispatch({ t: 'move', id: el.dataset.id, dir: Number(el.dataset.dir) }); },
    kick(el) { if (confirm(`Remover ${nameOf(app.state, el.dataset.id)} da sala?`)) dispatch({ t: 'kick', id: el.dataset.id }); },
    shuffle() { dispatch({ t: 'shuffle' }); },
    secs(el) { dispatch({ t: 'settings', turnSecs: Number(el.dataset.v) }); },
    toggleLetter(el) {
      const cur = app.state.settings.letters; const l = el.dataset.v;
      const next = cur.includes(l) ? cur.replace(l, '') : cur + l;
      if (next.length < 2) { toast('Deixe pelo menos 2 letras'); return; }
      dispatch({ t: 'settings', letters: next });
    },
    endMode(el) { dispatch({ t: 'settings', endMode: el.value }); },
    resetScores() { if (confirm('Zerar o placar de todos?')) dispatch({ t: 'resetScores' }); },
    roll() { if (!app.rolling) rollTheme(); },
    customTheme() {
      const el = document.getElementById('customTheme'); const t = cleanTheme(el.value);
      if (!t) { toast('Escreva um tema'); return; }
      el.value = ''; dispatch({ t: 'theme', theme: t });
    },
    start() { dispatch({ t: 'start' }); },
    letter(el) {
      const r = app.state.round;
      if (r.turn !== me.id || app.pending) return;
      app.pending = { l: el.dataset.v, seq: r.seq };
      sfx.key(); vibrate(25);
      dispatch({ t: 'letter', letter: el.dataset.v, seq: r.seq });
      render();
      const seq = r.seq;
      setTimeout(() => { if (app.pending && app.pending.seq === seq) { app.pending = null; render(); } }, 2500);
    },
    giveup() { const r = app.state.round; dispatch({ t: 'giveup', seq: r.seq }); },
    contest() { dispatch({ t: 'contest' }); },
    resolve(el) { dispatch({ t: 'resolve', accept: el.dataset.v === '1' }); },
    pause() { dispatch({ t: 'pause' }); },
    resume() { dispatch({ t: 'resume' }); },
    skip() { if (confirm(`Eliminar ${nameOf(app.state, app.state.round.turn)} desta rodada?`)) dispatch({ t: 'skip' }); },
    abort() { if (confirm('Encerrar a rodada sem vencedor?')) dispatch({ t: 'abort' }); },
    next() { dispatch({ t: 'next' }); },
  };

  // pointerdown nas letras deixa a resposta mais rápida no celular
  $app.addEventListener('pointerdown', (e) => {
    const el = e.target.closest('.key[data-act="letter"]');
    if (el && !el.disabled) { e.preventDefault(); handlers.letter(el); }
  });
  $app.addEventListener('click', (e) => {
    const el = e.target.closest('[data-act]');
    if (!el || el.disabled || el.dataset.act === 'letter' || el.dataset.act === 'endMode') return;
    const fn = handlers[el.dataset.act];
    if (fn) fn(el);
  });
  $app.addEventListener('change', (e) => {
    const el = e.target.closest('[data-act="endMode"]');
    if (el) handlers.endMode(el);
  });
  $app.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' || e.target.tagName !== 'INPUT') return;
    const map = { nm: app.room ? 'setname' : 'create', code: 'joincode', customTheme: 'customTheme' };
    const act = map[e.target.id];
    if (act) { e.preventDefault(); handlers[act](); }
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      if (app.state && app.state.phase === 'playing') keepAwake();
      if (app.connected) publish(app.T.pres + me.id, '1', true);
    }
  });
  window.addEventListener('pagehide', () => { if (app.connected) publish(app.T.pres + me.id, '0', true); });
  window.addEventListener('pageshow', (e) => { if (e.persisted && app.connected) publish(app.T.pres + me.id, '1', true); });
  window.addEventListener('hashchange', () => {
    const h = parseHash();
    if (!h || h.room !== app.room) location.reload();
  });

  // ---------------------------------------------------------------- início
  if (typeof window.mqtt === 'undefined') {
    $app.innerHTML = '<div class="card center"><p>Não consegui carregar o jogo. Verifique a internet e recarregue a página.</p><button class="btn primary" onclick="location.reload()">Recarregar</button></div>';
    return;
  }
  const h = parseHash();
  if (h) {
    connect(h.room, h.broker);
  }
  render();
})();
