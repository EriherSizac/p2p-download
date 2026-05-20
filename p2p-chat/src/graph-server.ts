// File: GRAPH SERVER — visualizador en vivo del grafo de peers.
// Created: 2026-05-19
// Updated: 2026-05-19
// Author: Erick Hernández Silva

/**
 * GRAPH SERVER — visualizador en vivo del grafo de peers.
 *
 * Por qué un servidor: queremos ver en tiempo real cómo los peers se
 * conectan y desconectan. Una página HTML estática no se entera de los
 * cambios en el proceso Node. Solución clásica: HTTP local + SSE.
 *
 *   • HTTP local (127.0.0.1, puerto efímero) sirve la página y un endpoint
 *     `/events` que es un Server-Sent Events stream. SSE = HTTP con
 *     `Content-Type: text/event-stream` y respuesta abierta para siempre:
 *     cada vez que el server escribe `data: ...\n\n`, el navegador lo
 *     entrega como un evento `message` al `EventSource` de la página.
 *   • Cada cambio (peer-connected, peer-disconnected, PEER_LIST recibido)
 *     dispara `emit(snapshot)`. El server reenvía a todos los clientes
 *     conectados. La página aplica el snapshot al `DataSet` de
 *     vis-network, que anima los cambios automáticamente.
 *
 * Solo escucha en 127.0.0.1: no expone nada a la red, es solo para el
 * navegador del propio usuario.
 *
 * Por qué SSE y no WebSocket: aquí la comunicación es unidireccional
 * (server→cliente). SSE es HTTP plano, sin handshake especial, sin libs.
 * WebSocket sería overkill.
 */

import http from 'node:http';
import { type AddressInfo } from 'node:net';
import { createLogger } from './logger.js';

const log = createLogger('graph-server');

/** Callbacks que la capa main expone al server para que el HTML los invoque. */
export interface GraphActions {
  /** Envía un DM al peer indicado. Devuelve si pudo entregarse al transport. */
  sendDm(toPeerId: string, text: string): boolean;
  /** Inicia una llamada al peer. Source default tone. */
  startCall(toPeerId: string, source?: string): Promise<{ ok: boolean; error?: string }>;
  /** Devuelve info ampliada de un peer para el panel lateral. */
  peerInfo(peerId: string): {
    peerId: string;
    rttMs?: number;
    isMe: boolean;
    isConnected: boolean;
    recent: Array<{ ts: number; dir: 'in' | 'out'; text: string }>;
    unread: number;
    history: Array<{ ts: number; kind: 'connected' | 'disconnected' }>;
  };
  /** Marca mensajes con peer como leídos (badge a cero). */
  markRead(peerId: string): void;
}

/** Evento push: snapshot completo o highlight puntual (pulso visual). */
export type GraphEvent =
  | ({ kind: 'snapshot' } & GraphSnapshot)
  | { kind: 'highlight'; peerId: string; label?: string; color?: string }
  | { kind: 'toast'; text: string }
  | { kind: 'ping-pulse'; from: string; to: string; phase: 'ping' | 'pong' };

/** Forma del snapshot que mandamos al navegador. */
export interface GraphSnapshot {
  nodes: Array<{
    id: string;
    label: string;
    color: { background: string; border: string };
    size: number;
    title?: string;
  }>;
  edges: Array<{
    id: string;
    from: string;
    to: string;
    dashes: boolean;
    color: { color: string };
    width: number;
  }>;
  stats: { total: number; mutuals: number; oneway: number; density: string };
  /** Timestamp del snapshot (ms epoch). Sirve para indicar "última actualización". */
  ts: number;
}

export class GraphServer {
  private server?: http.Server;
  private clients = new Set<http.ServerResponse>();
  private port = 0;
  private lastSnap?: GraphSnapshot;
  private actions?: GraphActions;

  /** Inyecta las acciones que el frontend podrá disparar vía /api/*. */
  setActions(actions: GraphActions): void {
    this.actions = actions;
  }

  /** Arranca el server. Devuelve el puerto efímero asignado por el SO. */
  async start(): Promise<number> {
    this.server = http.createServer((req, res) => this.handle(req, res));
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(0, '127.0.0.1', () => {
        this.port = (this.server!.address() as AddressInfo).port;
        resolve();
      });
    });
    log.info(`graph server escuchando en ${this.url()}`);
    return this.port;
  }

  /** URL canónica para abrir en el navegador. */
  url(): string {
    return `http://127.0.0.1:${this.port}/`;
  }

  /** Empuja un snapshot a todos los clientes conectados. */
  emit(snap: GraphSnapshot): void {
    this.lastSnap = snap;
    this.broadcast({ kind: 'snapshot', ...snap });
  }

  /** Empuja un evento "highlight" para que el HTML resalte/pulse un nodo. */
  highlight(peerId: string, opts?: { label?: string; color?: string }): void {
    this.broadcast({ kind: 'highlight', peerId, label: opts?.label, color: opts?.color });
  }

  /** Empuja un toast informativo (esquina del HTML). */
  toast(text: string): void {
    this.broadcast({ kind: 'toast', text });
  }

  /** Anima el arco entre dos peers para visualizar PING → PONG. */
  pingPulse(from: string, to: string, phase: 'ping' | 'pong'): void {
    this.broadcast({ kind: 'ping-pulse', from, to, phase });
  }

  private broadcast(ev: GraphEvent): void {
    const payload = `data: ${JSON.stringify(ev)}\n\n`;
    for (const c of this.clients) {
      try { c.write(payload); } catch { /* ignore: cleanup en close */ }
    }
  }

  /** Número de clientes conectados — útil para evitar trabajar de balde. */
  hasClients(): boolean {
    return this.clients.size > 0;
  }

  stop(): void {
    for (const c of this.clients) {
      try { c.end(); } catch { /* ignore */ }
    }
    this.clients.clear();
    this.server?.close();
    this.server = undefined;
  }

  // -------------------------------------------------------------------------
  // Internos.
  // -------------------------------------------------------------------------

  private handle(req: http.IncomingMessage, res: http.ServerResponse): void {
    const url = req.url ?? '/';
    if (url === '/events') return this.handleSse(req, res);

    // API JSON. Endpoints muy específicos; sin librería HTTP — un switch
    // simple. Cada handler responde con `{ok:true,...}` o status 4xx/5xx.
    if (url === '/api/dm' && req.method === 'POST')        { void this.handleDm(req, res); return; }
    if (url === '/api/whisper' && req.method === 'POST')   { void this.handleWhisper(req, res); return; }
    if (url === '/api/call' && req.method === 'POST')      { void this.handleCall(req, res); return; }
    if (url === '/api/ping' && req.method === 'POST')      { void this.handlePing(req, res); return; }
    if (url === '/api/highlight' && req.method === 'POST') { void this.handleHighlight(req, res); return; }
    if (url.startsWith('/api/peer/') && req.method === 'GET') { this.handlePeerInfo(url, res); return; }
    if (url.startsWith('/api/read/') && req.method === 'POST') { this.handleMarkRead(url, res); return; }

    // Default: la página HTML.
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
  }

  /** Lee JSON body con límite duro de 16KB para evitar DoS local. */
  private async readJson(req: http.IncomingMessage): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      let size = 0;
      req.on('data', (c: Buffer) => {
        size += c.length;
        if (size > 16 * 1024) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => {
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
        } catch (err) {
          reject(err as Error);
        }
      });
      req.on('error', reject);
    });
  }

  private json(res: http.ServerResponse, status: number, obj: unknown): void {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(obj));
  }

  private async handleDm(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    try {
      const body = await this.readJson(req);
      const to = String(body['to'] ?? '');
      const text = String(body['text'] ?? '').trim();
      if (!to || !text) return this.json(res, 400, { ok: false, error: 'to+text required' });
      const ok = this.actions.sendDm(to, text);
      this.json(res, ok ? 200 : 502, { ok });
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }

  private async handleCall(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    try {
      const body = await this.readJson(req);
      const to = String(body['to'] ?? '');
      const source = body['source'] ? String(body['source']) : undefined;
      if (!to) return this.json(res, 400, { ok: false, error: 'to required' });
      const r = await this.actions.startCall(to, source);
      this.json(res, r.ok ? 200 : 502, r);
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }

  private handlePeerInfo(url: string, res: http.ServerResponse): void {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    // /api/peer/<peerId>
    const peerId = decodeURIComponent(url.slice('/api/peer/'.length));
    if (!peerId) return this.json(res, 400, { ok: false, error: 'peerId required' });
    this.json(res, 200, { ok: true, info: this.actions.peerInfo(peerId) });
  }

  private async handleWhisper(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // Whisper = DM normal + feedback visual extra (pulso verde en el nodo).
    // Semánticamente equivalente al DM; el "extra" es solo UX.
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    try {
      const body = await this.readJson(req);
      const to = String(body['to'] ?? '');
      const text = String(body['text'] ?? '').trim();
      if (!to || !text) return this.json(res, 400, { ok: false, error: 'to+text required' });
      const ok = this.actions.sendDm(to, text);
      if (ok) {
        this.highlight(to, { color: '#27ae60', label: '🤫 whisper' });
        this.toast(`whisper → ${to.slice(0, 8)}: ${text.slice(0, 60)}`);
      }
      this.json(res, ok ? 200 : 502, { ok });
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }

  private async handlePing(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // Ping desde web: pulso visual amarillo. No genera tráfico nuevo (el
    // protocolo PING/PONG ya corre cada 5s para liveness). Es señal local.
    try {
      const body = await this.readJson(req);
      const to = String(body['to'] ?? '');
      if (!to) return this.json(res, 400, { ok: false, error: 'to required' });
      this.highlight(to, { color: '#f1c40f', label: '⚡ ping' });
      this.toast(`ping → ${to.slice(0, 8)}`);
      this.json(res, 200, { ok: true });
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }

  private async handleHighlight(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // Highlight genérico: marcar visualmente un peer. Color/label opcionales.
    try {
      const body = await this.readJson(req);
      const to = String(body['to'] ?? '');
      if (!to) return this.json(res, 400, { ok: false, error: 'to required' });
      const color = body['color'] ? String(body['color']) : '#9b59b6';
      const label = body['label'] ? String(body['label']) : '★ highlight';
      this.highlight(to, { color, label });
      this.json(res, 200, { ok: true });
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }

  private handleMarkRead(url: string, res: http.ServerResponse): void {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    const peerId = decodeURIComponent(url.slice('/api/read/'.length));
    if (!peerId) return this.json(res, 400, { ok: false, error: 'peerId required' });
    this.actions.markRead(peerId);
    this.json(res, 200, { ok: true });
  }

  private handleSse(req: http.IncomingMessage, res: http.ServerResponse): void {
    // Headers obligatorios para que el navegador entienda esto como SSE:
    //   text/event-stream → tipo MIME mágico
    //   no-cache          → cualquier proxy intermedio deja pasar los chunks
    //   keep-alive        → la conexión no se cierra después del primer flush
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // evita buffering en algunos proxies
    });
    // Comentario inicial: algunos clientes no entregan el primer evento si
    // el server no escribe algo "pronto". Un comentario `:` no es un evento.
    res.write(': hola\n\n');

    this.clients.add(res);
    log.debug(`SSE cliente conectado (total=${this.clients.size})`);

    // Mandar último snapshot conocido para que la página no arranque vacía.
    if (this.lastSnap) {
      try {
        res.write(`data: ${JSON.stringify({ kind: 'snapshot', ...this.lastSnap })}\n\n`);
      } catch {
        /* ignore */
      }
    }

    req.on('close', () => {
      this.clients.delete(res);
      log.debug(`SSE cliente desconectado (restantes=${this.clients.size})`);
    });
  }
}

// ---------------------------------------------------------------------------
// HTML embebido. vis-network desde CDN; tema oscuro; cliente SSE que aplica
// snapshots al DataSet (añade/quita/actualiza). vis-network se encarga de
// animar la transición.
// ---------------------------------------------------------------------------

const PAGE_HTML = `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>p2p-chat — grafo en vivo</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<style>
  html, body { margin:0; padding:0; height:100%; background:#0f1117; color:#e6e6e6;
               font-family: -apple-system, system-ui, "Segoe UI", sans-serif; }
  #app { display:flex; height:100%; flex-direction:column; }
  #header { padding: 10px 18px; border-bottom: 1px solid #222; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  #header h1 { margin:0; font-size:15px; font-weight:600; }
  #header .stats { font-size:12px; color:#8a8f99; font-family: ui-monospace, monospace; }
  #header .legend { margin-left:auto; font-size:11px; color:#8a8f99; }
  #header .legend span { margin-right:10px; }
  #header .swatch { display:inline-block; width:14px; height:2px; vertical-align:middle; margin-right:4px; }
  #header .pulse { width:8px; height:8px; border-radius:50%; background:#2ecc71; display:inline-block; vertical-align:middle; margin-right:6px; animation: blink 1.2s infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.3} }
  #status { font-size:11px; color:#8a8f99; }
  #status.off .pulse { background:#e74c3c; animation: none; }
  #main { display:flex; flex:1; min-height:0; }
  #net { flex:1; height:100%; }
  #panel { width:320px; border-left:1px solid #222; padding:16px; overflow-y:auto; background:#13161e; }
  #panel.hidden { display:none; }
  #panel h2 { margin:0 0 4px; font-size:14px; font-family: ui-monospace, monospace; word-break:break-all; display:flex; align-items:flex-start; gap:6px; }
  #panel h2 .copy { background:#1f242e; border:1px solid #2d343f; color:#cfd6e1; padding:2px 6px; border-radius:3px; cursor:pointer; font-size:11px; font-family:inherit; flex-shrink:0; }
  #panel h2 .copy:hover { background:#2a313d; }
  #panel h2 .copy.ok { background:#27ae60; border-color:#27ae60; color:#fff; }
  #panel .self-note { background:#1a2230; border:1px solid #2d3a4f; color:#8fa9d4; padding:8px 10px; border-radius:4px; font-size:12px; margin-bottom:12px; }
  #panel .meta { font-size:11px; color:#8a8f99; margin-bottom:14px; }
  #panel .meta .kv { display:flex; justify-content:space-between; margin-top:3px; }
  #panel .meta .kv span:last-child { font-family: ui-monospace, monospace; color:#cfd6e1; }
  #panel .actions { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }
  #panel button { background:#1f242e; border:1px solid #2d343f; color:#e6e6e6; padding:6px 10px; border-radius:4px; cursor:pointer; font-size:12px; }
  #panel button:hover { background:#2a313d; }
  #panel button.primary { background:#2563eb; border-color:#2563eb; }
  #panel button.primary:hover { background:#1d4fd6; }
  #panel button.danger { background:#dc2626; border-color:#dc2626; }
  #panel textarea { width:100%; box-sizing:border-box; min-height:64px; background:#0f1117; color:#e6e6e6; border:1px solid #2d343f; border-radius:4px; padding:6px; font-family:inherit; font-size:12px; resize:vertical; }
  #panel .recent { margin-top:14px; }
  #panel .recent h3 { font-size:12px; color:#8a8f99; text-transform:uppercase; letter-spacing:0.05em; margin:0 0 6px; }
  #panel .recent .msg { font-size:12px; padding:4px 0; border-bottom:1px solid #1a1f29; }
  #panel .recent .msg .dir { display:inline-block; width:14px; color:#6b7280; }
  #panel .recent .msg.in .dir { color:#3498db; }
  #panel .recent .msg.out .dir { color:#2ecc71; }
  #panel .recent .msg .t { color:#6b7280; font-size:10px; margin-left:6px; }
  #panel .close { float:right; cursor:pointer; color:#8a8f99; font-size:18px; line-height:1; }
  .me   { color:#2ecc71; font-weight:bold; }
  .peer { color:#3498db; }
  .solid { background:#27ae60; }
  .dashed { background:#7f8c8d; }
  #toast-container { position:fixed; bottom:18px; right:18px; display:flex; flex-direction:column; gap:8px; z-index:10; }
  .toast { background:#1f242e; border:1px solid #2d343f; padding:8px 14px; border-radius:6px; font-size:12px; max-width:340px;
           animation: slidein .25s ease-out, fadeout .4s ease-in 3.6s forwards; }
  @keyframes slidein { from { transform: translateX(20px); opacity:0 } to { transform: translateX(0); opacity:1 } }
  @keyframes fadeout { to { opacity:0; transform: translateX(20px) } }
</style>
</head>
<body>
<div id="app">
  <div id="header">
    <h1>Grafo de peers <span class="peer">p2p-chat</span></h1>
    <div id="status"><span class="pulse"></span><span id="status-text">conectando…</span></div>
    <div class="stats" id="stats">—</div>
    <div class="legend">
      <span><span class="swatch solid"></span>mutua</span>
      <span><span class="swatch dashed"></span>un sentido</span>
      <span class="me">● tú</span>
      <span class="peer">● otros</span>
    </div>
  </div>
  <div id="main">
    <div id="net"></div>
    <aside id="panel" class="hidden">
      <span class="close" onclick="window.__hidePanel()">×</span>
      <h2 id="p-title"><span id="p-id" style="flex:1; word-break:break-all">—</span><button class="copy" onclick="window.__copyPeerId()">📋 copiar</button></h2>
      <div class="meta" id="p-meta"></div>
      <div id="self-note" class="self-note" style="display:none">
        este es <strong>tu</strong> peerId. No puedes mandarte cosas a ti mismo —
        compártelo con otra persona para que te localice.
      </div>
      <div class="actions" id="p-actions">
        <button class="primary" onclick="window.__sendDm()">enviar DM</button>
        <button onclick="window.__sendWhisper()">🤫 whisper</button>
        <button onclick="window.__sendPing()">⚡ ping</button>
        <button onclick="window.__doHighlight()">★ highlight</button>
        <button onclick="window.__doCall()">📞 llamar</button>
      </div>
      <textarea id="p-text" placeholder="texto del mensaje…"></textarea>
      <div class="recent">
        <h3>recientes</h3>
        <div id="p-recent">(carga al seleccionar peer)</div>
      </div>
    </aside>
  </div>
</div>
<div id="toast-container"></div>
<script>
(function () {
  const nodes = new vis.DataSet([]);
  const edges = new vis.DataSet([]);
  const container = document.getElementById('net');
  const network = new vis.Network(container, { nodes, edges }, {
    nodes: { shape: 'dot', font: { color: '#fff', size: 14, face: 'monospace' } },
    edges: { smooth: { type: 'continuous' } },
    physics: {
      stabilization: false,
      barnesHut: { springLength: 180, gravitationalConstant: -3500, damping: 0.4 },
    },
    interaction: { hover: true, dragNodes: true, tooltipDelay: 100 },
  });

  const statusEl     = document.getElementById('status');
  const statusTextEl = document.getElementById('status-text');
  const statsEl      = document.getElementById('stats');
  const panelEl      = document.getElementById('panel');
  const pIdEl        = document.getElementById('p-id');
  const pMeta        = document.getElementById('p-meta');
  const pText        = document.getElementById('p-text');
  const pRecent      = document.getElementById('p-recent');
  const pActions     = document.getElementById('p-actions');
  const pSelfNote    = document.getElementById('self-note');
  const toastContainer = document.getElementById('toast-container');

  // Color/tamaño original por nodo, para restaurar después de highlight.
  const originalAttrs = new Map();
  // Peer actualmente abierto en el panel.
  let selectedPeer = null;

  function setStatus(text, on) {
    statusTextEl.textContent = text;
    statusEl.classList.toggle('off', !on);
  }

  function applySnapshot(snap) {
    const prevNodeIds = new Set(nodes.getIds());
    const prevEdgeIds = new Set(edges.getIds());
    const newNodeIds  = new Set(snap.nodes.map(n => n.id));
    const newEdgeIds  = new Set(snap.edges.map(e => e.id));

    for (const id of prevNodeIds) if (!newNodeIds.has(id)) { nodes.remove(id); originalAttrs.delete(id); }
    for (const id of prevEdgeIds) if (!newEdgeIds.has(id)) edges.remove(id);

    // Nuevos nodos: pop-in arrancando con tamaño mínimo → vis-network anima la expansión.
    const arriving = snap.nodes.filter(n => !prevNodeIds.has(n.id));
    if (arriving.length) {
      nodes.update(arriving.map(n => ({ ...n, size: 3 })));
      setTimeout(() => nodes.update(arriving.map(n => ({ id: n.id, size: n.size }))), 120);
    }
    // Nodos existentes: actualizar normalmente.
    nodes.update(snap.nodes.filter(n => prevNodeIds.has(n.id)));

    // Nuevos arcos: flash de color al aparecer.
    const arrivingEdges = snap.edges.filter(e => !prevEdgeIds.has(e.id));
    edges.update(snap.edges);
    if (arrivingEdges.length) {
      setTimeout(() => {
        edges.update(arrivingEdges.map(e => ({ id: e.id, color: { color: '#f1c40f' }, width: (e.width || 1) + 3 })));
        setTimeout(() => edges.update(arrivingEdges.map(e => ({ id: e.id, color: e.color, width: e.width }))), 700);
      }, 200);
    }

    for (const n of snap.nodes) originalAttrs.set(n.id, { color: n.color, size: n.size });

    const s = snap.stats;
    const when = new Date(snap.ts).toLocaleTimeString();
    statsEl.textContent = 'nodos=' + s.total + ' · mutuas=' + s.mutuals + ' · un sentido=' + s.oneway + ' · densidad=' + s.density + '  ·  actualizado ' + when;
    if (selectedPeer) refreshPanel(selectedPeer);
  }

  function applyHighlight(ev) {
    const orig = originalAttrs.get(ev.peerId);
    if (!orig) return;
    // Color y tamaño temporales; vis-network anima la transición.
    nodes.update({ id: ev.peerId, color: { background: ev.color, border: ev.color }, size: orig.size + 14 });
    // Tras ~1.2s, restaurar.
    setTimeout(() => {
      const cur = nodes.get(ev.peerId);
      if (cur) nodes.update({ id: ev.peerId, color: orig.color, size: orig.size });
    }, 1200);
  }

  function applyPingPulse(ev) {
    // Buscar arco entre los dos peers (key siempre menor|mayor).
    const edgeId = ev.from < ev.to ? ev.from + '|' + ev.to : ev.to + '|' + ev.from;
    const edge = edges.get(edgeId);
    const color = ev.phase === 'ping' ? '#f39c12' : '#2ecc71'; // naranja=ping, verde=pong
    if (edge) {
      const origColor = edge.color;
      const origWidth = edge.width || 1;
      edges.update({ id: edgeId, color: { color }, width: origWidth + 4 });
      setTimeout(() => edges.update({ id: edgeId, color: origColor, width: origWidth }), 500);
    } else {
      // Sin arco directo: pulsar el nodo destino.
      const targetId = ev.phase === 'ping' ? ev.to : ev.from;
      const orig = originalAttrs.get(targetId);
      if (orig) {
        nodes.update({ id: targetId, color: { background: color, border: color }, size: orig.size + 8 });
        setTimeout(() => nodes.update({ id: targetId, color: orig.color, size: orig.size }), 500);
      }
    }
  }

  function showToast(text) {
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = text;
    toastContainer.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  async function refreshPanel(peerId) {
    selectedPeer = peerId;
    panelEl.classList.remove('hidden');
    pIdEl.textContent = peerId;
    try {
      const r = await fetch('/api/peer/' + encodeURIComponent(peerId));
      const j = await r.json();
      if (!j.ok) { pMeta.textContent = j.error || 'error'; return; }
      const info = j.info;
      const rtt = info.rttMs !== undefined ? info.rttMs.toFixed(0) + ' ms' : '—';
      pMeta.innerHTML =
        '<div class="kv"><span>conectado</span><span>' + (info.isConnected ? 'sí' : 'no') + '</span></div>' +
        '<div class="kv"><span>RTT</span><span>' + rtt + '</span></div>' +
        '<div class="kv"><span>tú</span><span>' + (info.isMe ? 'sí' : 'no') + '</span></div>' +
        '<div class="kv"><span>sin leer</span><span>' + info.unread + '</span></div>';
      // No tiene sentido mandarte cosas a ti mismo: ocultar acciones + textarea
      // y enseñar la nota explicativa. El botón "copiar" sigue visible para
      // que puedas compartir tu peerId con otros.
      pSelfNote.style.display = info.isMe ? '' : 'none';
      pActions.style.display  = info.isMe ? 'none' : '';
      pText.style.display     = info.isMe ? 'none' : '';
      // Marcar como leídos al abrir el panel.
      if (info.unread > 0) fetch('/api/read/' + encodeURIComponent(peerId), { method: 'POST' });
      // Historial de conexiones (últimas 5, más reciente primero).
      if (info.history && info.history.length) {
        const histHtml = info.history.slice(-5).reverse().map(h => {
          const icon = h.kind === 'connected' ? '\u{1F7E2}' : '\u{1F534}';
          const t = new Date(h.ts).toLocaleTimeString().replace(/[&<>"']/g, '');
          return icon + ' ' + t;
        }).join(' · ');
        pMeta.innerHTML += '<div class="kv" style="margin-top:6px"><span>historial</span><span>' + histHtml + '</span></div>';
      }
      // Recientes
      pRecent.innerHTML = info.recent.length === 0
        ? '<div style="color:#6b7280;font-size:12px">(sin mensajes)</div>'
        : info.recent.map(m => {
            const arrow = m.dir === 'in' ? '←' : '→';
            const cls   = m.dir;
            const t = new Date(m.ts).toLocaleTimeString();
            const text = m.text.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
            return '<div class="msg ' + cls + '"><span class="dir">' + arrow + '</span>' + text + '<span class="t">' + t + '</span></div>';
          }).join('');
    } catch (err) {
      pMeta.textContent = 'error: ' + err.message;
    }
  }

  // ── API helpers ───────────────────────────────────────────────────
  async function api(path, body) {
    try {
      const r = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const j = await r.json();
      if (!j.ok) showToast('error: ' + (j.error || 'unknown'));
      return j;
    } catch (err) {
      showToast('error: ' + err.message);
      return { ok: false };
    }
  }

  // ── Acciones expuestas a botones ──────────────────────────────────
  window.__hidePanel = () => { panelEl.classList.add('hidden'); selectedPeer = null; };
  window.__copyPeerId = async () => {
    if (!selectedPeer) return;
    const btn = document.querySelector('#p-title .copy');
    try {
      // Clipboard API solo funciona en contextos seguros (https o localhost).
      // 127.0.0.1 cuenta como localhost → debería ir bien aquí.
      await navigator.clipboard.writeText(selectedPeer);
      if (btn) { btn.textContent = '✓ copiado'; btn.classList.add('ok'); setTimeout(() => { btn.textContent = '📋 copiar'; btn.classList.remove('ok'); }, 1500); }
    } catch (err) {
      // Fallback: select + execCommand (legacy pero universal).
      const ta = document.createElement('textarea');
      ta.value = selectedPeer;
      document.body.appendChild(ta); ta.select();
      try { document.execCommand('copy'); showToast('peerId copiado'); }
      catch { showToast('no se pudo copiar — selecciona manualmente'); }
      ta.remove();
    }
  };
  window.__sendDm = async () => {
    if (!selectedPeer) return;
    const text = pText.value.trim();
    if (!text) { showToast('escribe algo primero'); return; }
    const r = await api('/api/dm', { to: selectedPeer, text });
    if (r.ok) { pText.value = ''; showToast('DM enviado'); refreshPanel(selectedPeer); }
  };
  window.__sendWhisper = async () => {
    if (!selectedPeer) return;
    const text = pText.value.trim();
    if (!text) { showToast('escribe algo primero'); return; }
    const r = await api('/api/whisper', { to: selectedPeer, text });
    if (r.ok) { pText.value = ''; refreshPanel(selectedPeer); }
  };
  window.__sendPing = async () => {
    if (!selectedPeer) return;
    await api('/api/ping', { to: selectedPeer });
  };
  window.__doHighlight = async () => {
    if (!selectedPeer) return;
    await api('/api/highlight', { to: selectedPeer });
  };
  window.__doCall = async () => {
    if (!selectedPeer) return;
    const r = await api('/api/call', { to: selectedPeer });
    if (r.ok) showToast('llamando…'); else showToast('no se pudo llamar: ' + (r.error || ''));
  };

  // Click en nodo → abrir panel
  network.on('selectNode', (params) => {
    const id = params.nodes[0];
    if (id) refreshPanel(id);
  });
  network.on('deselectNode', () => window.__hidePanel());

  // ── SSE ───────────────────────────────────────────────────────────
  const ev = new EventSource('/events');
  ev.onopen = () => setStatus('en vivo', true);
  ev.onerror = () => setStatus('reconectando…', false);
  ev.onmessage = (m) => {
    try {
      const data = JSON.parse(m.data);
      if (data.kind === 'snapshot') applySnapshot(data);
      else if (data.kind === 'highlight') applyHighlight(data);
      else if (data.kind === 'toast') showToast(data.text);
      else if (data.kind === 'ping-pulse') applyPingPulse(data);
    } catch (err) {
      console.error('evento inválido', err);
    }
  };
})();
</script>
</body>
</html>`;
