/**
 * GRAPH SERVER — visualizador en vivo del grafo de peers (p2p-files).
 *
 * Estructura: HTTP local (127.0.0.1, puerto efímero) sirviendo la página y
 * un stream SSE en `/events`. Cada cambio en topología o descarga dispara
 * un snapshot/highlight/toast hacia los clientes.
 *
 * A diferencia del grafo de p2p-chat, aquí las acciones son específicas
 * del dominio de archivos:
 *   - listar catálogo remoto de un peer
 *   - solicitar descarga de un archivo
 *   - ver progreso de descargas activas
 *
 * Misma estructura que p2p-chat/graph-server, distinta API. Cada proyecto
 * mantiene su propia copia: aprendizaje claro y aislamiento total.
 */

import http from 'node:http';
import { type AddressInfo } from 'node:net';
import { createLogger } from './logger.js';

const log = createLogger('graph-server');

/** Resumen de archivo para la UI web. */
export interface RemoteFileSummary {
  fileId: string;
  name: string;
  size: number;
  numPieces: number;
}

/** Progreso de descarga activa para mostrar en panel. */
export interface DownloadProgress {
  fileId: string;
  name: string;
  totalPieces: number;
  havePieces: number;
  /** 0..1 */
  ratio: number;
}

/** Callbacks del dominio archivos. main.ts las inyecta. */
export interface GraphActions {
  /** Pide la lista (LIST/LIST_REPLY) al peer; usa cache si tiene. */
  listRemote(peerId: string): Promise<RemoteFileSummary[]>;
  /** Inicia descarga del fileId desde la red (todos los seeders). */
  download(fileId: string): Promise<{ ok: boolean; error?: string }>;
  /** Devuelve info ampliada de un peer para el panel. */
  peerInfo(peerId: string): {
    peerId: string;
    isMe: boolean;
    isConnected: boolean;
    sharedFiles: number;     // archivos que SABEMOS que este peer tiene
    downloads: DownloadProgress[]; // descargas en curso que nos interesan
  };
}

/** Eventos push al navegador. */
export type GraphEvent =
  | ({ kind: 'snapshot' } & GraphSnapshot)
  | { kind: 'highlight'; peerId: string; label?: string; color?: string }
  | { kind: 'toast'; text: string };

/** Snapshot del grafo para vis-network. */
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
  ts: number;
}

export class GraphServer {
  private server?: http.Server;
  private clients = new Set<http.ServerResponse>();
  private port = 0;
  private lastSnap?: GraphSnapshot;
  private actions?: GraphActions;

  setActions(actions: GraphActions): void {
    this.actions = actions;
  }

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

  url(): string { return `http://127.0.0.1:${this.port}/`; }

  emit(snap: GraphSnapshot): void {
    this.lastSnap = snap;
    this.broadcast({ kind: 'snapshot', ...snap });
  }

  highlight(peerId: string, opts?: { label?: string; color?: string }): void {
    this.broadcast({ kind: 'highlight', peerId, label: opts?.label, color: opts?.color });
  }

  toast(text: string): void {
    this.broadcast({ kind: 'toast', text });
  }

  hasClients(): boolean {
    return this.clients.size > 0;
  }

  stop(): void {
    for (const c of this.clients) { try { c.end(); } catch { /* ignore */ } }
    this.clients.clear();
    this.server?.close();
    this.server = undefined;
  }

  // --- internos -----------------------------------------------------------

  private broadcast(ev: GraphEvent): void {
    const payload = `data: ${JSON.stringify(ev)}\n\n`;
    for (const c of this.clients) {
      try { c.write(payload); } catch { /* close hará cleanup */ }
    }
  }

  private handle(req: http.IncomingMessage, res: http.ServerResponse): void {
    const url = req.url ?? '/';
    if (url === '/events') return this.handleSse(req, res);

    if (url.startsWith('/api/peer/') && req.method === 'GET')
      { void this.handlePeerInfo(url, res); return; }
    if (url.startsWith('/api/list/') && req.method === 'GET')
      { void this.handleListRemote(url, res); return; }
    if (url === '/api/download' && req.method === 'POST')
      { void this.handleDownload(req, res); return; }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
  }

  private handleSse(req: http.IncomingMessage, res: http.ServerResponse): void {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write(': hola\n\n');
    this.clients.add(res);
    log.debug(`SSE cliente conectado (total=${this.clients.size})`);
    if (this.lastSnap) {
      try { res.write(`data: ${JSON.stringify({ kind: 'snapshot', ...this.lastSnap })}\n\n`); }
      catch { /* ignore */ }
    }
    req.on('close', () => {
      this.clients.delete(res);
      log.debug(`SSE cliente desconectado (restantes=${this.clients.size})`);
    });
  }

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
        try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
        catch (err) { reject(err as Error); }
      });
      req.on('error', reject);
    });
  }

  private json(res: http.ServerResponse, status: number, obj: unknown): void {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(obj));
  }

  private handlePeerInfo(url: string, res: http.ServerResponse): void {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    const peerId = decodeURIComponent(url.slice('/api/peer/'.length));
    if (!peerId) return this.json(res, 400, { ok: false, error: 'peerId required' });
    this.json(res, 200, { ok: true, info: this.actions.peerInfo(peerId) });
  }

  private async handleListRemote(url: string, res: http.ServerResponse): Promise<void> {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    const peerId = decodeURIComponent(url.slice('/api/list/'.length));
    if (!peerId) return this.json(res, 400, { ok: false, error: 'peerId required' });
    try {
      const files = await this.actions.listRemote(peerId);
      this.json(res, 200, { ok: true, files });
    } catch (err) {
      this.json(res, 502, { ok: false, error: (err as Error).message });
    }
  }

  private async handleDownload(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (!this.actions) return this.json(res, 503, { ok: false, error: 'actions not wired' });
    try {
      const body = await this.readJson(req);
      const fileId = String(body['fileId'] ?? '');
      if (!fileId) return this.json(res, 400, { ok: false, error: 'fileId required' });
      const r = await this.actions.download(fileId);
      this.json(res, r.ok ? 200 : 502, r);
    } catch (err) {
      this.json(res, 400, { ok: false, error: (err as Error).message });
    }
  }
}

// ---------------------------------------------------------------------------
// HTML embebido — UI específica de archivos. Click nodo → panel con catálogo
// + botón descargar. Highlights animados al recibir HAVE o completar descarga.
// ---------------------------------------------------------------------------

function fmtSize(bytes: number): string {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

const PAGE_HTML = `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>p2p-files — grafo + catálogo en vivo</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<script>window.__fmtSize = ${fmtSize.toString()};</script>
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
  #panel { width:360px; border-left:1px solid #222; padding:16px; overflow-y:auto; background:#13161e; }
  #panel.hidden { display:none; }
  #panel h2 { margin:0 0 4px; font-size:14px; font-family: ui-monospace, monospace; word-break:break-all; }
  #panel .meta { font-size:11px; color:#8a8f99; margin-bottom:14px; }
  #panel .meta .kv { display:flex; justify-content:space-between; margin-top:3px; }
  #panel .meta .kv span:last-child { font-family: ui-monospace, monospace; color:#cfd6e1; }
  #panel .actions { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }
  #panel button { background:#1f242e; border:1px solid #2d343f; color:#e6e6e6; padding:6px 10px; border-radius:4px; cursor:pointer; font-size:12px; }
  #panel button:hover { background:#2a313d; }
  #panel button.primary { background:#2563eb; border-color:#2563eb; }
  #panel button.primary:hover { background:#1d4fd6; }
  #panel .close { float:right; cursor:pointer; color:#8a8f99; font-size:18px; line-height:1; }
  #panel .section h3 { font-size:11px; color:#8a8f99; text-transform:uppercase; letter-spacing:0.05em; margin:14px 0 6px; }
  .file { background:#0f1117; border:1px solid #1a1f29; border-radius:5px; padding:8px 10px; margin-bottom:6px; }
  .file .name { font-size:12px; font-weight:600; word-break:break-all; }
  .file .det  { font-size:11px; color:#8a8f99; margin-top:2px; }
  .file .det code { font-family: ui-monospace, monospace; color:#cfd6e1; }
  .file .row  { display:flex; justify-content:space-between; align-items:center; margin-top:6px; }
  .progress { height:6px; background:#1a1f29; border-radius:3px; overflow:hidden; margin-top:6px; }
  .progress > div { height:100%; background:#27ae60; transition: width .3s ease; }
  .me   { color:#2ecc71; font-weight:bold; }
  .peer { color:#3498db; }
  .solid { background:#27ae60; }
  .dashed { background:#7f8c8d; }
  #toast-container { position:fixed; bottom:18px; right:18px; display:flex; flex-direction:column; gap:8px; z-index:10; }
  .toast { background:#1f242e; border:1px solid #2d343f; padding:8px 14px; border-radius:6px; font-size:12px; max-width:360px;
           animation: slidein .25s ease-out, fadeout .4s ease-in 3.6s forwards; }
  @keyframes slidein { from { transform: translateX(20px); opacity:0 } to { transform: translateX(0); opacity:1 } }
  @keyframes fadeout { to { opacity:0; transform: translateX(20px) } }
</style>
</head>
<body>
<div id="app">
  <div id="header">
    <h1>Grafo + catálogo <span class="peer">p2p-files</span></h1>
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
      <h2 id="p-title">—</h2>
      <div class="meta" id="p-meta"></div>
      <div class="actions">
        <button class="primary" onclick="window.__loadCatalog()">📂 ver catálogo</button>
        <button onclick="window.__doHighlight()">★ highlight</button>
      </div>
      <div class="section" id="catalog-section">
        <h3>catálogo remoto</h3>
        <div id="catalog">(haz click en "ver catálogo")</div>
      </div>
      <div class="section">
        <h3>descargas activas</h3>
        <div id="downloads">—</div>
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
  const pTitle       = document.getElementById('p-title');
  const pMeta        = document.getElementById('p-meta');
  const catalogEl    = document.getElementById('catalog');
  const downloadsEl  = document.getElementById('downloads');
  const toastContainer = document.getElementById('toast-container');

  const originalAttrs = new Map();
  let selectedPeer = null;
  const escapeHtml = s => String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));

  function setStatus(text, on) {
    statusTextEl.textContent = text;
    statusEl.classList.toggle('off', !on);
  }

  function applySnapshot(snap) {
    const newNodeIds = new Set(snap.nodes.map(n => n.id));
    const newEdgeIds = new Set(snap.edges.map(e => e.id));
    for (const id of nodes.getIds()) if (!newNodeIds.has(id)) { nodes.remove(id); originalAttrs.delete(id); }
    for (const id of edges.getIds()) if (!newEdgeIds.has(id)) edges.remove(id);
    nodes.update(snap.nodes);
    edges.update(snap.edges);
    for (const n of snap.nodes) originalAttrs.set(n.id, { color: n.color, size: n.size });

    const s = snap.stats;
    const when = new Date(snap.ts).toLocaleTimeString();
    statsEl.textContent = 'nodos=' + s.total + ' · mutuas=' + s.mutuals + ' · un sentido=' + s.oneway + ' · densidad=' + s.density + '  ·  actualizado ' + when;
    if (selectedPeer) refreshPanel(selectedPeer);
  }

  function applyHighlight(ev) {
    const orig = originalAttrs.get(ev.peerId);
    if (!orig) return;
    nodes.update({ id: ev.peerId, color: { background: ev.color, border: ev.color }, size: orig.size + 14 });
    setTimeout(() => {
      const cur = nodes.get(ev.peerId);
      if (cur) nodes.update({ id: ev.peerId, color: orig.color, size: orig.size });
    }, 1200);
  }

  function showToast(text) {
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = text;
    toastContainer.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  function renderDownloads(downloads) {
    if (!downloads || downloads.length === 0) { downloadsEl.innerHTML = '<div style="color:#6b7280;font-size:12px">(ninguna)</div>'; return; }
    downloadsEl.innerHTML = downloads.map(d => {
      const pct = Math.round(d.ratio * 100);
      return '<div class="file">' +
        '<div class="name">' + escapeHtml(d.name) + '</div>' +
        '<div class="det">' + d.havePieces + ' / ' + d.totalPieces + ' piezas · ' + pct + '%</div>' +
        '<div class="progress"><div style="width:' + pct + '%"></div></div>' +
      '</div>';
    }).join('');
  }

  async function refreshPanel(peerId) {
    selectedPeer = peerId;
    panelEl.classList.remove('hidden');
    pTitle.textContent = peerId;
    try {
      const r = await fetch('/api/peer/' + encodeURIComponent(peerId));
      const j = await r.json();
      if (!j.ok) { pMeta.textContent = j.error || 'error'; return; }
      const info = j.info;
      pMeta.innerHTML =
        '<div class="kv"><span>conectado</span><span>' + (info.isConnected ? 'sí' : 'no') + '</span></div>' +
        '<div class="kv"><span>tú</span><span>' + (info.isMe ? 'sí' : 'no') + '</span></div>' +
        '<div class="kv"><span>archivos conocidos</span><span>' + info.sharedFiles + '</span></div>';
      renderDownloads(info.downloads);
    } catch (err) {
      pMeta.textContent = 'error: ' + err.message;
    }
  }

  async function api(path, body) {
    try {
      const opts = body ? { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(body) } : {};
      const r = await fetch(path, opts);
      const j = await r.json();
      if (!j.ok) showToast('error: ' + (j.error || 'unknown'));
      return j;
    } catch (err) {
      showToast('error: ' + err.message);
      return { ok: false };
    }
  }

  window.__hidePanel = () => { panelEl.classList.add('hidden'); selectedPeer = null; };

  window.__loadCatalog = async () => {
    if (!selectedPeer) return;
    catalogEl.innerHTML = '<div style="color:#6b7280;font-size:12px">cargando…</div>';
    const j = await api('/api/list/' + encodeURIComponent(selectedPeer));
    if (!j.ok) { catalogEl.innerHTML = '<div style="color:#e74c3c;font-size:12px">' + (j.error || 'error') + '</div>'; return; }
    if (!j.files || j.files.length === 0) {
      catalogEl.innerHTML = '<div style="color:#6b7280;font-size:12px">(este peer no comparte nada)</div>';
      return;
    }
    catalogEl.innerHTML = j.files.map(f => (
      '<div class="file">' +
        '<div class="name">' + escapeHtml(f.name) + '</div>' +
        '<div class="det"><code>' + f.fileId.slice(0, 12) + '…</code> · ' + window.__fmtSize(f.size) + ' · ' + f.numPieces + ' piezas</div>' +
        '<div class="row">' +
          '<button class="primary" onclick="window.__download(\\'' + f.fileId + '\\',\\'' + escapeHtml(f.name).replace(/'/g, "\\\\'") + '\\')">⬇ descargar</button>' +
        '</div>' +
      '</div>'
    )).join('');
  };

  window.__download = async (fileId, name) => {
    const r = await api('/api/download', { fileId });
    if (r.ok) showToast('descargando ' + name + '…');
  };

  window.__doHighlight = async () => {
    if (!selectedPeer) return;
    // No hay endpoint POST de highlight aquí; en p2p-files el highlight
    // se dispara desde eventos del scheduler (HAVE/PIECE/complete). Si el
    // usuario quiere uno manual, lo hacemos puramente local.
    const orig = originalAttrs.get(selectedPeer);
    if (!orig) return;
    nodes.update({ id: selectedPeer, color: { background: '#9b59b6', border: '#9b59b6' }, size: orig.size + 14 });
    setTimeout(() => nodes.update({ id: selectedPeer, color: orig.color, size: orig.size }), 1200);
  };

  network.on('selectNode', (params) => { const id = params.nodes[0]; if (id) refreshPanel(id); });
  network.on('deselectNode', () => window.__hidePanel());

  const ev = new EventSource('/events');
  ev.onopen = () => setStatus('en vivo', true);
  ev.onerror = () => setStatus('reconectando…', false);
  ev.onmessage = (m) => {
    try {
      const data = JSON.parse(m.data);
      if (data.kind === 'snapshot') applySnapshot(data);
      else if (data.kind === 'highlight') applyHighlight(data);
      else if (data.kind === 'toast') showToast(data.text);
    } catch (err) {
      console.error('evento inválido', err);
    }
  };
})();
</script>
</body>
</html>`;
