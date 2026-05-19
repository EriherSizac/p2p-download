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
    const payload = `data: ${JSON.stringify(snap)}\n\n`;
    for (const c of this.clients) {
      // try/catch porque un cliente puede haberse cerrado a medias.
      // Si falla, lo quitaremos en su evento 'close'.
      try { c.write(payload); } catch { /* ignore */ }
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
    if (req.url === '/events') {
      this.handleSse(req, res);
      return;
    }
    // Cualquier otra ruta → la página HTML.
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
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
        res.write(`data: ${JSON.stringify(this.lastSnap)}\n\n`);
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
  #header { padding: 10px 18px; border-bottom: 1px solid #222; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  #header h1 { margin:0; font-size:15px; font-weight:600; }
  #header .stats { font-size:12px; color:#8a8f99; font-family: ui-monospace, monospace; }
  #header .legend { margin-left:auto; font-size:11px; color:#8a8f99; }
  #header .legend span { margin-right:10px; }
  #header .swatch { display:inline-block; width:14px; height:2px; vertical-align:middle; margin-right:4px; }
  #header .pulse { width:8px; height:8px; border-radius:50%; background:#2ecc71; display:inline-block; vertical-align:middle; margin-right:6px; animation: pulse 1.2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  #status { font-size:11px; color:#8a8f99; }
  #status.off .pulse { background:#e74c3c; animation: none; }
  #net { width:100%; height: calc(100% - 56px); }
  .me   { color:#2ecc71; font-weight:bold; }
  .peer { color:#3498db; }
  .solid { background:#27ae60; }
  .dashed { background:#7f8c8d; }
</style>
</head>
<body>
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
<div id="net"></div>
<script>
(function () {
  // DataSets vacíos al inicio. Cada update del SSE los modifica in-place;
  // vis-network observa los cambios y anima nodos/aristas.
  const nodes = new vis.DataSet([]);
  const edges = new vis.DataSet([]);
  const container = document.getElementById('net');
  const network = new vis.Network(container, { nodes, edges }, {
    nodes: { shape: 'dot', font: { color: '#fff', size: 14, face: 'monospace' } },
    edges: { smooth: { type: 'continuous' } },
    physics: {
      stabilization: false, // queremos movimiento continuo, no equilibrio rígido
      barnesHut: { springLength: 180, gravitationalConstant: -3500, damping: 0.4 },
    },
    interaction: { hover: true, dragNodes: true, tooltipDelay: 100 },
  });

  const statusEl     = document.getElementById('status');
  const statusTextEl = document.getElementById('status-text');
  const statsEl      = document.getElementById('stats');

  function setStatus(text, on) {
    statusTextEl.textContent = text;
    statusEl.classList.toggle('off', !on);
  }

  /**
   * Aplica un snapshot al DataSet. Diff manual:
   *   1) elimina nodos/aristas que ya no existen
   *   2) update() añade los nuevos y actualiza los ya existentes
   * DataSet.update() es idempotente y dispara animaciones suaves.
   */
  function applySnapshot(snap) {
    const newNodeIds = new Set(snap.nodes.map(n => n.id));
    const newEdgeIds = new Set(snap.edges.map(e => e.id));

    for (const id of nodes.getIds()) {
      if (!newNodeIds.has(id)) nodes.remove(id);
    }
    for (const id of edges.getIds()) {
      if (!newEdgeIds.has(id)) edges.remove(id);
    }
    nodes.update(snap.nodes);
    edges.update(snap.edges);

    const s = snap.stats;
    const when = new Date(snap.ts).toLocaleTimeString();
    statsEl.textContent = 'nodos=' + s.total + ' · mutuas=' + s.mutuals + ' · un sentido=' + s.oneway + ' · densidad=' + s.density + '  ·  actualizado ' + when;
  }

  // EventSource = wrapper estándar del navegador para SSE. Hace reconexión
  // automática si la conexión se cae (con backoff).
  const ev = new EventSource('/events');
  ev.onopen = () => setStatus('en vivo', true);
  ev.onerror = () => setStatus('reconectando…', false);
  ev.onmessage = (m) => {
    try {
      const snap = JSON.parse(m.data);
      applySnapshot(snap);
    } catch (err) {
      console.error('snapshot inválido', err);
    }
  };
})();
</script>
</body>
</html>`;
