/**
 * MAIN — orquestación de capas + CLI
 * ----------------------------------
 * Junta las capas (descubrimiento → transporte → protocolo → catálogo →
 * scheduler) y expone una CLI sobre `readline`. Aquí no hay lógica de
 * aplicación: solo cableado entre componentes y traducción de comandos del
 * usuario en llamadas a las capas.
 */

import readline from 'node:readline';
import path from 'node:path';
import fs from 'node:fs';
import { Discovery } from './discovery.js';
import { Transport } from './transport.js';
import {
  generatePeerId,
  shortId,
  MSG,
  type Message,
  type FileSummary,
  type FileManifest,
} from './protocol.js';
import { createLogger } from './logger.js';
import { FileIndex } from './index-files.js';
import { Scheduler } from './scheduler.js';
import { isSupported as fwSupported, ruleExistsForPort, requestFirewallRule } from './firewall.js';
import { GraphServer, type GraphActions, type GraphSnapshot, type RemoteFileSummary } from './graph-server.js';
import { spawn as spawnChild } from 'node:child_process';

const log = createLogger('main');

// Puerto TCP fijo por defecto (41237). Con un fijo basta UNA regla de
// firewall inbound y persiste entre arranques. Override con `TCP_PORT=<otro>`
// si necesitas otra instancia paralela (y crea otra regla).
const TCP_PORT = Number(process.env['TCP_PORT'] ?? 41237);
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41234);
const SHARED_DIR = path.resolve(process.env['SHARED_DIR'] ?? './shared');
const DOWNLOAD_DIR = path.resolve(process.env['DOWNLOAD_DIR'] ?? './downloads');
const MANIFESTS_DIR = path.join(DOWNLOAD_DIR, '.manifests');

interface PendingResolver<T> {
  resolve: (v: T) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout;
}

/**
 * Prompt sí/no en stdin sin readline persistente. Lo usamos antes de
 * arrancar la CLI principal para no chocar con su propio readline.
 */
function askYesNo(question: string): Promise<boolean> {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(question, (ans) => {
      rl.close();
      resolve(/^y(es)?$/i.test(ans.trim()));
    });
  });
}

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  log.info(`peerId = ${shortId(peerId)} (full=${peerId})`);

  const index = new FileIndex(SHARED_DIR, MANIFESTS_DIR);
  await index.init();

  const transport = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  // Firewall (solo Windows): si no hay regla inbound TCP para nuestro puerto,
  // ofrecer crearla con UAC ANTES de arrancar discovery. Evita que el primer
  // round de peers nos conecte por TCP y muera por ETIMEDOUT.
  if (fwSupported()) {
    const tcpOk = await ruleExistsForPort(actualPort);
    if (!tcpOk) {
      const yes = await askYesNo(
        `firewall: no hay regla inbound TCP para :${actualPort}. ¿Crearla? (pedirá UAC) [y/N] `,
      );
      if (yes) {
        log.info('solicitando elevación…');
        const created = await requestFirewallRule(actualPort);
        log.info(created ? `regla creada para :${actualPort}` : 'no se creó (UAC denegado o error)');
      } else {
        log.warn('firewall sin abrir; peers remotos quizá no puedan conectar.');
      }
    } else {
      log.debug(`firewall: regla para :${actualPort} ya existe`);
    }
  }

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  const scheduler = new Scheduler(transport, index, DOWNLOAD_DIR, MANIFESTS_DIR);
  scheduler.start();

  // Hot reindex: si el usuario añade/quita archivos en SHARED_DIR, los
  // reindexamos. Debounce para evitar tormentas durante una copia masiva.
  let reindexTimer: NodeJS.Timeout | undefined;
  try {
    fs.watch(SHARED_DIR, () => {
      if (reindexTimer) clearTimeout(reindexTimer);
      reindexTimer = setTimeout(() => {
        void index.refresh().then(() => {
          // Anunciar nuestro nuevo catálogo a los peers conectados sin
          // re-crear peer-state (perderíamos bitfields remotos / in-flight).
          for (const f of index.list()) {
            const numBytes = Math.ceil(f.numPieces / 8);
            const full = Buffer.alloc(numBytes);
            for (let i = 0; i < f.numPieces; i++) full[i >> 3] = (full[i >> 3] ?? 0) | (1 << (7 - (i & 7)));
            transport.broadcast({
              type: MSG.HAVE,
              fileId: f.fileId,
              bitfield: full.toString('base64'),
            });
          }
        });
      }, 500);
    });
  } catch (err) {
    log.warn('fs.watch no disponible:', (err as Error).message);
  }

  const remoteCatalog = new Map<string, FileSummary[]>();
  const remoteManifests = new Map<string, FileManifest>(); // `${peerId}:${fileId}`

  const pendingList = new Map<string, PendingResolver<FileSummary[]>>();
  const pendingManifest = new Map<string, PendingResolver<FileManifest>>();

  // ─── Topología (gossip PEER_LIST) ────────────────────────────────────
  const peerLists = new Map<string, { peers: string[]; ts: number }>();

  /** Lista de conectados que enviamos a un peer en cada gossip. */
  const sendPeerList = (to: string): void => {
    transport.send(to, { type: MSG.PEER_LIST, peers: transport.connectedPeers() });
  };

  // ─── Live graph server (lazy) ────────────────────────────────────────
  let graphServer: GraphServer | undefined;

  /** Construye un snapshot vis-network del grafo actual. */
  const buildSnapshot = (): GraphSnapshot => {
    const me = peerId;
    const adj = new Map<string, Set<string>>();
    adj.set(me, new Set(transport.connectedPeers()));
    for (const [pid, info] of peerLists) adj.set(pid, new Set(info.peers));

    const nodeSet = new Set<string>(adj.keys());
    for (const targets of adj.values()) for (const t of targets) nodeSet.add(t);

    const visNodes = [...nodeSet].map((pid) => {
      const files = remoteCatalog.get(pid)?.length ?? 0;
      const tip = pid + (files > 0 ? `\nshares ${files} archivo(s)` : '');
      return {
        id: pid,
        label: shortId(pid) + (pid === me ? '\n(tú)' : files > 0 ? `\n📂 ${files}` : ''),
        color: pid === me
          ? { background: '#2ecc71', border: '#27ae60' }
          : { background: '#3498db', border: '#2980b9' },
        size: pid === me ? 28 : 22,
        title: tip,
      };
    });

    const edgeMap = new Map<string, { a: string; b: string; mutual: boolean }>();
    for (const [a, targets] of adj) {
      for (const b of targets) {
        const key = a < b ? `${a}|${b}` : `${b}|${a}`;
        const existing = edgeMap.get(key);
        if (existing) existing.mutual = true;
        else edgeMap.set(key, { a, b, mutual: adj.get(b)?.has(a) ?? false });
      }
    }
    const visEdges = [...edgeMap.entries()].map(([id, e]) => ({
      id, from: e.a, to: e.b,
      dashes: !e.mutual,
      color: { color: e.mutual ? '#27ae60' : '#7f8c8d' },
      width: e.mutual ? 2 : 1,
    }));

    const mutuals = [...edgeMap.values()].filter((e) => e.mutual).length;
    const total = nodeSet.size;
    const maxEdges = (total * (total - 1)) / 2;
    return {
      nodes: visNodes,
      edges: visEdges,
      stats: {
        total, mutuals,
        oneway: edgeMap.size - mutuals,
        density: maxEdges > 0 ? (mutuals / maxEdges).toFixed(2) : '0.00',
      },
      ts: Date.now(),
    };
  };

  /** Empuja snapshot si server activo con clientes — barato, idempotente. */
  const pushSnapshot = (): void => {
    if (!graphServer || !graphServer.hasClients()) return;
    graphServer.emit(buildSnapshot());
  };

  /** Construye GraphActions con acceso al estado del proceso. */
  function buildGraphActions(): GraphActions {
    return {
      async listRemote(peerId: string): Promise<RemoteFileSummary[]> {
        // Si tenemos cache reciente, lo devolvemos sin pegarle al peer.
        const cached = remoteCatalog.get(peerId);
        const files = cached ?? await requestList(peerId);
        return files.map((f) => ({
          fileId: f.fileId,
          name: f.name,
          size: f.size,
          numPieces: f.numPieces,
        }));
      },
      async download(fileId: string) {
        // Buscar el manifest en algún peer que lo tenga.
        let manifest: FileManifest | undefined;
        for (const [pid, files] of remoteCatalog) {
          if (files.some((f) => f.fileId === fileId)) {
            try {
              manifest = await requestManifest(pid, fileId);
              break;
            } catch { /* probar siguiente seeder */ }
          }
        }
        if (!manifest) return { ok: false, error: 'no se encontró manifest en ningún peer' };
        scheduler
          .startDownload(manifest)
          .then(() => {
            graphServer?.toast(`✔ descargado: ${manifest!.name}`);
            pushSnapshot();
          })
          .catch((err) => {
            graphServer?.toast(`✘ ${manifest!.name}: ${(err as Error).message}`);
          });
        return { ok: true };
      },
      peerInfo(peerId: string) {
        const sharedFiles = remoteCatalog.get(peerId)?.length ?? 0;
        // Downloads activas globales — no podemos filtrar por peer porque
        // el scheduler no expone esa relación. Mostramos todo.
        const downloads = scheduler.status().map((d) => ({
          fileId: '',
          name: d.name,
          totalPieces: d.total,
          havePieces: d.have,
          ratio: d.total > 0 ? d.have / d.total : 0,
        }));
        return {
          peerId,
          isMe: peerId === ctx_peerId(),
          isConnected: transport.isConnected(peerId),
          sharedFiles,
          downloads,
        };
      },
    };
  }
  const ctx_peerId = (): string => peerId;

  /** Lanza server (lazy) + abre navegador. Re-emite snapshot inicial. */
  const openLiveGraph = async (): Promise<string> => {
    if (!graphServer) {
      graphServer = new GraphServer();
      await graphServer.start();
      graphServer.setActions(buildGraphActions());
    }
    graphServer.emit(buildSnapshot());
    openInBrowser(graphServer.url());
    return graphServer.url();
  };

  // PEER_LIST inicial al conectar + cambios → snapshot.
  transport.on('peer-connected', (pid) => { sendPeerList(pid); pushSnapshot(); });
  transport.on('peer-disconnected', (pid) => { peerLists.delete(pid); pushSnapshot(); });

  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId) {
      transport.connect(p.peerId, p.host, p.port);
    }
  });

  transport.on('peer-connected', (id) => {
    scheduler.onPeerConnected(id);
  });
  transport.on('peer-disconnected', (id) => {
    scheduler.onPeerDisconnected(id);
  });

  transport.on('message', (from, msg: Message) => {
    handleMessage(from, msg);
    // El scheduler se interesa por HAVE/REQUEST/PIECE.
    scheduler.handleMessage(from, msg);
  });

  function handleMessage(from: string, msg: Message): void {
    switch (msg.type) {
      case MSG.BYE:
        log.debug(`bye de ${shortId(from)}`);
        break;
      case MSG.LIST:
        transport.send(from, { type: MSG.LIST_REPLY, files: index.list() });
        break;
      case MSG.LIST_REPLY: {
        remoteCatalog.set(from, msg.files);
        const p = pendingList.get(from);
        if (p) {
          clearTimeout(p.timer);
          pendingList.delete(from);
          p.resolve(msg.files);
        }
        // Llegó catálogo nuevo → highlight + snapshot (etiqueta de nodo
        // cambia con el número de archivos compartidos).
        graphServer?.highlight(from, { color: '#3498db', label: '📂 catálogo' });
        pushSnapshot();
        break;
      }
      case MSG.PEER_LIST: {
        // Gossip topológico — no reenviar (un hop).
        peerLists.set(from, { peers: msg.peers, ts: Date.now() });
        pushSnapshot();
        break;
      }
      case MSG.MANIFEST: {
        const entry = index.getByFileId(msg.fileId);
        if (!entry) {
          transport.send(from, {
            type: MSG.ERROR,
            code: 'NOT_FOUND',
            message: `fileId ${shortId(msg.fileId)} no disponible`,
          });
          return;
        }
        transport.send(from, { type: MSG.MANIFEST_REPLY, manifest: entry.manifest });
        break;
      }
      case MSG.MANIFEST_REPLY: {
        const key = `${from}:${msg.manifest.fileId}`;
        remoteManifests.set(key, msg.manifest);
        const p = pendingManifest.get(key);
        if (p) {
          clearTimeout(p.timer);
          pendingManifest.delete(key);
          p.resolve(msg.manifest);
        }
        break;
      }
      case MSG.ERROR:
        log.warn(`error de ${shortId(from)}: ${msg.code} ${msg.message}`);
        break;
      default:
        // HAVE/REQUEST/PIECE los maneja el scheduler.
        break;
    }
  }

  function requestList(target: string, timeoutMs = 3000): Promise<FileSummary[]> {
    return new Promise((resolve, reject) => {
      if (!transport.isConnected(target)) {
        reject(new Error('peer no conectado'));
        return;
      }
      const timer = setTimeout(() => {
        pendingList.delete(target);
        reject(new Error('timeout esperando LIST_REPLY'));
      }, timeoutMs);
      pendingList.set(target, { resolve, reject, timer });
      transport.send(target, { type: MSG.LIST });
    });
  }

  function requestManifest(target: string, fileId: string, timeoutMs = 5000): Promise<FileManifest> {
    return new Promise((resolve, reject) => {
      if (!transport.isConnected(target)) {
        reject(new Error('peer no conectado'));
        return;
      }
      const key = `${target}:${fileId}`;
      const cached = remoteManifests.get(key);
      if (cached) { resolve(cached); return; }
      const timer = setTimeout(() => {
        pendingManifest.delete(key);
        reject(new Error('timeout esperando MANIFEST_REPLY'));
      }, timeoutMs);
      pendingManifest.set(key, { resolve, reject, timer });
      transport.send(target, { type: MSG.MANIFEST, fileId });
    });
  }

  // Gossip periódico de PEER_LIST. 15s = balance entre frescura del grafo
  // web y tráfico de fondo. Es independiente de eventos peer-connected
  // (que mandan PEER_LIST inmediato) — sirve para mantener sincronizados
  // peers que llevan rato conectados pero cuyos vecinos cambiaron.
  const peerListTimer = setInterval(() => {
    for (const pid of transport.connectedPeers()) sendPeerList(pid);
  }, 15_000);

  setupCli({
    discovery,
    transport,
    index,
    scheduler,
    remoteCatalog,
    requestList,
    requestManifest,
    openLiveGraph,
    graphHighlight: (pid, opts) => graphServer?.highlight(pid, opts),
  });

  const shutdown = (): void => {
    log.info('cerrando…');
    clearInterval(peerListTimer);
    graphServer?.stop();
    transport.broadcast({ type: MSG.BYE });
    scheduler.stop();
    discovery.stop();
    transport.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

interface CliCtx {
  discovery: Discovery;
  transport: Transport;
  index: FileIndex;
  scheduler: Scheduler;
  remoteCatalog: Map<string, FileSummary[]>;
  requestList: (peerId: string) => Promise<FileSummary[]>;
  requestManifest: (peerId: string, fileId: string) => Promise<FileManifest>;
  openLiveGraph: () => Promise<string>;
  graphHighlight?: (peerId: string, opts?: { color?: string; label?: string }) => void;
}

/**
 * Abre URL/fichero en el navegador del sistema. Mismo helper que en p2p-chat
 * porque ambos proyectos lo necesitan y queremos mantener cada uno independiente.
 *   - Windows: `cmd /c start "" <url>` (el "" es título obligatorio)
 *   - macOS:   `open <url>`
 *   - Linux:   `xdg-open <url>`
 */
function openInBrowser(target: string): void {
  const spawnDetached = (cmd: string, args: string[]): void => {
    try {
      const child = spawnChild(cmd, args, { detached: true, stdio: 'ignore' });
      child.unref();
    } catch { /* no-op */ }
  };
  if (process.platform === 'win32') spawnDetached('cmd', ['/c', 'start', '""', target]);
  else if (process.platform === 'darwin') spawnDetached('open', [target]);
  else spawnDetached('xdg-open', [target]);
}

function setupCli(ctx: CliCtx): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.setPrompt('p2p> ');

  const out = (s: string): void => {
    process.stdout.write(s.endsWith('\n') ? s : s + '\n');
  };

  const printMenu = (): void => {
    out('');
    out('  ┌─ menú · p2p-files ───────────────────────────────────┐');
    out('  │  share                 lista mis archivos publicados │');
    out('  │  share <ruta>          publica un archivo (cualquier │');
    out('  │                        ruta absoluta o relativa)     │');
    out('  │  unshare <nombre>      retira del catálogo           │');
    out('  │  search [nombre]       busca en peers conectados     │');
    out('  │  get <peerId> <name>   descarga un archivo           │');
    out('  │  peers | status        info del enjambre             │');
    out('  │  graph [open]          grafo ASCII | live HTML+SSE   │');
    out('  │  ping <peerId>         pulso visual en grafo web     │');
    out('  │  highlight <p> [color] marca visual en grafo web     │');
    out('  │  menu | help | quit                                  │');
    out('  └──────────────────────────────────────────────────────┘');
  };

  printMenu();
  rl.prompt();

  const resolvePeer = (prefix: string): string | undefined => {
    const all = ctx.discovery.list().map((p) => p.peerId);
    const matches = all.filter((id) => id.startsWith(prefix));
    return matches.length === 1 ? matches[0] : undefined;
  };

  rl.on('line', async (line) => {
    const [cmd, ...rest] = line.trim().split(/\s+/);
    try {
      switch (cmd) {
        case '':
          break;
        case 'peers': {
          const list = ctx.discovery.list();
          if (list.length === 0) { out('(no hay peers descubiertos)'); break; }
          for (const p of list) {
            const conn = ctx.transport.isConnected(p.peerId) ? 'conectado' : 'descubierto';
            out(`  ${shortId(p.peerId)}  ${p.host}:${p.port}  ${conn}`);
          }
          break;
        }
        case 'list': {
          const prefix = rest[0];
          if (!prefix) { out('uso: list <peerId>'); break; }
          const target = resolvePeer(prefix);
          if (!target) { out('peer no resuelto (prefijo ambiguo o desconocido)'); break; }
          if (!ctx.transport.isConnected(target)) { out('peer no conectado todavía'); break; }
          const files = await ctx.requestList(target);
          if (files.length === 0) { out('(catálogo vacío)'); break; }
          out('  fileId    name                                      size       piezas');
          for (const f of files) {
            out(`  ${shortId(f.fileId)}  ${f.name.padEnd(40).slice(0, 40)}  ${String(f.size).padStart(10)}  ${f.numPieces}`);
          }
          break;
        }
        case 'get': {
          const prefix = rest[0];
          const name = rest.slice(1).join(' ');
          if (!prefix || !name) { out('uso: get <peerId> <nombreArchivo>'); break; }
          const target = resolvePeer(prefix);
          if (!target) { out('peer no resuelto'); break; }
          if (!ctx.transport.isConnected(target)) { out('peer no conectado'); break; }
          // Asegurar catálogo del peer; preferimos uno fresco.
          let files = ctx.remoteCatalog.get(target);
          if (!files) files = await ctx.requestList(target);
          const summary = files.find((f) => f.name === name);
          if (!summary) { out(`archivo "${name}" no está en el catálogo de ${shortId(target)}`); break; }
          const manifest = await ctx.requestManifest(target, summary.fileId);
          out(`comenzando descarga ${manifest.name} (${manifest.size} bytes, ${manifest.numPieces} piezas)`);
          ctx.scheduler
            .startDownload(manifest)
            .then((p) => out(`✔ descargado: ${p}`))
            .catch((err) => out(`✘ falló descarga: ${err.message}`));
          break;
        }
        case 'graph': {
          // `graph open` arranca el server web con grafo + catálogo en vivo.
          // Sin args, mensaje breve apuntando a la versión interactiva (en
          // este proyecto no hay render ASCII separado — el HTML hace todo).
          const sub = rest[0];
          if (sub === 'open' || sub === 'live' || sub === 'html') {
            const url = await ctx.openLiveGraph();
            out(`📊 grafo en vivo: ${url}`);
            out('   click en un nodo para ver su catálogo y descargar archivos');
          } else {
            out('uso: graph open  (abre HTML interactivo con SSE)');
          }
          break;
        }

        case 'ping': {
          const prefix = rest[0];
          if (!prefix) { out('uso: ping <peerId>'); break; }
          const target = resolvePeer(prefix);
          if (!target) { out('peer no resuelto'); break; }
          ctx.graphHighlight?.(target, { color: '#f1c40f', label: '⚡ ping' });
          out(`⚡ ping → ${shortId(target)} (mira el grafo web)`);
          break;
        }

        case 'highlight': {
          const prefix = rest[0];
          if (!prefix) { out('uso: highlight <peerId> [color]'); break; }
          const target = resolvePeer(prefix);
          if (!target) { out('peer no resuelto'); break; }
          const color = rest[1] ?? '#9b59b6';
          ctx.graphHighlight?.(target, { color, label: '★ highlight' });
          out(`★ highlight → ${shortId(target)}`);
          break;
        }

        case 'status': {
          const ds = ctx.scheduler.status();
          if (ds.length === 0) { out('(sin descargas activas)'); }
          for (const d of ds) {
            const pct = ((d.have / d.total) * 100).toFixed(1);
            out(`  ${d.name}  ${d.have}/${d.total} piezas  ${pct}%  ${d.rateKBs} KB/s`);
          }
          out(`  peers conectados: ${ctx.transport.connectedPeers().length}`);
          break;
        }
        case 'share': {
          // Sin argumentos: imprimir catálogo local. Con ruta: añadir.
          if (rest.length === 0) {
            const list = ctx.index.list();
            if (list.length === 0) {
              out('Catálogo local vacío. No hay archivos publicados.');
              out('Para publicar un archivo:');
              out(`  · uso interactivo:  share <ruta>`);
              out(`  · o copia archivos en  ${SHARED_DIR}  (se reindexan en caliente).`);
              break;
            }
            out('Archivos publicados en este peer:');
            out('  fileId    name                                      size       piezas');
            for (const f of list) {
              out(`  ${shortId(f.fileId)}  ${f.name.padEnd(40).slice(0, 40)}  ${String(f.size).padStart(10)}  ${f.numPieces}`);
            }
            break;
          }

          // share <ruta>: la ruta puede contener espacios → reunirla.
          const raw = rest.join(' ').replace(/^['"]|['"]$/g, '');
          const manifest = await ctx.index.addPath(raw);

          // Anunciar HAVE con bitfield lleno a todos los peers conectados.
          const numBytes = Math.ceil(manifest.numPieces / 8);
          const full = Buffer.alloc(numBytes);
          for (let i = 0; i < manifest.numPieces; i++) {
            full[i >> 3] = (full[i >> 3] ?? 0) | (1 << (7 - (i & 7)));
          }
          ctx.transport.broadcast({
            type: MSG.HAVE,
            fileId: manifest.fileId,
            bitfield: full.toString('base64'),
          });
          out(`Publicado: ${manifest.name}  (${manifest.size} bytes, ${manifest.numPieces} piezas, fileId=${shortId(manifest.fileId)})`);
          break;
        }
        case 'unshare': {
          const name = rest.join(' ');
          if (!name) { out('uso: unshare <nombreArchivo>'); break; }
          const ok = ctx.index.removeExternal(name);
          out(ok ? `Quitado del catálogo: ${name}` : `No se pudo quitar "${name}" (¿está en SHARED_DIR? hay que borrarlo del disco).`);
          break;
        }
        case 'search': {
          const query = rest.join(' ').toLowerCase();
          const connected = ctx.transport.connectedPeers();
          if (connected.length === 0) { out('no hay peers conectados — espera al descubrimiento'); break; }
          out(`buscando${query ? ` "${query}"` : ''} en ${connected.length} peer(s)…`);
          const results = await Promise.allSettled(
            connected.map(async (pid) => ({ pid, files: await ctx.requestList(pid) })),
          );
          let total = 0;
          out('  peerId    fileId    name                                size');
          for (const r of results) {
            if (r.status !== 'fulfilled') continue;
            const matches = query
              ? r.value.files.filter((f) => f.name.toLowerCase().includes(query))
              : r.value.files;
            for (const f of matches) {
              out(`  ${shortId(r.value.pid)}  ${shortId(f.fileId)}  ${f.name.padEnd(36).slice(0, 36)}  ${String(f.size).padStart(10)}`);
              total += 1;
            }
          }
          out(`(${total} resultado(s)) — usa: get <peerId> <name>`);
          break;
        }
        case 'menu':
          printMenu();
          break;
        case 'quit':
        case 'exit':
          rl.close();
          process.kill(process.pid, 'SIGINT');
          return;
        case 'help':
          out('comandos: peers | list <peerId> | get <peerId> <name> | status | share [ruta] | unshare <name> | search [nombre] | graph [open] | ping <peerId> | highlight <peerId> [color] | menu | quit');
          break;
        default:
          out(`comando desconocido: ${cmd}`);
      }
    } catch (err) {
      out(`error: ${(err as Error).message}`);
    }
    rl.prompt();
  });
}

bootstrap().catch((err) => {
  log.error('bootstrap falló', err);
  process.exit(1);
});
