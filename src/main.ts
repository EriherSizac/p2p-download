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

const log = createLogger('main');

const TCP_PORT = Number(process.env['TCP_PORT'] ?? 0);
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41234);
const SHARED_DIR = path.resolve(process.env['SHARED_DIR'] ?? './shared');
const DOWNLOAD_DIR = path.resolve(process.env['DOWNLOAD_DIR'] ?? './downloads');
const MANIFESTS_DIR = path.join(DOWNLOAD_DIR, '.manifests');

interface PendingResolver<T> {
  resolve: (v: T) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout;
}

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  log.info(`peerId = ${shortId(peerId)} (full=${peerId})`);

  const index = new FileIndex(SHARED_DIR, MANIFESTS_DIR);
  await index.init();

  const transport = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  const scheduler = new Scheduler(transport, index, DOWNLOAD_DIR, MANIFESTS_DIR);
  scheduler.start();

  const remoteCatalog = new Map<string, FileSummary[]>();
  const remoteManifests = new Map<string, FileManifest>(); // `${peerId}:${fileId}`

  const pendingList = new Map<string, PendingResolver<FileSummary[]>>();
  const pendingManifest = new Map<string, PendingResolver<FileManifest>>();

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
      case MSG.CHAT:
        // TODO(ALUMNO): imprimir el chat sin romper el prompt de readline.
        log.info(`[chat][${shortId(from)}] ${msg.text}`);
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

  setupCli({
    discovery,
    transport,
    index,
    scheduler,
    remoteCatalog,
    requestList,
    requestManifest,
  });

  const shutdown = (): void => {
    log.info('cerrando…');
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
}

function setupCli(ctx: CliCtx): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.setPrompt('p2p> ');
  rl.prompt();

  const out = (s: string): void => {
    process.stdout.write(s.endsWith('\n') ? s : s + '\n');
  };

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
        case 'msg': {
          // TODO(ALUMNO): implementar mensajería directa.
          // - Validar rest[0] como prefijo de peerId conectado.
          // - Construir Message CHAT con {text: rest.slice(1).join(' '), ts: Date.now()}.
          // - transport.send(peerId, msg). Si false, avisar al usuario.
          out('(msg) ejercicio para alumnos — ver docs/EXERCISES.md');
          break;
        }
        case 'quit':
        case 'exit':
          rl.close();
          process.kill(process.pid, 'SIGINT');
          return;
        case 'help':
          out('comandos: peers | list <peerId> | get <peerId> <name> | status | msg <peerId> <texto> | quit');
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
