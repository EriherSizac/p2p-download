/**
 * MAIN — orquestación de capas + CLI
 * ----------------------------------
 * Junta las capas (descubrimiento → transporte → protocolo → catálogo) y
 * expone una CLI sobre `readline`. Aquí no hay lógica de aplicación: solo
 * cableado entre componentes y traducción de comandos del usuario.
 *
 * 💡 Nota didáctica: separar la composición (este archivo) del comportamiento
 * (los módulos de capa) facilita los tests unitarios y deja una "vista de
 * pájaro" muy clara del sistema.
 */

import readline from 'node:readline';
import path from 'node:path';
import { Discovery } from './discovery.js';
import { Transport } from './transport.js';
import { generatePeerId, shortId, MSG, type Message, type FileSummary, type FileManifest } from './protocol.js';
import { createLogger } from './logger.js';
import { FileIndex } from './index-files.js';

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

  // Catálogos remotos cacheados a medida que llegan LIST_REPLY/MANIFEST_REPLY.
  const remoteCatalog = new Map<string, FileSummary[]>(); // peerId → archivos
  const remoteManifests = new Map<string, FileManifest>(); // `${peerId}:${fileId}` → manifest

  // Promesas pendientes de respuesta — se resuelven al recibir el reply.
  const pendingList = new Map<string, PendingResolver<FileSummary[]>>();
  const pendingManifest = new Map<string, PendingResolver<FileManifest>>(); // key = `${peerId}:${fileId}`

  // Conexiones automáticas con tie-break lexicográfico.
  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId) {
      transport.connect(p.peerId, p.host, p.port);
    }
  });

  transport.on('message', (from, msg: Message) => {
    handleMessage(from, msg);
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
          transport.send(from, { type: MSG.ERROR, code: 'NOT_FOUND', message: `fileId ${msg.fileId.slice(0, 8)} no disponible` });
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
        // Pista: process.stdout.write('\r…\n') + rl.prompt(true).
        log.info(`[chat][${shortId(from)}] ${msg.text}`);
        break;
      default:
        log.debug(`msg tipo 0x${msg.type.toString(16)} de ${shortId(from)}`);
    }
  }

  function requestList(targetPeer: string, timeoutMs = 3000): Promise<FileSummary[]> {
    return new Promise((resolve, reject) => {
      if (!transport.isConnected(targetPeer)) {
        reject(new Error('peer no conectado'));
        return;
      }
      const timer = setTimeout(() => {
        pendingList.delete(targetPeer);
        reject(new Error('timeout esperando LIST_REPLY'));
      }, timeoutMs);
      pendingList.set(targetPeer, { resolve, reject, timer });
      transport.send(targetPeer, { type: MSG.LIST });
    });
  }

  setupCli({
    discovery,
    transport,
    index,
    remoteCatalog,
    requestList,
  });

  const shutdown = (): void => {
    log.info('cerrando…');
    transport.broadcast({ type: MSG.BYE });
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
  remoteCatalog: Map<string, FileSummary[]>;
  requestList: (peerId: string) => Promise<FileSummary[]>;
}

function setupCli(ctx: CliCtx): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.setPrompt('p2p> ');
  rl.prompt();

  const out = (s: string): void => {
    process.stdout.write(s.endsWith('\n') ? s : s + '\n');
  };

  /** Resuelve un prefijo de peerId al peerId completo si es único. */
  const resolvePeer = (prefix: string): string | undefined => {
    const all = ctx.discovery.list().map((p) => p.peerId);
    const matches = all.filter((id) => id.startsWith(prefix));
    if (matches.length === 1) return matches[0];
    return undefined;
  };

  rl.on('line', async (line) => {
    const [cmd, ...rest] = line.trim().split(/\s+/);
    try {
      switch (cmd) {
        case '':
          break;
        case 'peers': {
          const list = ctx.discovery.list();
          if (list.length === 0) {
            out('(no hay peers descubiertos)');
          } else {
            for (const p of list) {
              const conn = ctx.transport.isConnected(p.peerId) ? 'conectado' : 'descubierto';
              out(`  ${shortId(p.peerId)}  ${p.host}:${p.port}  ${conn}`);
            }
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
          out('  fileId    name                                size       piezas');
          for (const f of files) {
            out(`  ${shortId(f.fileId)}  ${f.name.padEnd(36).slice(0, 36)}  ${String(f.size).padStart(10)}  ${f.numPieces}`);
          }
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
        case 'get':
        case 'status':
          out(`(${cmd}) disponible en etapas siguientes`);
          break;
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
