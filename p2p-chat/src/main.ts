/**
 * MAIN — orquestación de capas + CLI (chat)
 * -----------------------------------------
 * Junta las 4 capas (descubrimiento → transporte → framing → protocolo) y
 * expone una CLI sobre `readline` enfocada en mensajería peer-to-peer.
 *
 * Este proyecto es deliberadamente independiente de `p2p-files`: comparte la
 * estructura por capas pero no la implementación, para que el alumno pueda
 * estudiar cada uno por separado sin saltar entre carpetas.
 */

import readline from 'node:readline';
import { Discovery } from './discovery.js';
import { Transport } from './transport.js';
import {
  generatePeerId,
  newMessageId,
  shortId,
  MSG,
  type Message,
} from './protocol.js';
import { createLogger } from './logger.js';

const log = createLogger('main');

const TCP_PORT = Number(process.env['TCP_PORT'] ?? 0);
// Puerto distinto al de p2p-files (41234) para que ambos puedan coexistir
// en la misma LAN sin confundir sus enjambres.
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41235);

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  log.info(`peerId = ${shortId(peerId)} (full=${peerId})`);

  const transport = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  // Sink que setupCli rellena al construir readline; usado desde handleMessage
  // para imprimir chats entrantes sin romper el prompt.
  const cliSinks: { chat?: (from: string, text: string) => void } = {};

  // Tie-break lexicográfico: solo el peerId menor inicia conexión saliente,
  // así evitamos sockets duplicados cuando ambos se descubren a la vez.
  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId) transport.connect(p.peerId, p.host, p.port);
  });

  transport.on('message', (from, msg: Message) => {
    handleMessage(from, msg);
  });

  function handleMessage(from: string, msg: Message): void {
    switch (msg.type) {
      case MSG.BYE:
        log.debug(`bye de ${shortId(from)}`);
        break;
      case MSG.CHAT:
        cliSinks.chat?.(from, msg.text);
        // TODO(ALUMNO): responder con CHAT_ACK al emisor.
        // Pista: transport.send(from, { type: MSG.CHAT_ACK, messageId: msg.messageId });
        break;
      case MSG.CHAT_ACK:
        // TODO(ALUMNO): correlacionar el ACK con el messageId pendiente y
        // marcar el mensaje como entregado en la UI.
        log.debug(`ack ${msg.messageId} de ${shortId(from)}`);
        break;
      default:
        break;
    }
  }

  setupCli({ discovery, transport, peerId, sinks: cliSinks });

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
  peerId: string;
  sinks: { chat?: (from: string, text: string) => void };
}

function setupCli(ctx: CliCtx): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.setPrompt('chat> ');

  const out = (s: string): void => {
    process.stdout.write(s.endsWith('\n') ? s : s + '\n');
  };

  const printMenu = (): void => {
    out('');
    out('  ┌─ menú · p2p-chat ────────────────────────────────────┐');
    out('  │  chat <texto>          difunde mensaje a todos       │');
    out('  │  msg <peerId> <texto>  mensaje directo (ejercicio)   │');
    out('  │  peers                 peers descubiertos            │');
    out('  │  who                   peers conectados (handshake)  │');
    out('  │  menu | help | quit                                  │');
    out('  └──────────────────────────────────────────────────────┘');
  };

  ctx.sinks.chat = (from: string, text: string): void => {
    process.stdout.write('\r\x1b[K');
    process.stdout.write(`\x1b[35m[chat][${shortId(from)}]\x1b[0m ${text}\n`);
    rl.prompt(true);
  };

  printMenu();
  rl.prompt();

  const resolvePeer = (prefix: string): string | undefined => {
    const all = ctx.discovery.list().map((p) => p.peerId);
    const matches = all.filter((id) => id.startsWith(prefix));
    return matches.length === 1 ? matches[0] : undefined;
  };

  rl.on('line', (line) => {
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
        case 'who': {
          const ids = ctx.transport.connectedPeers();
          if (ids.length === 0) { out('(sin peers conectados)'); break; }
          for (const id of ids) out(`  ${shortId(id)}`);
          break;
        }
        case 'chat': {
          const text = rest.join(' ');
          if (!text) { out('uso: chat <texto>'); break; }
          const connected = ctx.transport.connectedPeers();
          if (connected.length === 0) { out('no hay peers conectados'); break; }
          ctx.transport.broadcast({
            type: MSG.CHAT,
            messageId: newMessageId(),
            text,
            ts: Date.now(),
          });
          out(`(chat) → ${connected.length} peer(s)`);
          break;
        }
        case 'msg': {
          // TODO(ALUMNO): mensajería directa 1-a-1.
          // - Validar rest[0] como prefijo de peerId conectado (resolvePeer).
          // - Construir Message CHAT con messageId nuevo, text, ts.
          // - transport.send(peerId, msg). Si false, avisar.
          // - Bonus: esperar CHAT_ACK con timeout y mostrar ✓/✗.
          out('(msg) ejercicio para alumnos — ver docs/EXERCISES.md');
          void resolvePeer;
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
          out('comandos: peers | who | chat <texto> | msg <peerId> <texto> | menu | quit');
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
