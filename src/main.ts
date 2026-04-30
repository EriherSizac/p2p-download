/**
 * MAIN — orquestación de capas + CLI
 * ----------------------------------
 * Junta las capas (descubrimiento → transporte → protocolo) y expone una CLI
 * sobre `readline`. Aquí no hay lógica de aplicación: solo cableado entre
 * componentes y traducción de comandos del usuario a llamadas a las capas.
 *
 * 💡 Nota didáctica: separar la composición (este archivo) del comportamiento
 * (los módulos de capa) facilita los tests unitarios y deja una "vista de
 * pájaro" muy clara del sistema.
 */

import readline from 'node:readline';
import { Discovery } from './discovery.js';
import { Transport } from './transport.js';
import { generatePeerId, shortId, MSG, type Message } from './protocol.js';
import { createLogger } from './logger.js';

const log = createLogger('main');

const TCP_PORT = Number(process.env['TCP_PORT'] ?? 0);
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41234);

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  log.info(`peerId = ${shortId(peerId)} (full=${peerId})`);

  const transport = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  // Cuando aparece un nuevo peer en LAN, intentamos conectar.
  // 💡 Tie-breaking: para evitar dobles conexiones cuando ambos peers se
  // descubren simultáneamente, solo inicia la conexión el peer cuyo id es
  // lexicográficamente menor. El otro lado simplemente acepta la entrante.
  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId) {
      transport.connect(p.peerId, p.host, p.port);
    }
  });

  transport.on('peer-connected', (id) => {
    log.debug(`saludo ok con ${shortId(id)}`);
  });

  transport.on('message', (from, msg: Message) => {
    onMessage(from, msg);
  });

  setupCli(discovery, transport);

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

function onMessage(from: string, msg: Message): void {
  switch (msg.type) {
    case MSG.BYE:
      log.debug(`bye de ${shortId(from)}`);
      break;
    case MSG.CHAT:
      // TODO(ALUMNO): imprimir el chat sin romper el prompt de readline.
      // Pista: process.stdout.write('\r…\n') + rl.prompt(true).
      log.info(`[chat][${shortId(from)}] ${msg.text}`);
      break;
    default:
      // En etapas posteriores aquí se enchufan: LIST/LIST_REPLY, MANIFEST/…,
      // HAVE/REQUEST/PIECE. Por ahora, en Etapa 1, solo log.
      log.debug(`msg tipo 0x${msg.type.toString(16)} de ${shortId(from)}`);
  }
}

function setupCli(discovery: Discovery, transport: Transport): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const prompt = (): void => rl.setPrompt('p2p> ');
  prompt();
  rl.prompt();

  rl.on('line', (line) => {
    const [cmd, ...rest] = line.trim().split(/\s+/);
    switch (cmd) {
      case '':
        break;
      case 'peers': {
        const list = discovery.list();
        if (list.length === 0) {
          process.stdout.write('(no hay peers descubiertos)\n');
        } else {
          for (const p of list) {
            const conn = transport.isConnected(p.peerId) ? 'conectado' : 'descubierto';
            process.stdout.write(
              `  ${shortId(p.peerId)}  ${p.host}:${p.port}  ${conn}\n`,
            );
          }
        }
        break;
      }
      case 'msg': {
        // TODO(ALUMNO): implementar mensajería directa.
        // - Validar que rest[0] es un peerId (o prefijo) conectado.
        // - Construir el Message de tipo CHAT con {text: rest.slice(1).join(' '), ts: Date.now()}.
        // - Llamar a transport.send(peerId, msg). Si devuelve false, avisar.
        process.stdout.write('(msg) ejercicio para alumnos — ver docs/EXERCISES.md\n');
        void rest;
        break;
      }
      case 'list':
      case 'get':
      case 'status':
        process.stdout.write(`(${cmd}) disponible en etapas siguientes\n`);
        break;
      case 'quit':
      case 'exit':
        rl.close();
        process.kill(process.pid, 'SIGINT');
        return;
      case 'help':
        process.stdout.write(
          'comandos: peers | list <peerId> | get <peerId> <name> | status | msg <peerId> <texto> | quit\n',
        );
        break;
      default:
        process.stdout.write(`comando desconocido: ${cmd}\n`);
    }
    rl.prompt();
  });

  rl.on('close', () => {
    /* el SIGINT handler se ocupa del shutdown */
  });
}

bootstrap().catch((err) => {
  log.error('bootstrap falló', err);
  process.exit(1);
});
