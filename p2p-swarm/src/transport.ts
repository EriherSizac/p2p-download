/**
 * CAPA 2 — TRANSPORTE
 * -------------------
 * Problema: una vez que sabemos que existe un peer (capa 1), necesitamos un
 * canal fiable y orientado a conexión para hablar con él. Para transferir
 * archivos queremos retransmisiones, control de congestión y orden — todo
 * eso lo da TCP.
 *
 * Solución: cada peer mantiene un servidor TCP escuchando en `tcpPort`. Cuando
 * descubrimos otro peer, abrimos una conexión saliente. El primer mensaje en
 * cualquier sentido es HELLO — sirve para identificar al peer remoto al socket
 * que llegó (entrante o saliente) y registrarlo en el pool.
 *
 * Pool: indexamos conexiones por peerId para evitar duplicados. Si llega una
 * segunda conexión al mismo peerId, ganan ambos? No: nos quedamos con la más
 * antigua y cerramos la nueva. Sencillo y predecible.
 *
 * 💡 Nota didáctica: en BitTorrent real hay handshake binario más estricto y
 * resolución de "ties" por comparación de peerIds. Aquí buscamos claridad.
 */

import net from 'node:net';
import { EventEmitter } from 'node:events';
import { createLogger } from './logger.js';
import { FrameParser, frame } from './framing.js';
import { decode, encode, MSG, PROTOCOL_VERSION, shortId, type Message } from './protocol.js';

const log = createLogger('transport');

export interface TransportOpts {
  peerId: string;
  tcpPort: number;
}

interface Conn {
  peerId: string;
  socket: net.Socket;
  remoteAddress: string;
  remotePort: number;
  outgoing: boolean;
}

export interface UnreachableInfo {
  peerId: string;
  host: string;
  port: number;
  code: string;
  message: string;
}

export declare interface Transport {
  on(event: 'message', listener: (peerId: string, msg: Message) => void): this;
  on(event: 'peer-connected', listener: (peerId: string) => void): this;
  on(event: 'peer-disconnected', listener: (peerId: string) => void): this;
  on(event: 'peer-unreachable', listener: (info: UnreachableInfo) => void): this;
}

export class Transport extends EventEmitter {
  private server!: net.Server;
  private conns = new Map<string, Conn>();
  /** Conexiones aún sin HELLO recibido. Indexadas por un id transitorio. */
  private pending = new Map<number, { socket: net.Socket; outgoing: boolean }>();
  private nextPendingId = 1;
  private actualPort = 0;

  constructor(private readonly opts: TransportOpts) {
    super();
  }

  async start(): Promise<number> {
    this.server = net.createServer((socket) => this.acceptIncoming(socket));
    await new Promise<void>((resolve, reject) => {
      // Si el puerto fijo está ocupado (otra instancia, otro proceso), damos
      // un mensaje claro. NO caemos a puerto efímero porque eso rompería la
      // regla de firewall ya creada para el puerto fijo.
      this.server.once('error', (err: NodeJS.ErrnoException) => {
        if (err.code === 'EADDRINUSE') {
          reject(new Error(
            `puerto TCP ${this.opts.tcpPort} ocupado. ` +
            `Cierra la otra instancia o lanza con \`TCP_PORT=<otro> npm run dev\`. ` +
            `Si usas otro puerto, recuerda actualizar la regla de firewall.`,
          ));
        } else reject(err);
      });
      this.server.listen(this.opts.tcpPort, '0.0.0.0', () => {
        const addr = this.server.address();
        if (addr && typeof addr === 'object') this.actualPort = addr.port;
        resolve();
      });
    });
    log.info(`servidor TCP escuchando en :${this.actualPort}`);
    return this.actualPort;
  }

  stop(): void {
    for (const c of this.conns.values()) {
      try {
        c.socket.destroy();
      } catch {
        /* ignore */
      }
    }
    this.conns.clear();
    try {
      this.server.close();
    } catch {
      /* ignore */
    }
  }

  /** Conecta a un peer descubierto si no tenemos ya una conexión activa. */
  connect(peerId: string, host: string, port: number): void {
    if (this.conns.has(peerId)) return;
    if (peerId === this.opts.peerId) return;
    log.debug(`conectando a ${shortId(peerId)} @ ${host}:${port}`);
    const socket = net.connect({ host, port });
    socket.once('error', (err) => {
      const code = (err as NodeJS.ErrnoException).code ?? '';
      log.warn(`conexión a ${shortId(peerId)} falló: ${err.message}`);
      // ETIMEDOUT/ECONNREFUSED/EHOSTUNREACH → puerto inalcanzable. Casi
      // siempre firewall (Windows bloquea inbound TCP por defecto) o el
      // peer no está escuchando. Emitimos evento para que main pregunte.
      if (code === 'ETIMEDOUT' || code === 'ECONNREFUSED' || code === 'EHOSTUNREACH') {
        this.emit('peer-unreachable', {
          peerId,
          host,
          port,
          code,
          message: err.message,
        });
      }
    });
    this.attach(socket, true, peerId);
  }

  send(peerId: string, msg: Message): boolean {
    const c = this.conns.get(peerId);
    if (!c) {
      log.debug(`send: sin conexión a ${shortId(peerId)}`);
      return false;
    }
    try {
      c.socket.write(frame(encode(msg)));
      return true;
    } catch (err) {
      log.warn(`send error a ${shortId(peerId)}: ${(err as Error).message}`);
      return false;
    }
  }

  broadcast(msg: Message): void {
    for (const id of this.conns.keys()) this.send(id, msg);
  }

  connectedPeers(): string[] {
    return [...this.conns.keys()];
  }

  isConnected(peerId: string): boolean {
    return this.conns.has(peerId);
  }

  /** Puerto TCP real en el que escuchamos (tras `start()`). */
  port(): number {
    return this.actualPort;
  }

  // --- internas ----------------------------------------------------------

  private acceptIncoming(socket: net.Socket): void {
    log.debug(`conexión entrante de ${socket.remoteAddress}:${socket.remotePort}`);
    this.attach(socket, false, undefined);
  }

  private attach(socket: net.Socket, outgoing: boolean, expectedPeerId: string | undefined): void {
    const pid = this.nextPendingId++;
    this.pending.set(pid, { socket, outgoing });

    const parser = new FrameParser((raw) => {
      let msg: Message;
      try {
        msg = decode(raw);
      } catch (err) {
        log.warn(`decode falló: ${(err as Error).message}`);
        socket.destroy();
        return;
      }

      // Hasta que no haya HELLO, el único mensaje aceptado es HELLO.
      if (this.pending.has(pid)) {
        if (msg.type !== MSG.HELLO) {
          log.warn('mensaje pre-HELLO descartado');
          socket.destroy();
          return;
        }
        if (expectedPeerId && msg.peerId !== expectedPeerId) {
          log.warn(
            `peerId inesperado: esperaba ${shortId(expectedPeerId)}, recibí ${shortId(msg.peerId)}`,
          );
          socket.destroy();
          return;
        }
        if (msg.peerId === this.opts.peerId) {
          // Auto-conexión (puede pasar en localhost): cerrar.
          socket.destroy();
          this.pending.delete(pid);
          return;
        }
        if (this.conns.has(msg.peerId)) {
          // Duplicado: ya teníamos conexión a este peer. Cerrar la nueva.
          log.debug(`conexión duplicada con ${shortId(msg.peerId)}, cerrando`);
          socket.destroy();
          this.pending.delete(pid);
          return;
        }
        // Promover de pending a conexión activa.
        this.pending.delete(pid);
        const conn: Conn = {
          peerId: msg.peerId,
          socket,
          remoteAddress: socket.remoteAddress ?? '?',
          remotePort: socket.remotePort ?? 0,
          outgoing,
        };
        this.conns.set(msg.peerId, conn);
        // Si nosotros iniciamos la conexión, el peer remoto aún no nos ha
        // visto: respondemos con nuestro propio HELLO si no lo enviamos antes.
        // (Lo enviamos ya en `socket.on('connect')` para outgoing — ver abajo.)
        log.info(`peer-connected ${shortId(msg.peerId)} (${outgoing ? 'out' : 'in'})`);
        this.emit('peer-connected', msg.peerId);
        return;
      }

      // Conexión ya promovida: emitir el mensaje hacia arriba.
      const peerId = this.findPeerIdBySocket(socket);
      if (!peerId) {
        log.warn('mensaje en socket no asociado, descartando');
        return;
      }
      this.emit('message', peerId, msg);
    });

    socket.on('data', (chunk) => {
      try {
        parser.push(chunk);
      } catch (err) {
        log.warn(`framing error: ${(err as Error).message}`);
        socket.destroy();
      }
    });

    socket.on('close', () => this.cleanupSocket(socket, pid));
    socket.on('error', (err) => log.debug(`socket error: ${err.message}`));

    const sendHello = (): void => {
      try {
        socket.write(
          frame(
            encode({ type: MSG.HELLO, peerId: this.opts.peerId, version: PROTOCOL_VERSION }),
          ),
        );
      } catch {
        /* el close lo recogerá */
      }
    };

    if (outgoing) {
      // Saliente: enviamos HELLO al conectar.
      socket.once('connect', sendHello);
    } else {
      // Entrante: enviamos HELLO inmediatamente — el remoto aún no sabe
      // quiénes somos. El cruce de HELLOs no causa problema: el orden no
      // importa, ambos extremos solo esperan UN HELLO antes de promover.
      sendHello();
    }
  }

  private cleanupSocket(socket: net.Socket, pendingId: number): void {
    if (this.pending.has(pendingId)) {
      this.pending.delete(pendingId);
      return;
    }
    const peerId = this.findPeerIdBySocket(socket);
    if (peerId) {
      this.conns.delete(peerId);
      log.info(`peer-disconnected ${shortId(peerId)}`);
      this.emit('peer-disconnected', peerId);
    }
  }

  private findPeerIdBySocket(socket: net.Socket): string | undefined {
    for (const [id, c] of this.conns) if (c.socket === socket) return id;
    return undefined;
  }
}
