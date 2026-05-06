/**
 * CAPA 1 — DESCUBRIMIENTO
 * -----------------------
 * Problema: en una red P2P no hay servidor central que sepa quién está conectado.
 * Cada peer necesita encontrar a los demás por sí mismo.
 *
 * Solución (en LAN): broadcast UDP. Cada peer anuncia su existencia periódicamente
 * a la dirección de broadcast (255.255.255.255) en un puerto fijo. Todos los peers
 * escuchan ese puerto y mantienen una tabla de peers vivos con timeout.
 *
 * Limitación: el broadcast no sale del router. Para WAN haría falta otro mecanismo
 * (DHT, tracker, mDNS sobre VPN, etc.) — ver discusión en docs/NAT.md.
 *
 * 💡 Nota didáctica: usamos un único puerto UDP fijo donde todos hablan/escuchan.
 * Reusable y simple. El protocolo de anuncio es JSON con {peerId, tcpHost, tcpPort}.
 * Para evitar escuchar nuestros propios paquetes, ignoramos los que vengan con
 * nuestro peerId.
 */

import dgram from 'node:dgram';
import { EventEmitter } from 'node:events';
import { createLogger } from './logger.js';

const log = createLogger('discovery');

export interface DiscoveredPeer {
  peerId: string;
  host: string;
  port: number;
  lastSeen: number;
}

interface AnnounceMsg {
  peerId: string;
  tcpHost: string; // generalmente "0.0.0.0" → el receptor usa rinfo.address
  tcpPort: number;
  v: number; // versión del protocolo de descubrimiento
}

const ANNOUNCE_INTERVAL_MS = 3_000;
const PEER_TIMEOUT_MS = 10_000;
const ANNOUNCE_VERSION = 1;

export interface DiscoveryOpts {
  peerId: string;
  tcpPort: number;
  discoveryPort: number;
}

export declare interface Discovery {
  on(event: 'peer-up', listener: (p: DiscoveredPeer) => void): this;
  on(event: 'peer-down', listener: (p: DiscoveredPeer) => void): this;
}

export class Discovery extends EventEmitter {
  private socket: dgram.Socket;
  private peers = new Map<string, DiscoveredPeer>();
  private announceTimer?: NodeJS.Timeout;
  private gcTimer?: NodeJS.Timeout;

  constructor(private readonly opts: DiscoveryOpts) {
    super();
    // `reuseAddr` permite que múltiples procesos en la misma máquina escuchen
    // el mismo puerto — útil para correr varios peers en localhost en clase.
    this.socket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
  }

  async start(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.socket.once('error', reject);
      this.socket.bind(this.opts.discoveryPort, () => {
        this.socket.setBroadcast(true);
        resolve();
      });
    });

    this.socket.on('message', (buf, rinfo) => this.onMessage(buf, rinfo));
    this.socket.on('error', (err) => log.error('udp error', err));

    this.announceTimer = setInterval(() => this.announce(), ANNOUNCE_INTERVAL_MS);
    this.gcTimer = setInterval(() => this.gc(), 2_000);
    this.announce();
    log.info(`escuchando UDP en :${this.opts.discoveryPort}`);
  }

  stop(): void {
    if (this.announceTimer) clearInterval(this.announceTimer);
    if (this.gcTimer) clearInterval(this.gcTimer);
    try {
      this.socket.close();
    } catch {
      /* ya cerrado */
    }
  }

  list(): DiscoveredPeer[] {
    return [...this.peers.values()].sort((a, b) => a.peerId.localeCompare(b.peerId));
  }

  get(peerId: string): DiscoveredPeer | undefined {
    return this.peers.get(peerId);
  }

  private announce(): void {
    const msg: AnnounceMsg = {
      peerId: this.opts.peerId,
      tcpHost: '0.0.0.0', // el receptor usa la IP de origen UDP, no este campo.
      tcpPort: this.opts.tcpPort,
      v: ANNOUNCE_VERSION,
    };
    const buf = Buffer.from(JSON.stringify(msg));
    this.socket.send(buf, 0, buf.length, this.opts.discoveryPort, '255.255.255.255', (err) => {
      if (err) log.warn('broadcast error', err.message);
    });
  }

  private onMessage(buf: Buffer, rinfo: dgram.RemoteInfo): void {
    let parsed: AnnounceMsg;
    try {
      parsed = JSON.parse(buf.toString('utf8')) as AnnounceMsg;
    } catch {
      return; // basura — ignorar.
    }
    if (parsed.v !== ANNOUNCE_VERSION) return;
    if (parsed.peerId === this.opts.peerId) return; // ignorar nuestros propios anuncios.

    const existing = this.peers.get(parsed.peerId);
    const peer: DiscoveredPeer = {
      peerId: parsed.peerId,
      host: rinfo.address,
      port: parsed.tcpPort,
      lastSeen: Date.now(),
    };
    this.peers.set(parsed.peerId, peer);
    if (!existing) {
      log.info(`peer-up ${parsed.peerId.slice(0, 8)} @ ${peer.host}:${peer.port}`);
      this.emit('peer-up', peer);
    }
  }

  private gc(): void {
    const cutoff = Date.now() - PEER_TIMEOUT_MS;
    for (const [id, p] of this.peers) {
      if (p.lastSeen < cutoff) {
        this.peers.delete(id);
        log.info(`peer-down ${id.slice(0, 8)}`);
        this.emit('peer-down', p);
      }
    }
  }
}
