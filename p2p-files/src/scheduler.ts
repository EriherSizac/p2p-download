/**
 * SCHEDULER — lógica BitTorrent-like
 * ----------------------------------
 * Coordina las descargas activas y atiende los uploads. Para cada descarga
 * mantiene un store con bitfield local y, por cada peer conectado, un
 * peer-state con su bitfield remoto + requests en vuelo.
 *
 * Política de selección: **rarest-first** entre las piezas que aún nos
 * faltan. La idea es maximizar la "salud" del enjambre: si un peer único
 * tiene una pieza rara y se desconecta, esa pieza desaparece del enjambre.
 * Pidiendo primero las raras la replicamos cuanto antes.
 *
 * Paralelismo:
 *   - hasta 4 requests en vuelo por peer (evita saturar a un peer lento).
 *   - hasta 16 requests en vuelo por descarga (límite global).
 *   - hasta 8 PIECEs subiendo en paralelo por conexión entrante (lectura
 *     concurrente del disco; más allá no aporta y satura I/O).
 *
 * Timeouts: si un REQUEST no recibe PIECE en `PIECE_TIMEOUT_MS`, lo damos
 * por perdido, lo quitamos de in-flight y dejamos que el siguiente tick lo
 * reasigne (posiblemente a otro peer).
 *
 * 💡 Nota didáctica: BitTorrent real añade estrategias como tit-for-tat,
 * choking/unchoking y endgame mode. Aquí dejamos lo esencial para que se vea
 * con claridad cómo se reparte el trabajo entre múltiples peers.
 */

import { EventEmitter } from 'node:events';
import { createLogger } from './logger.js';
import { PieceStore } from './store.js';
import { PeerState } from './peer-state.js';
import { FileIndex } from './index-files.js';
import { saveManifest } from './manifest.js';
import {
  MSG,
  shortId,
  type FileManifest,
  type Message,
} from './protocol.js';
import { Transport } from './transport.js';

const log = createLogger('sched');

const PIECE_TIMEOUT_MS = 10_000;
const TICK_MS = 1_000;
const MAX_INFLIGHT_PER_PEER = 4;
const MAX_INFLIGHT_PER_DOWNLOAD = 16;
const MAX_CONCURRENT_UPLOADS_PER_CONN = 8;

interface ActiveDownload {
  manifest: FileManifest;
  store: PieceStore;
  totalInFlight: number;
  startedAt: number;
  bytesAtStart: number;
  bytesDownloaded: number;
  /** Resolver para la promesa devuelta a la CLI por `download()`. */
  done?: { resolve: (path: string) => void; reject: (e: Error) => void };
}

interface UploadQueueState {
  inFlight: number;
  pending: Array<{ fileId: string; pieceIndex: number }>;
}

export class Scheduler extends EventEmitter {
  private downloads = new Map<string, ActiveDownload>(); // fileId → ActiveDownload
  private peers = new Map<string, PeerState>(); // peerId → PeerState
  private uploads = new Map<string, UploadQueueState>(); // peerId → cola de subidas
  private tickTimer?: NodeJS.Timeout;

  constructor(
    private readonly transport: Transport,
    private readonly index: FileIndex,
    private readonly downloadDir: string,
    private readonly manifestsDir: string,
  ) {
    super();
  }

  start(): void {
    this.tickTimer = setInterval(() => this.tick(), TICK_MS);
  }

  stop(): void {
    if (this.tickTimer) clearInterval(this.tickTimer);
    for (const d of this.downloads.values()) {
      void d.store.close();
      d.done?.reject(new Error('scheduler stopped'));
    }
    this.downloads.clear();
  }

  // --- ciclo de vida de peers -------------------------------------------

  onPeerConnected(peerId: string): void {
    this.peers.set(peerId, new PeerState(peerId));
    this.uploads.set(peerId, { inFlight: 0, pending: [] });
    // Anunciar lo que tenemos: todos los archivos completos del index +
    // todas las descargas en curso (con su bitfield parcial).
    for (const f of this.index.list()) {
      const entry = this.index.getByFileId(f.fileId)!;
      // Bitfield "lleno" para un archivo completo del index.
      const numBytes = Math.ceil(f.numPieces / 8);
      const full = new Uint8Array(numBytes);
      // poner bits 0..numPieces-1
      for (let i = 0; i < f.numPieces; i++) full[i >> 3] = (full[i >> 3] ?? 0) | (1 << (7 - (i & 7)));
      this.transport.send(peerId, {
        type: MSG.HAVE,
        fileId: entry.manifest.fileId,
        bitfield: Buffer.from(full).toString('base64'),
      });
    }
    for (const d of this.downloads.values()) {
      this.transport.send(peerId, {
        type: MSG.HAVE,
        fileId: d.manifest.fileId,
        bitfield: d.store.bitfieldBase64(),
      });
    }
  }

  onPeerDisconnected(peerId: string): void {
    const ps = this.peers.get(peerId);
    if (ps) {
      // Liberar in-flight: el tick los reasignará.
      const orphans = ps.allInFlight();
      for (const r of orphans) {
        const d = this.downloads.get(r.fileId);
        if (d) d.totalInFlight = Math.max(0, d.totalInFlight - 1);
      }
    }
    this.peers.delete(peerId);
    this.uploads.delete(peerId);
  }

  // --- entrada de mensajes (delegada por main.ts) ------------------------

  handleMessage(from: string, msg: Message): void {
    switch (msg.type) {
      case MSG.HAVE: {
        const ps = this.peers.get(from);
        if (!ps) return;
        // Si ya estamos descargando este file, conocemos el numPieces; si no,
        // lo aprenderemos cuando alguien nos pase un manifiesto. Aún así, el
        // bitfield trae bytes; usamos su longitud para crearlo.
        const numBytes = Buffer.from(msg.bitfield, 'base64').length;
        const bitsAvailable = numBytes * 8;
        ps.setBitfield(msg.fileId, msg.bitfield, numBytes);
        const d = this.downloads.get(msg.fileId);
        if (d) {
          // Asegurar tamaño coherente con el manifest local.
          ps.setBitfield(msg.fileId, msg.bitfield, Math.ceil(d.manifest.numPieces / 8));
          this.kick(msg.fileId);
        }
        void bitsAvailable;
        break;
      }
      case MSG.REQUEST: {
        this.enqueueUpload(from, msg.fileId, msg.pieceIndex);
        break;
      }
      case MSG.PIECE: {
        const d = this.downloads.get(msg.fileId);
        if (!d) {
          log.debug(`PIECE de ${shortId(from)} para fileId desconocido, descartada`);
          return;
        }
        const ps = this.peers.get(from);
        if (ps) {
          if (ps.isInFlight(msg.fileId, msg.pieceIndex)) {
            ps.removeInFlight(msg.fileId, msg.pieceIndex);
            d.totalInFlight = Math.max(0, d.totalInFlight - 1);
          }
        }
        void this.handleIncomingPiece(from, d, msg.pieceIndex, msg.data);
        break;
      }
      default:
        // El scheduler solo maneja HAVE/REQUEST/PIECE; el resto es de main.
        break;
    }
  }

  // --- API pública --------------------------------------------------------

  /**
   * Inicia una descarga. Devuelve una promesa que se resuelve con la ruta
   * final del archivo descargado.
   */
  async startDownload(manifest: FileManifest): Promise<string> {
    let active = this.downloads.get(manifest.fileId);
    if (active) {
      log.warn(`ya hay una descarga activa para ${manifest.name}`);
      return new Promise((resolve, reject) => {
        if (!active!.done) active!.done = { resolve, reject };
        else {
          // Si ya hay un waiter, encadenamos.
          const prev = active!.done;
          active!.done = {
            resolve: (p) => { prev.resolve(p); resolve(p); },
            reject: (e) => { prev.reject(e); reject(e); },
          };
        }
      });
    }

    // Si ya está en el index local (lo tenemos completo), short-circuit.
    const owned = this.index.getByFileId(manifest.fileId);
    if (owned) return owned.fullPath;

    const store = new PieceStore(this.downloadDir, manifest);
    await store.open();
    await saveManifest(this.manifestsDir, manifest);

    const bytesAtStart = store.numHave() * manifest.pieceSize;
    active = {
      manifest,
      store,
      totalInFlight: 0,
      startedAt: Date.now(),
      bytesAtStart,
      bytesDownloaded: bytesAtStart,
    };
    this.downloads.set(manifest.fileId, active);

    // Anunciar a todos lo que ya tenemos (puede ser bitfield vacío en una
    // descarga limpia, o parcial si reanudamos).
    this.transport.broadcast({
      type: MSG.HAVE,
      fileId: manifest.fileId,
      bitfield: store.bitfieldBase64(),
    });

    const result = new Promise<string>((resolve, reject) => {
      active!.done = { resolve, reject };
    });

    if (store.isComplete()) {
      // Reanudación de algo que ya estaba completo.
      void this.completeDownload(active);
    } else {
      this.kick(manifest.fileId);
    }
    return result;
  }

  status(): Array<{ name: string; have: number; total: number; rateKBs: number }> {
    const out: Array<{ name: string; have: number; total: number; rateKBs: number }> = [];
    for (const d of this.downloads.values()) {
      const elapsedSec = Math.max(1, (Date.now() - d.startedAt) / 1000);
      const downloadedSinceStart = Math.max(0, d.bytesDownloaded - d.bytesAtStart);
      out.push({
        name: d.manifest.name,
        have: d.store.numHave(),
        total: d.manifest.numPieces,
        rateKBs: Math.round(downloadedSinceStart / 1024 / elapsedSec),
      });
    }
    return out;
  }

  // --- núcleo: selección y envío de requests -----------------------------

  private kick(fileId: string): void {
    const d = this.downloads.get(fileId);
    if (!d) return;
    if (d.store.isComplete()) {
      void this.completeDownload(d);
      return;
    }

    // Lista de peers candidatos (con bitfield para este fileId).
    const candidatePeers = [...this.peers.values()].filter((p) => p.getBitfield(fileId));

    // Calcular rareza por pieza: cuántos peers la tienen.
    const numPieces = d.manifest.numPieces;
    const rarity = new Array<number>(numPieces).fill(0);
    for (const ps of candidatePeers) {
      for (let i = 0; i < numPieces; i++) {
        if (ps.hasPiece(fileId, i)) rarity[i] = (rarity[i] ?? 0) + 1;
      }
    }

    // Piezas que nos faltan, ordenadas por rareza ascendente (rarest-first),
    // tie-break aleatorio para distribuir carga entre peers.
    const wanted: number[] = [];
    for (let i = 0; i < numPieces; i++) {
      if (!d.store.hasPiece(i) && (rarity[i] ?? 0) > 0) wanted.push(i);
    }
    wanted.sort((a, b) => {
      const ra = rarity[a] ?? 0;
      const rb = rarity[b] ?? 0;
      if (ra !== rb) return ra - rb;
      return Math.random() - 0.5;
    });

    for (const pieceIndex of wanted) {
      if (d.totalInFlight >= MAX_INFLIGHT_PER_DOWNLOAD) break;
      // Saltar si ya está pedida a algún peer (evitar duplicar mientras
      // no estemos en endgame mode — que no implementamos).
      const alreadyPending = candidatePeers.some((p) => p.isInFlight(fileId, pieceIndex));
      if (alreadyPending) continue;

      // Elegir un peer que tenga la pieza y aún tenga slot disponible.
      const choices = candidatePeers.filter(
        (p) => p.hasPiece(fileId, pieceIndex) && p.inFlightCount() < MAX_INFLIGHT_PER_PEER,
      );
      if (choices.length === 0) continue;
      // Tie-break por menor in-flight para repartir, luego aleatorio.
      choices.sort((a, b) => a.inFlightCount() - b.inFlightCount() || Math.random() - 0.5);
      const chosen = choices[0]!;

      chosen.addInFlight(fileId, pieceIndex);
      d.totalInFlight += 1;
      const sent = this.transport.send(chosen.peerId, { type: MSG.REQUEST, fileId, pieceIndex });
      if (!sent) {
        // El send falló: liberar inmediatamente.
        chosen.removeInFlight(fileId, pieceIndex);
        d.totalInFlight = Math.max(0, d.totalInFlight - 1);
      } else {
        log.debug(`REQUEST ${shortId(fileId)}#${pieceIndex} → ${shortId(chosen.peerId)}`);
      }
    }
  }

  private async handleIncomingPiece(
    from: string,
    d: ActiveDownload,
    pieceIndex: number,
    data: Buffer,
  ): Promise<void> {
    if (d.store.hasPiece(pieceIndex)) return;
    const ok = await d.store.writePiece(pieceIndex, data).catch((err) => {
      log.error('writePiece falló', err);
      return false;
    });
    if (!ok) {
      const ps = this.peers.get(from);
      if (ps) ps.failures += 1;
      log.warn(`pieza ${pieceIndex} de ${shortId(from)} inválida (hash o tamaño)`);
      this.kick(d.manifest.fileId);
      return;
    }
    d.bytesDownloaded += data.length;
    log.debug(`PIECE ok ${shortId(d.manifest.fileId)}#${pieceIndex} (${d.store.numHave()}/${d.manifest.numPieces})`);

    // Anunciar HAVE actualizado a todos los peers conectados.
    this.transport.broadcast({
      type: MSG.HAVE,
      fileId: d.manifest.fileId,
      bitfield: d.store.bitfieldBase64(),
    });

    if (d.store.isComplete()) {
      await this.completeDownload(d);
    } else {
      this.kick(d.manifest.fileId);
    }
  }

  private async completeDownload(d: ActiveDownload): Promise<void> {
    try {
      const finalPath = await d.store.finalize();
      this.index.add(d.manifest, finalPath);
      this.downloads.delete(d.manifest.fileId);
      log.info(`descarga completa: ${d.manifest.name} → ${finalPath}`);
      d.done?.resolve(finalPath);
    } catch (err) {
      log.error('finalize falló', err);
      d.done?.reject(err as Error);
      this.downloads.delete(d.manifest.fileId);
    }
  }

  // --- subida (servir REQUESTs) ------------------------------------------

  private enqueueUpload(peerId: string, fileId: string, pieceIndex: number): void {
    let q = this.uploads.get(peerId);
    if (!q) {
      q = { inFlight: 0, pending: [] };
      this.uploads.set(peerId, q);
    }
    q.pending.push({ fileId, pieceIndex });
    this.pumpUploads(peerId);
  }

  private pumpUploads(peerId: string): void {
    const q = this.uploads.get(peerId);
    if (!q) return;
    while (q.inFlight < MAX_CONCURRENT_UPLOADS_PER_CONN && q.pending.length > 0) {
      const item = q.pending.shift()!;
      q.inFlight += 1;
      void this.serveOne(peerId, item.fileId, item.pieceIndex)
        .catch((err) => log.warn(`upload error: ${err.message}`))
        .finally(() => {
          const cur = this.uploads.get(peerId);
          if (cur) {
            cur.inFlight = Math.max(0, cur.inFlight - 1);
            this.pumpUploads(peerId);
          }
        });
    }
  }

  private async serveOne(peerId: string, fileId: string, pieceIndex: number): Promise<void> {
    // Buscar la fuente: index local (archivo completo) o store activo (en
    // curso) — un peer puede servir piezas mientras descarga.
    let data: Buffer | null = null;
    const owned = this.index.getByFileId(fileId);
    if (owned) {
      const fs = await import('node:fs/promises');
      const fh = await fs.open(owned.fullPath, 'r');
      try {
        const m = owned.manifest;
        const len = pieceIndex < m.numPieces - 1
          ? m.pieceSize
          : m.size - (m.numPieces - 1) * m.pieceSize;
        const buf = Buffer.alloc(len);
        await fh.read(buf, 0, len, pieceIndex * m.pieceSize);
        data = buf;
      } finally {
        await fh.close();
      }
    } else {
      const d = this.downloads.get(fileId);
      if (d && d.store.hasPiece(pieceIndex)) {
        data = await d.store.readPiece(pieceIndex);
      }
    }

    if (!data) {
      this.transport.send(peerId, {
        type: MSG.ERROR,
        code: 'PIECE_NOT_AVAILABLE',
        message: `${shortId(fileId)}#${pieceIndex}`,
      });
      return;
    }
    this.transport.send(peerId, { type: MSG.PIECE, fileId, pieceIndex, data });
  }

  // --- mantenimiento ------------------------------------------------------

  private tick(): void {
    // Limpiar timeouts en cada peer y re-disparar scheduling.
    for (const ps of this.peers.values()) {
      const dead = ps.reapTimeouts(PIECE_TIMEOUT_MS);
      for (const r of dead) {
        const d = this.downloads.get(r.fileId);
        if (d) d.totalInFlight = Math.max(0, d.totalInFlight - 1);
        ps.failures += 1;
        log.debug(`timeout ${shortId(r.fileId)}#${r.pieceIndex} en ${shortId(ps.peerId)}`);
      }
    }
    for (const fileId of this.downloads.keys()) this.kick(fileId);
  }
}
