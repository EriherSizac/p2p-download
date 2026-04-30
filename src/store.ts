/**
 * STORE — persistencia de piezas
 * ------------------------------
 * Cada descarga abre un archivo `.part` del tamaño total preasignado y
 * un sidecar `<fileId>.bitfield` con un bit por pieza completada.
 *
 * - writePiece(idx, buf): escribe en el offset correcto y pone el bit.
 * - readPiece(idx): lee la pieza (para servirla a otros peers o verificar).
 * - hasPiece(idx) / bitfield(): consulta del estado.
 * - finalize(): valida hash global, renombra .part → nombre real, devuelve path.
 *
 * Reanudación:
 *   - Al abrir un store con el manifiesto, si ya existe `.part` y `.bitfield`,
 *     confiamos en el bitfield. Para extra robustez podríamos re-verificar
 *     hash de cada pieza, pero es caro en archivos grandes — hacemos
 *     verificación opcional con `verifyOnLoad`.
 *
 * 💡 Nota didáctica: separar los datos (.part) del estado (.bitfield) hace
 * la reanudación trivial. BitTorrent real combina ambos en un solo archivo
 * + un .resume aparte; aquí preferimos la claridad.
 */

import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import type { FileManifest } from './protocol.js';
import {
  fromBase64,
  getBit,
  newBitfield,
  popcount,
  setBit,
  toBase64,
} from './bitfield.js';

export class PieceStore {
  private fh: fs.promises.FileHandle | null = null;
  private bf: Uint8Array;
  private partPath: string;
  private bitfieldPath: string;
  private finalPath: string;
  private complete = false;

  constructor(
    private readonly downloadDir: string,
    public readonly manifest: FileManifest,
  ) {
    this.bf = newBitfield(manifest.numPieces);
    this.partPath = path.join(downloadDir, `${manifest.fileId}.part`);
    this.bitfieldPath = path.join(downloadDir, `${manifest.fileId}.bitfield`);
    this.finalPath = path.join(downloadDir, manifest.name);
  }

  async open(): Promise<void> {
    await fs.promises.mkdir(this.downloadDir, { recursive: true });

    // Cargar bitfield existente si lo hay.
    try {
      const raw = await fs.promises.readFile(this.bitfieldPath);
      this.bf = fromBase64(raw.toString('utf8'), this.bf.length);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    }

    // Abrir/crear el .part del tamaño exacto.
    this.fh = await fs.promises.open(this.partPath, 'a+');
    const stat = await this.fh.stat();
    if (stat.size < this.manifest.size) {
      await this.fh.truncate(this.manifest.size);
    }

    if (this.isComplete()) {
      this.complete = true;
    }
  }

  async close(): Promise<void> {
    if (this.fh) {
      await this.fh.close();
      this.fh = null;
    }
  }

  pieceLength(idx: number): number {
    if (idx < this.manifest.numPieces - 1) return this.manifest.pieceSize;
    const remainder = this.manifest.size % this.manifest.pieceSize;
    return remainder === 0 ? this.manifest.pieceSize : remainder;
  }

  hasPiece(idx: number): boolean {
    return getBit(this.bf, idx);
  }

  bitfield(): Uint8Array {
    return this.bf;
  }

  bitfieldBase64(): string {
    return toBase64(this.bf);
  }

  numHave(): number {
    return popcount(this.bf);
  }

  isComplete(): boolean {
    return popcount(this.bf) === this.manifest.numPieces;
  }

  async readPiece(idx: number): Promise<Buffer> {
    if (!this.fh) throw new Error('store cerrado');
    if (!this.hasPiece(idx)) throw new Error(`pieza ${idx} no disponible`);
    const len = this.pieceLength(idx);
    const buf = Buffer.alloc(len);
    await this.fh.read(buf, 0, len, idx * this.manifest.pieceSize);
    return buf;
  }

  /**
   * Escribe una pieza, verificando su hash. Si el hash no coincide, no
   * persiste nada y devuelve false (el caller decide reintentar / penalizar).
   */
  async writePiece(idx: number, data: Buffer): Promise<boolean> {
    if (!this.fh) throw new Error('store cerrado');
    if (this.hasPiece(idx)) return true;
    if (data.length !== this.pieceLength(idx)) return false;
    const expected = this.manifest.pieceHashes[idx];
    if (!expected) return false;
    const got = createHash('sha256').update(data).digest('hex');
    if (got !== expected) return false;
    await this.fh.write(data, 0, data.length, idx * this.manifest.pieceSize);
    setBit(this.bf, idx);
    await this.persistBitfield();
    return true;
  }

  /**
   * Verifica todas las piezas leyendo del .part y reconstruye el bitfield.
   * Útil tras un crash en el que el bitfield no se haya actualizado.
   */
  async verifyOnLoad(): Promise<void> {
    if (!this.fh) throw new Error('store cerrado');
    const newBf = newBitfield(this.manifest.numPieces);
    for (let i = 0; i < this.manifest.numPieces; i++) {
      const len = this.pieceLength(i);
      const buf = Buffer.alloc(len);
      await this.fh.read(buf, 0, len, i * this.manifest.pieceSize);
      const h = createHash('sha256').update(buf).digest('hex');
      if (h === this.manifest.pieceHashes[i]) setBit(newBf, i);
    }
    this.bf = newBf;
    await this.persistBitfield();
  }

  /**
   * Renombra .part → nombre real y limpia sidecar. Verifica hash global.
   */
  async finalize(): Promise<string> {
    if (!this.isComplete()) throw new Error('no se puede finalizar: descarga incompleta');
    if (!this.fh) throw new Error('store cerrado');

    // Verificación final: hash global del archivo.
    const hasher = createHash('sha256');
    const stream = fs.createReadStream(this.partPath, { highWaterMark: 64 * 1024 });
    await new Promise<void>((resolve, reject) => {
      stream.on('data', (c) => hasher.update(c));
      stream.on('end', resolve);
      stream.on('error', reject);
    });
    const got = hasher.digest('hex');
    if (got !== this.manifest.fileHash) {
      throw new Error(
        `hash global no coincide: esperado ${this.manifest.fileHash.slice(0, 8)}, obtenido ${got.slice(0, 8)}`,
      );
    }

    await this.fh.close();
    this.fh = null;
    // Si el destino ya existe (re-descarga), sobrescribimos.
    await fs.promises.rename(this.partPath, this.finalPath).catch(async (err) => {
      if ((err as NodeJS.ErrnoException).code === 'EEXIST' || (err as NodeJS.ErrnoException).code === 'EPERM') {
        await fs.promises.unlink(this.finalPath).catch(() => {});
        await fs.promises.rename(this.partPath, this.finalPath);
      } else {
        throw err;
      }
    });
    await fs.promises.unlink(this.bitfieldPath).catch(() => {});
    this.complete = true;
    return this.finalPath;
  }

  private async persistBitfield(): Promise<void> {
    await fs.promises.writeFile(this.bitfieldPath, this.bitfieldBase64());
  }
}
