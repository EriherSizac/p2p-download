/**
 * MANIFIESTOS DE ARCHIVO
 * ----------------------
 * Un manifiesto describe un archivo en términos del protocolo P2P:
 *   - identidad (fileId = SHA-256 del contenido completo).
 *   - tamaño total y tamaño de pieza.
 *   - una lista de hashes SHA-256, uno por pieza.
 *
 * ¿Por qué hash por pieza si ya tenemos hash total?
 *   - El hash total solo se puede verificar al final. Sin hash por pieza, un
 *     peer malicioso podría servirnos basura durante 99% de la descarga y solo
 *     descubrirlo al cerrar el archivo. Verificando pieza a pieza descartamos
 *     y re-pedimos solo el bloque corrupto.
 *
 * 💡 Nota didáctica: BitTorrent hace exactamente esto en su .torrent
 * (campo `pieces` con SHA-1 concatenados). Aquí usamos SHA-256 y JSON.
 *
 * Coste: para un archivo de N bytes con piezas de P bytes:
 *   - tamaño manifiesto ≈ (N/P) * 64 bytes hex + overhead JSON.
 *   - cómputo: 1 pasada de hashing por archivo, en streaming.
 */

import fs from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import type { FileManifest } from './protocol.js';

export const DEFAULT_PIECE_SIZE = 256 * 1024; // 256 KB

/**
 * Calcula el manifiesto completo de un archivo leyendo su contenido en streaming.
 * Mantiene dos hashers en paralelo:
 *   - uno por pieza, que se reinicia cada vez que se cierra una pieza.
 *   - uno global, que ve el archivo entero.
 */
export async function buildManifest(
  filePath: string,
  pieceSize: number = DEFAULT_PIECE_SIZE,
): Promise<FileManifest> {
  const stat = await fs.promises.stat(filePath);
  if (!stat.isFile()) throw new Error(`No es un archivo regular: ${filePath}`);
  const size = stat.size;

  const numPieces = Math.max(1, Math.ceil(size / pieceSize));
  const pieceHashes: string[] = [];
  const fileHasher = createHash('sha256');
  let pieceHasher = createHash('sha256');
  let pieceFilled = 0;

  await new Promise<void>((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { highWaterMark: 64 * 1024 });
    stream.on('data', (chunk: string | Buffer) => {
      const buf = typeof chunk === 'string' ? Buffer.from(chunk) : chunk;
      let offset = 0;
      while (offset < buf.length) {
        const remainingInPiece = pieceSize - pieceFilled;
        const take = Math.min(remainingInPiece, buf.length - offset);
        const slice = buf.subarray(offset, offset + take);
        pieceHasher.update(slice);
        fileHasher.update(slice);
        pieceFilled += take;
        offset += take;
        if (pieceFilled === pieceSize) {
          pieceHashes.push(pieceHasher.digest('hex'));
          pieceHasher = createHash('sha256');
          pieceFilled = 0;
        }
      }
    });
    stream.on('end', () => {
      if (pieceFilled > 0) pieceHashes.push(pieceHasher.digest('hex'));
      resolve();
    });
    stream.on('error', reject);
  });

  // Caso especial: archivo vacío → 1 pieza vacía, hash de bytes vacíos.
  if (pieceHashes.length === 0) {
    pieceHashes.push(createHash('sha256').digest('hex'));
  }

  const fileHash = fileHasher.digest('hex');
  const fileId = fileHash; // identidad = contenido.
  return {
    fileId,
    name: path.basename(filePath),
    size,
    pieceSize,
    numPieces,
    pieceHashes,
    fileHash,
  };
}

/**
 * Persiste un manifiesto en disco como JSON.
 */
export async function saveManifest(dir: string, manifest: FileManifest): Promise<string> {
  await fs.promises.mkdir(dir, { recursive: true });
  const file = path.join(dir, `${manifest.fileId}.json`);
  await fs.promises.writeFile(file, JSON.stringify(manifest, null, 2));
  return file;
}

export async function loadManifest(dir: string, fileId: string): Promise<FileManifest | null> {
  const file = path.join(dir, `${fileId}.json`);
  try {
    const raw = await fs.promises.readFile(file, 'utf8');
    return JSON.parse(raw) as FileManifest;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw err;
  }
}
