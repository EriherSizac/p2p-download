/**
 * BITFIELD helpers
 * ----------------
 * Un bitfield es un arreglo compacto de bits que indica qué piezas de un
 * archivo posee un peer. Para un archivo de N piezas usamos ceil(N/8) bytes.
 *
 * Convención (la misma que BitTorrent):
 *   - el bit más significativo del byte 0 corresponde a la pieza 0.
 *   - es decir, byte = pieceIndex >> 3, mask = 1 << (7 - (pieceIndex & 7)).
 *
 * 💡 Nota didáctica: usar bitfields permite anuncios pequeños (un peer con
 * 100k piezas viaja en ~12.5 KB) y consultas O(1) "¿tienes la pieza X?".
 */

export function newBitfield(numPieces: number): Uint8Array {
  return new Uint8Array(Math.ceil(numPieces / 8));
}

export function getBit(bf: Uint8Array, idx: number): boolean {
  const byte = bf[idx >> 3];
  if (byte === undefined) return false;
  return (byte & (1 << (7 - (idx & 7)))) !== 0;
}

export function setBit(bf: Uint8Array, idx: number): void {
  const i = idx >> 3;
  if (i >= bf.length) return;
  bf[i] = (bf[i] ?? 0) | (1 << (7 - (idx & 7)));
}

export function clearBit(bf: Uint8Array, idx: number): void {
  const i = idx >> 3;
  if (i >= bf.length) return;
  bf[i] = (bf[i] ?? 0) & ~(1 << (7 - (idx & 7)));
}

export function popcount(bf: Uint8Array): number {
  let n = 0;
  for (let i = 0; i < bf.length; i++) {
    let v = bf[i] ?? 0;
    v = v - ((v >> 1) & 0x55);
    v = (v & 0x33) + ((v >> 2) & 0x33);
    n += (((v + (v >> 4)) & 0x0f) * 0x01) & 0xff;
  }
  return n;
}

export function toBase64(bf: Uint8Array): string {
  return Buffer.from(bf.buffer, bf.byteOffset, bf.byteLength).toString('base64');
}

export function fromBase64(s: string, expectedBytes: number): Uint8Array {
  const buf = Buffer.from(s, 'base64');
  // Aceptamos longitudes mayores y truncamos; rellenamos con 0 si fueran menores.
  const out = new Uint8Array(expectedBytes);
  out.set(buf.subarray(0, expectedBytes));
  return out;
}
