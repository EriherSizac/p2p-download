/**
 * PEER-STATE — estado por peer remoto
 * -----------------------------------
 * Para cada peer conectado guardamos:
 *   - bitfields: qué piezas tiene de cada archivo (lo aprendemos de HAVE).
 *   - inFlight: qué piezas le hemos pedido y aún no nos ha enviado, con el
 *     timestamp de envío para detectar timeouts.
 *   - score: contador simple de fallos (piezas con hash incorrecto, timeouts).
 *     Se podría usar para preferir peers fiables — aquí el scheduler lo
 *     consulta de manera mínima.
 *
 * 💡 Nota didáctica: este archivo es puro estado en memoria; toda la lógica
 * de selección vive en scheduler.ts. Esta separación (datos vs. política) es
 * útil cuando se quiere experimentar con distintas estrategias.
 */

import { fromBase64 } from './bitfield.js';

export interface InFlightRequest {
  fileId: string;
  pieceIndex: number;
  sentAt: number;
}

export class PeerState {
  /** fileId → bitfield del peer remoto. */
  private bitfields = new Map<string, Uint8Array>();
  /** clave `${fileId}:${pieceIndex}` → request en vuelo. */
  private inFlight = new Map<string, InFlightRequest>();
  /** total de fallos imputables a este peer. */
  failures = 0;

  constructor(public readonly peerId: string) {}

  setBitfield(fileId: string, base64: string, expectedBytes: number): void {
    this.bitfields.set(fileId, fromBase64(base64, expectedBytes));
  }

  getBitfield(fileId: string): Uint8Array | undefined {
    return this.bitfields.get(fileId);
  }

  hasPiece(fileId: string, pieceIndex: number): boolean {
    const bf = this.bitfields.get(fileId);
    if (!bf) return false;
    const byte = bf[pieceIndex >> 3];
    if (byte === undefined) return false;
    return (byte & (1 << (7 - (pieceIndex & 7)))) !== 0;
  }

  inFlightCount(): number {
    return this.inFlight.size;
  }

  isInFlight(fileId: string, pieceIndex: number): boolean {
    return this.inFlight.has(`${fileId}:${pieceIndex}`);
  }

  addInFlight(fileId: string, pieceIndex: number): void {
    this.inFlight.set(`${fileId}:${pieceIndex}`, { fileId, pieceIndex, sentAt: Date.now() });
  }

  removeInFlight(fileId: string, pieceIndex: number): void {
    this.inFlight.delete(`${fileId}:${pieceIndex}`);
  }

  /** Devuelve y olvida los requests cuyo sentAt es más viejo que `cutoff`. */
  reapTimeouts(cutoffMs: number): InFlightRequest[] {
    const now = Date.now();
    const dead: InFlightRequest[] = [];
    for (const [k, r] of this.inFlight) {
      if (now - r.sentAt > cutoffMs) {
        dead.push(r);
        this.inFlight.delete(k);
      }
    }
    return dead;
  }

  allInFlight(): InFlightRequest[] {
    return [...this.inFlight.values()];
  }
}
