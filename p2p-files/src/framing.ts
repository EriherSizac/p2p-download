/**
 * CAPA 3 — FRAMING
 * ----------------
 * Problema: TCP es un stream de **bytes**, no de mensajes. Si emisor escribe
 * dos veces 100 bytes, el receptor puede leer 200 de golpe, o 80+120, o como
 * el kernel decida. No hay separación de "mensajes" a nivel de socket.
 *
 * Solución: cada mensaje se prefija con su longitud (uint32 big-endian).
 * El receptor mantiene un buffer acumulador: cuando ya tiene 4 bytes, sabe
 * cuánto debe leer; cuando completa esa cantidad, emite el mensaje y avanza.
 *
 * 💡 Nota didáctica: alternativas históricas eran usar separadores (newline,
 * NUL byte). El prefijo de longitud es preferible porque:
 *   - permite payload binario arbitrario sin escapado.
 *   - no necesita escanear byte a byte buscando el separador.
 *   - acota el tamaño máximo (descarta basura/ataques DoS).
 */

const MAX_FRAME = 4 * 1024 * 1024; // 4 MB. Una pieza de 256 KB cabe sobrada.

/**
 * Envuelve un payload en un frame [len:uint32BE][payload].
 */
export function frame(payload: Buffer): Buffer {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

/**
 * Parser stateful. Se le van inyectando chunks (Buffer) según llegan del
 * socket; cada vez que detecta un mensaje completo, lo entrega al callback.
 *
 * Uso:
 *   const parser = new FrameParser(msg => handle(msg));
 *   socket.on('data', chunk => parser.push(chunk));
 */
export class FrameParser {
  private buf: Buffer = Buffer.alloc(0);
  private expected: number | null = null;

  constructor(private readonly onMessage: (msg: Buffer) => void) {}

  push(chunk: Buffer): void {
    // 💡 Concat en cada push es O(n²) en el peor caso; para fines didácticos
    // es claro y suficiente. En producción se usaría una lista de chunks.
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    this.drain();
  }

  private drain(): void {
    // Bucle: vamos sacando todos los mensajes completos disponibles.
    while (true) {
      if (this.expected === null) {
        if (this.buf.length < 4) return;
        this.expected = this.buf.readUInt32BE(0);
        if (this.expected > MAX_FRAME) {
          throw new Error(`Frame too large: ${this.expected} > ${MAX_FRAME}`);
        }
        this.buf = this.buf.subarray(4);
      }
      if (this.buf.length < this.expected) return;
      const msg = this.buf.subarray(0, this.expected);
      this.buf = this.buf.subarray(this.expected);
      this.expected = null;
      this.onMessage(msg);
    }
  }
}
