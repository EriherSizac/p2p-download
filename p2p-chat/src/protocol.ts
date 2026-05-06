/**
 * CAPA 4 — PROTOCOLO (chat)
 * -------------------------
 * Una vez tenemos mensajes (gracias al framing), distinguimos **tipos** de
 * mensaje y serializamos sus payloads. En este proyecto el protocolo solo
 * cubre mensajería: HELLO para identificarse, CHAT para enviar texto y BYE
 * como cierre limpio.
 *
 * Cada mensaje empieza con 1 byte de **tipo** seguido de un payload JSON.
 *
 * 💡 Nota didáctica: la lección importante es la **discriminación por tipo**.
 * Aquí no necesitamos payload binario crudo (no transferimos archivos), así
 * que JSON es trivial y suficiente.
 */

import { createHash, randomBytes } from 'node:crypto';

// ---------------------------------------------------------------------------
// Tipos de mensaje (discriminantes numéricos).
// ---------------------------------------------------------------------------

export const MSG = {
  HELLO: 0x01,
  CHAT: 0x02,
  /** Acuse de recibo de un CHAT (ejercicio: ver docs/EXERCISES.md). */
  CHAT_ACK: 0x03,
  BYE: 0x04,
} as const;

export type MsgType = (typeof MSG)[keyof typeof MSG];

// ---------------------------------------------------------------------------
// Estructuras tipadas.
// ---------------------------------------------------------------------------

export type Message =
  | { type: typeof MSG.HELLO; peerId: string; version: number }
  | {
      type: typeof MSG.CHAT;
      /** id único del mensaje, útil para emparejar ACKs (ejercicio). */
      messageId: string;
      text: string;
      ts: number;
    }
  | { type: typeof MSG.CHAT_ACK; messageId: string }
  | { type: typeof MSG.BYE };

export const PROTOCOL_VERSION = 1;

// ---------------------------------------------------------------------------
// Codec.
// ---------------------------------------------------------------------------

export function encode(msg: Message): Buffer {
  const t = Buffer.from([msg.type]);
  if (msg.type === MSG.BYE) return t;
  const { type: _omit, ...rest } = msg as Record<string, unknown>;
  void _omit;
  return Buffer.concat([t, Buffer.from(JSON.stringify(rest))]);
}

export function decode(buf: Buffer): Message {
  if (buf.length < 1) throw new Error('Empty message');
  const type = buf[0] as MsgType;
  const body = buf.subarray(1);

  switch (type) {
    case MSG.BYE:
      return { type };
    case MSG.HELLO:
    case MSG.CHAT:
    case MSG.CHAT_ACK: {
      const obj = JSON.parse(body.toString('utf8')) as Record<string, unknown>;
      return { type, ...obj } as Message;
    }
    default:
      throw new Error(`Unknown message type: 0x${(type as number).toString(16)}`);
  }
}

// ---------------------------------------------------------------------------
// Identidad: peerId efímero generado al arranque.
// ---------------------------------------------------------------------------

/**
 * Genera un peerId único hex (SHA-256 sobre 32 bytes aleatorios).
 *
 * 💡 Nota didáctica: esto NO es identidad criptográfica — cualquiera podría
 * suplantar un peerId al no firmar nada. Para un chat real haría falta un
 * keypair (Ed25519) y firmar los HELLO/CHAT.
 */
export function generatePeerId(): string {
  return createHash('sha256').update(randomBytes(32)).digest('hex');
}

export function shortId(peerId: string): string {
  return peerId.slice(0, 8);
}

/** id corto y aleatorio para correlacionar CHAT ↔ CHAT_ACK. */
export function newMessageId(): string {
  return randomBytes(8).toString('hex');
}
