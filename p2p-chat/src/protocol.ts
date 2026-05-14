// File: CAPA 4 — PROTOCOLO (chat + llamadas A/V)
// Created: 2026-05-13
// Updated: 2026-05-13
// Author: Erick Hernández Silva

/**
 * CAPA 4 — PROTOCOLO (chat + llamadas A/V)
 * ----------------------------------------
 * Una vez tenemos mensajes (gracias al framing), distinguimos **tipos** de
 * mensaje y serializamos sus payloads. En este proyecto el protocolo cubre:
 *
 *   - Mensajería: HELLO, CHAT, CHAT_ACK, BYE.
 *   - Liveness: PING/PONG (mide RTT por peer).
 *   - Señalización WebRTC: CALL_OFFER/ANSWER/ICE/END. El audio/vídeo real
 *     viaja fuera de este socket — por SRTP/UDP directo entre peers — pero
 *     necesitamos un canal fiable (TCP) para intercambiar la SDP y los
 *     candidatos ICE. Eso es la señalización; ver docs/PROTOCOL.md.
 *
 * Cada mensaje empieza con 1 byte de **tipo** seguido de un payload JSON.
 *
 * 💡 Nota didáctica: la lección importante es la **discriminación por tipo**.
 * El protocolo crece añadiendo entradas a la tabla, no rediseñando el framing.
 */

import { createHash, randomBytes } from 'node:crypto';

// ---------------------------------------------------------------------------
// Tipos de mensaje (discriminantes numéricos).
// ---------------------------------------------------------------------------

export const MSG = {
  HELLO: 0x01,
  CHAT: 0x02,
  /** Acuse de recibo de un CHAT. El emisor lo usa para mostrar ✓/✗ en UI. */
  CHAT_ACK: 0x03,
  BYE: 0x04,
  /** Heartbeat saliente: el receptor responde con PONG (mismo nonce). */
  PING: 0x05,
  /** Respuesta al PING; el emisor calcula RTT = ahora - ts del nonce. */
  PONG: 0x06,
  /** Señalización WebRTC: oferta SDP del que inicia la llamada. */
  CALL_OFFER: 0x10,
  /** Señalización WebRTC: respuesta SDP del que acepta la llamada. */
  CALL_ANSWER: 0x11,
  /** Señalización WebRTC: candidato ICE individual (trickle). */
  CALL_ICE: 0x12,
  /** Señalización WebRTC: rechazo, hangup o fallo. */
  CALL_END: 0x13,
} as const;

export type MsgType = (typeof MSG)[keyof typeof MSG];

// ---------------------------------------------------------------------------
// Estructuras tipadas.
// ---------------------------------------------------------------------------

export interface IceCandidatePayload {
  candidate: string;
  sdpMid?: string;
  sdpMLineIndex?: number;
}

export type Message =
  | { type: typeof MSG.HELLO; peerId: string; version: number }
  | { type: typeof MSG.CHAT; messageId: string; text: string; ts: number }
  | { type: typeof MSG.CHAT_ACK; messageId: string }
  | { type: typeof MSG.BYE }
  | { type: typeof MSG.PING; nonce: number }
  | { type: typeof MSG.PONG; nonce: number }
  | { type: typeof MSG.CALL_OFFER; callId: string; sdp: string }
  | { type: typeof MSG.CALL_ANSWER; callId: string; sdp: string }
  | { type: typeof MSG.CALL_ICE; callId: string; candidate: IceCandidatePayload | null }
  | { type: typeof MSG.CALL_END; callId: string; reason?: string };

export const PROTOCOL_VERSION = 2;

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
    case MSG.CHAT_ACK:
    case MSG.PING:
    case MSG.PONG:
    case MSG.CALL_OFFER:
    case MSG.CALL_ANSWER:
    case MSG.CALL_ICE:
    case MSG.CALL_END: {
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

/** id corto y aleatorio para correlacionar mensajes de una misma llamada. */
export function newCallId(): string {
  return randomBytes(6).toString('hex');
}
