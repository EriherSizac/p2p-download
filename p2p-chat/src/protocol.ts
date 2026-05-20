// File: CAPA 4 — PROTOCOLO (chat + llamadas A/V)
// Created: 2026-05-13
// Updated: 2026-05-19
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

// Discriminantes numéricos. Bloques organizados por "familia" — los hex no
// son consecutivos a propósito: deja hueco para crecer en cada familia sin
// reordenar (p.ej. añadir CHAT_EDIT entre CHAT_ACK y BYE rompería todos
// los protocolos en producción; con huecos no).
export const MSG = {
  // ── Handshake + mensajería básica (0x01–0x04) ─────────────────────────
  HELLO: 0x01,
  CHAT: 0x02,
  /** Acuse de recibo de un CHAT. El emisor lo usa para mostrar ✓/✗ en UI. */
  CHAT_ACK: 0x03,
  BYE: 0x04,

  // ── Liveness (0x05–0x06) ──────────────────────────────────────────────
  /** Heartbeat saliente: el receptor responde con PONG (mismo nonce). */
  PING: 0x05,
  /** Respuesta al PING; el emisor calcula RTT = ahora - ts del nonce. */
  PONG: 0x06,

  // ── Gossip de topología (0x07) ────────────────────────────────────────
  /** Anuncia qué peers conoce el emisor. Se usa para reconstruir el
   *  grafo global de conectividad (comando `graph`). */
  PEER_LIST: 0x07,

  // ── Señalización WebRTC (0x10–0x13) ───────────────────────────────────
  // Ojo: SOLO viaja por aquí la SDP y los candidatos ICE. El audio real
  // va por SRTP/UDP fuera de este protocolo (ver src/call.ts).
  /** Oferta SDP del que inicia la llamada. */
  CALL_OFFER: 0x10,
  /** Respuesta SDP del que acepta la llamada. */
  CALL_ANSWER: 0x11,
  /** Un candidato ICE individual (trickle). `candidate: null` = fin. */
  CALL_ICE: 0x12,
  /** Rechazo, hangup o fallo. `reason` es informativo. */
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
  | { type: typeof MSG.PING; nonce: number; manual?: true }
  | { type: typeof MSG.PONG; nonce: number; manual?: true }
  | { type: typeof MSG.PEER_LIST; peers: string[] }
  | { type: typeof MSG.CALL_OFFER; callId: string; sdp: string }
  | { type: typeof MSG.CALL_ANSWER; callId: string; sdp: string }
  | { type: typeof MSG.CALL_ICE; callId: string; candidate: IceCandidatePayload | null }
  | { type: typeof MSG.CALL_END; callId: string; reason?: string };

export const PROTOCOL_VERSION = 2;

// ---------------------------------------------------------------------------
// Codec.
// ---------------------------------------------------------------------------

/**
 * Serializa Message → Buffer. Formato: [type:u8][payload JSON].
 * BYE no lleva payload — es solo el byte de tipo.
 */
export function encode(msg: Message): Buffer {
  const t = Buffer.from([msg.type]);
  if (msg.type === MSG.BYE) return t;
  // Omitimos `type` del JSON; ya está en el primer byte. Si lo dejásemos
  // duplicaría info y pagaríamos bytes en cada mensaje sin motivo.
  const { type: _omit, ...rest } = msg as Record<string, unknown>;
  void _omit;
  return Buffer.concat([t, Buffer.from(JSON.stringify(rest))]);
}

/**
 * Deserializa Buffer → Message. Lanza si el tipo es desconocido — la capa
 * de transporte usa esa excepción para cerrar el socket (mensaje basura
 * = peer hostil o protocolo incompatible).
 */
export function decode(buf: Buffer): Message {
  if (buf.length < 1) throw new Error('Empty message');
  const type = buf[0] as MsgType;
  const body = buf.subarray(1);

  switch (type) {
    case MSG.BYE:
      // Sin payload — devolvemos directamente.
      return { type };
    // Todos los demás tipos son JSON en el body. Cada uno tiene su forma
    // pero el codec es el mismo: parse → { type, ...campos }.
    case MSG.HELLO:
    case MSG.CHAT:
    case MSG.CHAT_ACK:
    case MSG.PING:
    case MSG.PONG:
    case MSG.PEER_LIST:
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
