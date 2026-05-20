// File: PROTOCOLO SWARM
// Created: 2026-05-19
// Updated: 2026-05-19
// Author: Erick Hernández Silva

/**
 * PROTOCOLO SWARM
 * ---------------
 * Extiende el protocolo base (HELLO, BYE, PING, PONG, PEER_LIST) con
 * tres mensajes de routing:
 *
 *   SEARCH  — inundación para localizar un peer no conectado directamente.
 *             Cada nodo intermedio añade su id a `hops` y reenvía con TTL-1.
 *             El target responde con FOUND.
 *
 *   FOUND   — respuesta del target. Viaja de vuelta HOP a HOP usando la
 *             lista `path` como guía (path[i-1] = siguiente salto hacia origen).
 *
 *   DELIVER — mensaje de texto enrutado por path. Mismo mecanismo que FOUND
 *             pero en dirección contraria (hacia el target).
 *
 * El campo `hops` en SEARCH sirve para evitar bucles: si ya apareces en hops
 * no reenvías. El TTL es una segunda línea de defensa.
 */

import { createHash, randomBytes } from 'node:crypto';

export const MSG = {
  HELLO:    0x01,
  BYE:      0x04,
  PING:     0x05,
  PONG:     0x06,
  PEER_LIST: 0x07,
  SEARCH:   0x20,
  FOUND:    0x21,
  DELIVER:  0x22,
} as const;

export type MsgType = (typeof MSG)[keyof typeof MSG];
export const PROTOCOL_VERSION = 1;

export type Message =
  | { type: typeof MSG.HELLO;     peerId: string; version: number }
  | { type: typeof MSG.BYE }
  | { type: typeof MSG.PING;      nonce: number; manual?: true }
  | { type: typeof MSG.PONG;      nonce: number; manual?: true }
  | { type: typeof MSG.PEER_LIST; peers: string[] }
  | { type: typeof MSG.SEARCH;    searchId: string; targetId: string; originId: string; ttl: number; hops: string[] }
  | { type: typeof MSG.FOUND;     searchId: string; targetId: string; path: string[] }
  | { type: typeof MSG.DELIVER;   searchId: string; targetId: string; originId: string; text: string; path: string[] };

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
    case MSG.BYE: return { type };
    case MSG.HELLO:
    case MSG.PING:
    case MSG.PONG:
    case MSG.PEER_LIST:
    case MSG.SEARCH:
    case MSG.FOUND:
    case MSG.DELIVER: {
      const obj = JSON.parse(body.toString('utf8')) as Record<string, unknown>;
      return { type, ...obj } as Message;
    }
    default:
      throw new Error(`Unknown message type: 0x${(type as number).toString(16)}`);
  }
}

export function generatePeerId(): string {
  return createHash('sha256').update(randomBytes(32)).digest('hex');
}

export function shortId(peerId: string): string {
  return peerId.slice(0, 8);
}

export function newSearchId(): string {
  return randomBytes(6).toString('hex');
}
