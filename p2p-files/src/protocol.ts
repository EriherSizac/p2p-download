/**
 * CAPA 4 — PROTOCOLO
 * ------------------
 * Problema: una vez tenemos mensajes (gracias al framing), necesitamos
 * distinguir **tipos** de mensaje y serializar/deserializar sus payloads.
 *
 * Solución: cada mensaje empieza con 1 byte de **tipo** seguido de un payload.
 * La mayoría de payloads son JSON (legibles, fáciles de extender). El tipo
 * PIECE es híbrido: cabecera JSON + bytes binarios crudos, porque meter
 * binario dentro de JSON costaría base64 (~33% extra).
 *
 * 💡 Nota didáctica: BitTorrent real usa un protocolo binario más compacto
 * (bencode + tipos numéricos), pero JSON aquí ayuda a la introspección. La
 * lección importante es la **discriminación por tipo**, no el formato.
 */

import { createHash, randomBytes } from 'node:crypto';

// ---------------------------------------------------------------------------
// Tipos de mensaje (discriminantes numéricos).
// ---------------------------------------------------------------------------

export const MSG = {
  HELLO: 0x01,
  LIST: 0x02,
  LIST_REPLY: 0x03,
  MANIFEST: 0x04,
  MANIFEST_REPLY: 0x05,
  HAVE: 0x06,
  REQUEST: 0x07,
  PIECE: 0x08,
  ERROR: 0x09,
  BYE: 0x0a,
  /** Gossip topológico. El emisor lista sus peerIds conectados; el
   *  receptor lo usa para construir el grafo global de la red. */
  PEER_LIST: 0x0b,
} as const;

export type MsgType = (typeof MSG)[keyof typeof MSG];

// ---------------------------------------------------------------------------
// Estructuras tipadas.
// ---------------------------------------------------------------------------

export interface FileSummary {
  fileId: string;
  name: string;
  size: number;
  pieceSize: number;
  numPieces: number;
}

export interface FileManifest extends FileSummary {
  /** Hashes hex SHA-256 por pieza. Longitud === numPieces. */
  pieceHashes: string[];
  /** Hash hex SHA-256 del archivo completo. */
  fileHash: string;
}

export type Message =
  | { type: typeof MSG.HELLO; peerId: string; version: number }
  | { type: typeof MSG.LIST }
  | { type: typeof MSG.LIST_REPLY; files: FileSummary[] }
  | { type: typeof MSG.MANIFEST; fileId: string }
  | { type: typeof MSG.MANIFEST_REPLY; manifest: FileManifest }
  | { type: typeof MSG.HAVE; fileId: string; bitfield: string /* base64 */ }
  | { type: typeof MSG.REQUEST; fileId: string; pieceIndex: number }
  | { type: typeof MSG.PIECE; fileId: string; pieceIndex: number; data: Buffer }
  | { type: typeof MSG.ERROR; code: string; message: string }
  | { type: typeof MSG.BYE }
  | { type: typeof MSG.PEER_LIST; peers: string[] };

export const PROTOCOL_VERSION = 1;

// ---------------------------------------------------------------------------
// Codec.
// ---------------------------------------------------------------------------

export function encode(msg: Message): Buffer {
  const t = Buffer.from([msg.type]);
  switch (msg.type) {
    case MSG.LIST:
    case MSG.BYE:
      return t;
    case MSG.PIECE: {
      // Estructura: [type:1][headerLen:uint32BE][headerJSON][rawBytes]
      const header = Buffer.from(
        JSON.stringify({ fileId: msg.fileId, pieceIndex: msg.pieceIndex }),
      );
      const len = Buffer.alloc(4);
      len.writeUInt32BE(header.length, 0);
      return Buffer.concat([t, len, header, msg.data]);
    }
    default: {
      // Resto: [type:1][JSON]
      const { type: _omit, ...rest } = msg as Record<string, unknown>;
      void _omit;
      return Buffer.concat([t, Buffer.from(JSON.stringify(rest))]);
    }
  }
}

export function decode(buf: Buffer): Message {
  if (buf.length < 1) throw new Error('Empty message');
  const type = buf[0] as MsgType;
  const body = buf.subarray(1);

  switch (type) {
    case MSG.LIST:
      return { type };
    case MSG.BYE:
      return { type };
    case MSG.PIECE: {
      if (body.length < 4) throw new Error('PIECE: header length missing');
      const headerLen = body.readUInt32BE(0);
      if (body.length < 4 + headerLen) throw new Error('PIECE: truncated header');
      const header = JSON.parse(body.subarray(4, 4 + headerLen).toString('utf8')) as {
        fileId: string;
        pieceIndex: number;
      };
      const data = body.subarray(4 + headerLen);
      return { type, fileId: header.fileId, pieceIndex: header.pieceIndex, data };
    }
    case MSG.HELLO:
    case MSG.LIST_REPLY:
    case MSG.MANIFEST:
    case MSG.MANIFEST_REPLY:
    case MSG.HAVE:
    case MSG.REQUEST:
    case MSG.ERROR:
    case MSG.PEER_LIST: {
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
 * suplantar un peerId al no firmar nada. Para uso real haría falta un
 * keypair (Ed25519) y firmar los HELLO/HAVE. Se discute en docs/NAT.md.
 */
export function generatePeerId(): string {
  return createHash('sha256').update(randomBytes(32)).digest('hex');
}

export function shortId(peerId: string): string {
  return peerId.slice(0, 8);
}
