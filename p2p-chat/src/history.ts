// File: HISTORY — persistencia simple de mensajes
// Created: 2026-05-13
// Updated: 2026-05-13
// Author: Erick Hernández Silva

/**
 * HISTORY — persistencia simple de mensajes
 * -----------------------------------------
 * Cada CHAT enviado o recibido se escribe como una línea JSON en
 * `./history.jsonl`. Una línea por mensaje permite append O(1) sin reescribir
 * el fichero entero, y `tail` se reduce a leer las últimas N líneas.
 *
 * 💡 Nota didáctica: JSONL (JSON-lines) es el formato natural cuando solo
 * necesitas append. Si tu acceso es por id o por consultas estructuradas,
 * usarías SQLite; aquí buscamos transparencia: el alumno puede abrir el
 * fichero con `cat` y leerlo.
 */

import { appendFile, readFile, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';

export type Direction = 'in' | 'out';

export interface HistoryRecord {
  ts: number;
  dir: Direction;
  /** peerId del otro extremo (no el nuestro). */
  peerId: string;
  /** Para correlacionar con CHAT_ACK al releer. */
  messageId: string;
  text: string;
}

const FILE = path.resolve(process.cwd(), 'history.jsonl');

export async function append(rec: HistoryRecord): Promise<void> {
  await appendFile(FILE, JSON.stringify(rec) + '\n', 'utf8');
}

/**
 * Devuelve los últimos `n` registros. Lee el fichero entero, parsea por
 * líneas y se queda con la cola. Suficiente para historiales pequeños.
 *
 * 💡 Nota didáctica: para historiales grandes haría falta leer desde el final
 * usando `fs.read` con `position`, retrocediendo en bloques hasta acumular n
 * saltos de línea. Lo dejamos como mejora.
 */
export async function tail(n: number): Promise<HistoryRecord[]> {
  if (!existsSync(FILE)) return [];
  const text = await readFile(FILE, 'utf8');
  const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
  const slice = lines.slice(-n);
  const out: HistoryRecord[] = [];
  for (const line of slice) {
    try {
      out.push(JSON.parse(line) as HistoryRecord);
    } catch {
      // línea corrupta — ignorar
    }
  }
  return out;
}

export async function size(): Promise<number> {
  if (!existsSync(FILE)) return 0;
  return (await stat(FILE)).size;
}

export function filePath(): string {
  return FILE;
}
