/**
 * DEMO — 3 peers en localhost
 * ---------------------------
 * Lanza 3 procesos `tsx src/main.ts` con distintas carpetas SHARED/DOWNLOAD
 * y el mismo DISCOVERY_PORT. peer1 trae un archivo de prueba; peer2 y peer3
 * lo descargan en paralelo.
 *
 * Útil para demostrar en clase la transferencia multi-peer end-to-end sin
 * necesitar dos máquinas en la misma LAN.
 */

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve('.demo');
const DISCOVERY_PORT = '51234';
const FILE_NAME = 'sample-5mb.bin';

interface Peer {
  name: string;
  proc: ChildProcessWithoutNullStreams;
  peerId?: string;
  shortId?: string;
}

function setupDirs(): void {
  fs.rmSync(ROOT, { recursive: true, force: true });
  for (const n of ['peer1', 'peer2', 'peer3']) {
    fs.mkdirSync(path.join(ROOT, n, 'shared'), { recursive: true });
    fs.mkdirSync(path.join(ROOT, n, 'downloads'), { recursive: true });
  }
  const data = randomBytes(5 * 1024 * 1024);
  fs.writeFileSync(path.join(ROOT, 'peer1', 'shared', FILE_NAME), data);
  console.log(`[demo] archivo semilla creado en peer1: ${FILE_NAME} (${data.length} bytes)`);
}

function startPeer(name: string): Peer {
  const env = {
    ...process.env,
    TCP_PORT: '0',
    DISCOVERY_PORT,
    SHARED_DIR: path.join(ROOT, name, 'shared'),
    DOWNLOAD_DIR: path.join(ROOT, name, 'downloads'),
    LOG_LEVEL: process.env['LOG_LEVEL'] ?? 'info',
  };
  const proc = spawn('npx', ['tsx', 'src/main.ts'], {
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
    shell: true,
  });
  const peer: Peer = { name, proc };
  const onLine = (line: string): void => {
    process.stdout.write(`[${name}] ${line}\n`);
    const m = /peerId = ([0-9a-f]+) \(full=([0-9a-f]+)\)/.exec(line);
    if (m) {
      peer.shortId = m[1];
      peer.peerId = m[2];
    }
  };
  bindLines(proc.stdout, onLine);
  bindLines(proc.stderr, onLine);
  return peer;
}

function bindLines(stream: NodeJS.ReadableStream, onLine: (s: string) => void): void {
  let buf = '';
  stream.on('data', (chunk: Buffer) => {
    buf += chunk.toString('utf8');
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i);
      buf = buf.slice(i + 1);
      onLine(line);
    }
  });
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

async function waitFor<T>(fn: () => T | undefined, timeoutMs: number): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const v = fn();
    if (v !== undefined) return v;
    await sleep(200);
  }
  throw new Error('waitFor: timeout');
}

async function main(): Promise<void> {
  setupDirs();
  const peer1 = startPeer('peer1');
  const peer2 = startPeer('peer2');
  const peer3 = startPeer('peer3');

  await waitFor(() => peer1.peerId, 10_000);
  await waitFor(() => peer2.peerId, 10_000);
  await waitFor(() => peer3.peerId, 10_000);

  console.log('[demo] esperando 4s a que los peers se descubran…');
  await sleep(4000);

  console.log(`[demo] peer2 -> get ${peer1.shortId} ${FILE_NAME}`);
  peer2.proc.stdin.write(`get ${peer1.shortId} ${FILE_NAME}\n`);
  console.log(`[demo] peer3 -> get ${peer1.shortId} ${FILE_NAME}`);
  peer3.proc.stdin.write(`get ${peer1.shortId} ${FILE_NAME}\n`);

  await sleep(15_000);

  console.log('[demo] cerrando peers…');
  for (const p of [peer1, peer2, peer3]) p.proc.kill('SIGINT');
  await sleep(500);
  for (const p of [peer1, peer2, peer3]) p.proc.kill('SIGKILL');
}

main().catch((err) => {
  console.error('[demo] error:', err);
  process.exit(1);
});
