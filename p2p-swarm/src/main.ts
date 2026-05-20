// File: P2P-SWARM — demostración de routing en malla parcial.
// Created: 2026-05-19
// Updated: 2026-05-19
// Author: Erick Hernández Silva

/**
 * P2P-SWARM — demostración de routing en malla parcial.
 *
 * Diferencia clave vs p2p-chat:
 *   - Cada peer se conecta a MAX_PEERS vecinos como máximo (no full-mesh).
 *   - Para llegar a un peer no-vecino se usa inundación con TTL (SEARCH).
 *   - El camino encontrado se visualiza en el grafo.
 *
 * Protocolo de búsqueda (flood):
 *   1. Origen envía SEARCH a todos sus vecinos.
 *   2. Cada nodo reenvía si no vio el searchId antes y TTL > 0.
 *   3. El target responde FOUND con el path completo.
 *   4. FOUND viaja de vuelta HOP a HOP por path[i-1].
 *   5. Origen recibe FOUND → sabe el camino → puede enviar DELIVER.
 *
 * DELIVER sigue el mismo path en sentido contrario (hacia el target).
 */

import readline from 'node:readline';
import { spawn }  from 'node:child_process';
import { Discovery }  from './discovery.js';
import { Transport }  from './transport.js';
import { GraphServer, type GraphSnapshot } from './graph-server.js';
import {
  MSG, generatePeerId, shortId, newSearchId,
  type Message,
} from './protocol.js';

// ── Configuración ────────────────────────────────────────────────────────────

const TCP_PORT       = Number(process.env['TCP_PORT']       ?? 41250);
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41249);
/** Máximo de peers directamente conectados. Núcleo de la demo. */
const MAX_PEERS      = Number(process.env['MAX_PEERS']      ?? 3);
const SEARCH_TTL     = Number(process.env['SEARCH_TTL']     ?? 7);
const SEARCH_TIMEOUT = 8_000;
const PEER_LIST_INTERVAL  = 12_000;
const PING_INTERVAL       = 5_000;

// ── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  console.log(`peerId : ${peerId}`);
  console.log(`shortId: ${shortId(peerId)}`);
  console.log(`MAX_PEERS=${MAX_PEERS}  TTL=${SEARCH_TTL}  TCP=${TCP_PORT}  DISC=${DISCOVERY_PORT}\n`);

  const transport  = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  // ── Estado ──────────────────────────────────────────────────────────────

  const peerLists   = new Map<string, { peers: string[]; ts: number }>();
  const liveness    = new Map<string, { rttMs?: number; pendingPings: Map<number, number> }>();
  let nextNonce     = 1;

  const seenSearches    = new Set<string>();
  const pendingSearches = new Map<string, {
    resolve: (path: string[]) => void;
    reject:  (err: Error) => void;
    timer:   NodeJS.Timeout;
  }>();

  let graphServer: GraphServer | undefined;

  // ── Descubrimiento → conectar solo si hay hueco ─────────────────────────

  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId && transport.connectedPeers().length < MAX_PEERS) {
      transport.connect(p.peerId, p.host, p.port);
    }
  });

  // ── Helpers ──────────────────────────────────────────────────────────────

  const sendPeerList = (to: string): void => {
    transport.send(to, { type: MSG.PEER_LIST, peers: transport.connectedPeers() });
  };

  transport.on('peer-connected', (pid) => { sendPeerList(pid); pushSnapshot(); });
  transport.on('peer-disconnected', (pid) => { peerLists.delete(pid); liveness.delete(pid); pushSnapshot(); });

  setInterval(() => {
    for (const pid of transport.connectedPeers()) sendPeerList(pid);
  }, PEER_LIST_INTERVAL);

  setInterval(() => {
    for (const pid of transport.connectedPeers()) {
      let liv = liveness.get(pid);
      if (!liv) { liv = { pendingPings: new Map() }; liveness.set(pid, liv); }
      const nonce = nextNonce++;
      liv.pendingPings.set(nonce, Date.now());
      transport.send(pid, { type: MSG.PING, nonce });
    }
  }, PING_INTERVAL);

  // ── Búsqueda: iniciar flood ──────────────────────────────────────────────

  function startSearch(targetId: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      const searchId = newSearchId();
      const timer = setTimeout(() => {
        pendingSearches.delete(searchId);
        graphServer?.searchPath(searchId, [], 'timeout');
        reject(new Error(`timeout: ${shortId(targetId)} no respondió en ${SEARCH_TIMEOUT}ms`));
      }, SEARCH_TIMEOUT);

      pendingSearches.set(searchId, { resolve, reject, timer });
      seenSearches.add(searchId);

      const msg: Message = {
        type: MSG.SEARCH, searchId, targetId,
        originId: peerId, ttl: SEARCH_TTL, hops: [peerId],
      };
      const peers = transport.connectedPeers();
      if (peers.length === 0) {
        clearTimeout(timer);
        pendingSearches.delete(searchId);
        reject(new Error('sin vecinos conectados'));
        return;
      }
      for (const pid of peers) transport.send(pid, msg);
    });
  }

  // ── Manejador de mensajes ────────────────────────────────────────────────

  transport.on('message', (from: string, msg: Message) => {
    switch (msg.type) {

      case MSG.PING:
        transport.send(from, { type: MSG.PONG, nonce: msg.nonce, ...(msg.manual && { manual: true }) });
        break;

      case MSG.PONG: {
        const liv = liveness.get(from);
        if (!liv) break;
        const sentAt = liv.pendingPings.get(msg.nonce);
        if (sentAt === undefined) break;
        liv.pendingPings.delete(msg.nonce);
        const rtt = Date.now() - sentAt;
        liv.rttMs = liv.rttMs === undefined ? rtt : 0.2 * rtt + 0.8 * liv.rttMs;
        break;
      }

      case MSG.PEER_LIST:
        peerLists.set(from, { peers: msg.peers, ts: Date.now() });
        pushSnapshot();
        break;

      case MSG.SEARCH: {
        if (seenSearches.has(msg.searchId)) break;
        seenSearches.add(msg.searchId);

        if (msg.targetId === peerId) {
          const path = [...msg.hops, peerId];
          const found: Message = { type: MSG.FOUND, searchId: msg.searchId, targetId: peerId, path };
          const prevHop = msg.hops.at(-1);
          if (prevHop && transport.isConnected(prevHop)) transport.send(prevHop, found);
          break;
        }
        if (msg.ttl <= 0) break;

        const fwd: Message = {
          type: MSG.SEARCH, searchId: msg.searchId, targetId: msg.targetId,
          originId: msg.originId, ttl: msg.ttl - 1, hops: [...msg.hops, peerId],
        };
        for (const pid of transport.connectedPeers()) {
          if (pid !== from && !msg.hops.includes(pid)) transport.send(pid, fwd);
        }
        break;
      }

      case MSG.FOUND: {
        const myIdx = msg.path.indexOf(peerId);
        if (myIdx <= 0) {
          const pending = pendingSearches.get(msg.searchId);
          if (pending) {
            clearTimeout(pending.timer);
            pendingSearches.delete(msg.searchId);
            pending.resolve(msg.path);
            graphServer?.searchPath(msg.searchId, msg.path, 'found');
            graphServer?.toast(`🎯 ${shortId(msg.targetId)} — ${msg.path.length - 1} salto(s)`);
          }
          break;
        }
        const prevHop = myIdx > 0 ? msg.path[myIdx - 1] : undefined;
        if (prevHop && transport.isConnected(prevHop)) transport.send(prevHop, msg);
        break;
      }

      case MSG.DELIVER: {
        const myIdx   = msg.path.indexOf(peerId);
        const lastIdx = msg.path.length - 1;
        if (myIdx === lastIdx) {
          const hops = msg.path.length - 1;
          cliPrint(`\n📩 [${shortId(msg.originId)} via ${hops} salto(s)]: ${msg.text}`);
          graphServer?.msgReceived(msg.originId, msg.text, hops);
          break;
        }
        if (myIdx >= 0 && myIdx < lastIdx) {
          const nextHop = msg.path[myIdx + 1] ?? '';
          if (nextHop && transport.isConnected(nextHop)) transport.send(nextHop, msg);
        }
        break;
      }

      case MSG.BYE: break;
    }
  });

  // ── Grafo en vivo ────────────────────────────────────────────────────────

  const buildSnapshot = (): GraphSnapshot => {
    const me = peerId;
    const adj = new Map<string, Set<string>>();
    adj.set(me, new Set(transport.connectedPeers()));
    for (const [pid, info] of peerLists) adj.set(pid, new Set(info.peers));
    const nodeSet = new Set<string>(adj.keys());
    for (const targets of adj.values()) for (const t of targets) nodeSet.add(t);

    const visNodes = [...nodeSet].map((pid) => {
      const rttTip = (liveness.get(pid)?.rttMs ?? undefined) !== undefined
        ? `\nRTT ≈ ${liveness.get(pid)!.rttMs!.toFixed(0)} ms` : '';
      return {
        id: pid,
        label: shortId(pid) + (pid === me ? '\n(tú)' : ''),
        color: pid === me ? { background: '#2ecc71', border: '#27ae60' } : { background: '#3498db', border: '#2980b9' },
        size: pid === me ? 28 : 22,
        title: pid + rttTip,
      };
    });

    const edgeMap = new Map<string, { a: string; b: string; mutual: boolean }>();
    for (const [a, targets] of adj) {
      for (const b of targets) {
        const key = a < b ? `${a}|${b}` : `${b}|${a}`;
        const ex  = edgeMap.get(key);
        if (ex) ex.mutual = true;
        else edgeMap.set(key, { a, b, mutual: adj.get(b)?.has(a) ?? false });
      }
    }
    const visEdges = [...edgeMap.entries()].map(([id, e]) => ({
      id, from: e.a, to: e.b, dashes: !e.mutual,
      color: { color: e.mutual ? '#27ae60' : '#7f8c8d' },
      width: e.mutual ? 2 : 1,
    }));

    const mutuals  = [...edgeMap.values()].filter((e) => e.mutual).length;
    const total    = nodeSet.size;
    const maxEdges = (total * (total - 1)) / 2;
    return {
      nodes: visNodes, edges: visEdges,
      stats: { total, mutuals, oneway: edgeMap.size - mutuals, density: maxEdges > 0 ? (mutuals / maxEdges).toFixed(2) : '0.00', maxPeers: MAX_PEERS },
      directPeers: transport.connectedPeers(),
      ts: Date.now(),
    };
  };

  const pushSnapshot = (): void => {
    if (!graphServer?.hasClients()) return;
    graphServer.emit(buildSnapshot());
  };

  const openGraph = async (): Promise<void> => {
    if (!graphServer) {
      graphServer = new GraphServer();
      await graphServer.start();
      graphServer.setActions({
        async sendMsg(targetId, text) {
          const allKnown = gatherKnown(peerId, transport, peerLists);
          const match = resolveId(targetId, allKnown);
          if (!match) throw new Error(`peer desconocido: "${targetId}"`);
          if (transport.isConnected(match)) {
            transport.send(match, { type: MSG.DELIVER, searchId: newSearchId(), targetId: match, originId: peerId, text, path: [peerId, match] });
            return;
          }
          const path = await startSearch(match);
          const nextHop = path[1];
          if (!nextHop || !transport.isConnected(nextHop)) throw new Error('ruta expiró');
          transport.send(nextHop, { type: MSG.DELIVER, searchId: newSearchId(), targetId: match, originId: peerId, text, path });
        },
        async findPeer(targetId) {
          const allKnown = gatherKnown(peerId, transport, peerLists);
          const match = resolveId(targetId, allKnown);
          if (!match) throw new Error(`peer desconocido: "${targetId}"`);
          if (match === peerId) return [peerId];
          if (transport.isConnected(match)) return [peerId, match];
          return startSearch(match);
        },
      });
    }
    graphServer.emit(buildSnapshot());
    const url = graphServer.url();
    cliPrint(`grafo: ${url}`);
    // Abre el navegador por defecto. La URL es 127.0.0.1:puerto — sin input de usuario.
    spawn('cmd', ['/c', 'start', '', url], { detached: true, stdio: 'ignore' }).unref();
  };

  // ── CLI ──────────────────────────────────────────────────────────────────

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'swarm> ' });
  rl.prompt();

  const cliPrint = (text: string): void => {
    process.stdout.write('\r\x1b[K' + text + '\n');
    rl.prompt(true);
  };

  rl.on('line', async (raw) => {
    const line  = raw.trim();
    const parts = line.split(/\s+/);
    const cmd   = parts[0] ?? '';
    try {
      switch (cmd) {
        case 'peers': {
          const ps = transport.connectedPeers();
          if (ps.length === 0) { cliPrint('sin peers directos'); break; }
          for (const p of ps) {
            const rtt = liveness.get(p)?.rttMs;
            cliPrint(`  ${shortId(p)}  ${rtt !== undefined ? rtt.toFixed(0) + ' ms' : '—'}`);
          }
          cliPrint(`(${ps.length}/${MAX_PEERS} slots)`);
          break;
        }

        case 'find': {
          const target = parts[1];
          if (!target) { cliPrint('uso: find <shortId|fullId>'); break; }
          const allKnown = gatherKnown(peerId, transport, peerLists);
          const match = resolveId(target, allKnown);
          if (!match) { cliPrint(`prefijo "${target}" no encontrado`); break; }
          if (match === peerId) { cliPrint('ese eres tú'); break; }
          if (transport.isConnected(match)) { cliPrint(`${shortId(match)} es vecino directo`); break; }
          cliPrint(`buscando ${shortId(match)}…`);
          const path = await startSearch(match);
          cliPrint(`encontrado: ${path.map(shortId).join(' → ')}  (${path.length - 1} salto(s))`);
          break;
        }

        case 'send': {
          const target = parts[1];
          const text   = parts.slice(2).join(' ');
          if (!target || !text) { cliPrint('uso: send <shortId|fullId> <texto>'); break; }
          const allKnown = gatherKnown(peerId, transport, peerLists);
          const match = resolveId(target, allKnown);
          if (!match) { cliPrint(`peer desconocido: "${target}"`); break; }

          if (transport.isConnected(match)) {
            transport.send(match, { type: MSG.DELIVER, searchId: newSearchId(), targetId: match, originId: peerId, text, path: [peerId, match] });
            cliPrint(`→ enviado directo a ${shortId(match)}`);
            break;
          }
          cliPrint(`buscando ruta a ${shortId(match)}…`);
          const path = await startSearch(match);
          const nextHop = path[1];
          if (!nextHop || !transport.isConnected(nextHop)) { cliPrint('ruta expiró'); break; }
          transport.send(nextHop, { type: MSG.DELIVER, searchId: newSearchId(), targetId: match, originId: peerId, text, path });
          cliPrint(`→ enrutado: ${path.map(shortId).join(' → ')}`);
          break;
        }

        case 'graph':
          await openGraph();
          break;

        case '':
        case 'help':
          cliPrint([
            'comandos:',
            '  peers                — vecinos directos + RTT',
            '  find <id>            — flood para localizar peer',
            '  send <id> <texto>    — enviar con routing automático',
            '  graph                — abrir grafo en el navegador',
            '',
            `  MAX_PEERS=${MAX_PEERS}  SEARCH_TTL=${SEARCH_TTL}  TCP=${TCP_PORT}`,
          ].join('\n'));
          break;

        default:
          cliPrint(`desconocido: "${cmd}" — escribe "help"`);
      }
    } catch (err) {
      cliPrint(`error: ${(err as Error).message}`);
    }
    rl.prompt();
  });

  rl.on('close', () => process.exit(0));
}

// ── Utilidades ───────────────────────────────────────────────────────────────

function gatherKnown(
  peerId: string,
  transport: Transport,
  peerLists: Map<string, { peers: string[] }>,
): Set<string> {
  const s = new Set<string>([peerId]);
  for (const pid of transport.connectedPeers()) s.add(pid);
  for (const info of peerLists.values()) for (const p of info.peers) s.add(p);
  return s;
}

function resolveId(target: string, known: Set<string>): string | undefined {
  return [...known].find((p) => p === target || p.startsWith(target));
}

bootstrap().catch((err) => { console.error(err); process.exit(1); });
