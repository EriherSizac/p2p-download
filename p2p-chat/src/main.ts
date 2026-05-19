// File: MAIN — orquestación de capas + CLI (chat + llamadas)
// Created: 2026-05-13
// Updated: 2026-05-13
// Author: Erick Hernández Silva

/**
 * MAIN — orquestación de capas + CLI (chat + llamadas)
 * ----------------------------------------------------
 * Junta las 5 capas:
 *   1) Descubrimiento (UDP broadcast)
 *   2) Transporte    (TCP + pool)
 *   3) Framing       (length-prefix)
 *   4) Protocolo     (HELLO, CHAT, CHAT_ACK, PING/PONG, CALL_OFFER/ANSWER/ICE/END, BYE)
 *   5) Llamadas A/V  (WebRTC vía werift, señalizada por la 4)
 *
 * y expone una CLI sobre `readline`. Este proyecto es deliberadamente
 * independiente de `p2p-files`: comparte la estructura por capas pero no la
 * implementación.
 */

import readline from 'node:readline';
import { spawn as spawnChild } from 'node:child_process';
import { Discovery } from './discovery.js';
import { Transport } from './transport.js';
import {
  generatePeerId,
  newMessageId,
  newCallId,
  shortId,
  MSG,
  type Message,
} from './protocol.js';
import { createLogger } from './logger.js';
import { append as histAppend, tail as histTail, filePath as histPath } from './history.js';
import { Call } from './call.js';
import { isSupported as fwSupported, ruleExistsForPort, udpRuleExists, requestFirewallRule } from './firewall.js';
import { GraphServer, type GraphSnapshot } from './graph-server.js';

const log = createLogger('main');

// Puerto TCP fijo por defecto (41236). Razón: si fuese efímero, cada arranque
// cambiaría el puerto y habría que recrear la regla de firewall — molesto.
// Con un fijo, basta UNA regla inbound. Si necesitas varias instancias en la
// misma máquina, override con `TCP_PORT=<otro>` y crea otra regla.
const TCP_PORT = Number(process.env['TCP_PORT'] ?? 41236);
// Puerto distinto al de p2p-files (41234) para que ambos puedan coexistir
// en la misma LAN sin confundir sus enjambres.
const DISCOVERY_PORT = Number(process.env['DISCOVERY_PORT'] ?? 41235);

// Acuse de recibo: cuánto esperamos antes de declarar un CHAT como ✗ no entregado.
const CHAT_ACK_TIMEOUT_MS = 3_000;

// PING/PONG: con qué frecuencia medimos RTT por peer.
const PING_INTERVAL_MS = 5_000;
// Coeficiente del EWMA (media móvil exponencial). 0.2 = el nuevo valor pesa
// 20%, el histórico 80% → suaviza picos transitorios.
const RTT_EWMA_ALPHA = 0.2;

interface PendingAck {
  peerId: string;
  timer: NodeJS.Timeout;
  shortMsgId: string;
}

interface PeerLiveness {
  /** RTT estimado en ms (EWMA). undefined hasta el primer PONG. */
  rttMs?: number;
  /** PINGs enviados aún sin respuesta. Key=nonce, value=ts envío. */
  pendingPings: Map<number, number>;
}

/** Cuánto tiempo entre gossips de PEER_LIST (s). Bajo = grafo más fresco. */
const PEER_LIST_INTERVAL_MS = 15_000;

/**
 * Prompt sí/no en stdin sin readline persistente. Lo usamos antes de
 * arrancar la CLI principal para no chocar con su propio readline.
 * Cierra el interface al resolver para liberar stdin.
 */
function askYesNo(question: string): Promise<boolean> {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(question, (ans) => {
      rl.close();
      resolve(/^y(es)?$/i.test(ans.trim()));
    });
  });
}

async function bootstrap(): Promise<void> {
  const peerId = generatePeerId();
  log.info(`peerId = ${shortId(peerId)} (full=${peerId})`);

  const transport = new Transport({ peerId, tcpPort: TCP_PORT });
  const actualPort = await transport.start();

  // Firewall: si estamos en Windows y no hay regla inbound para nuestro
  // puerto, ofrecer crearla (UAC) ANTES de empezar a descubrir peers.
  // Evita que el primer round de conexiones muera por ETIMEDOUT.
  if (fwSupported()) {
    const tcpOk = await ruleExistsForPort(actualPort);
    const udpOk = await udpRuleExists();
    if (!tcpOk || !udpOk) {
      const missing: string[] = [];
      if (!tcpOk) missing.push(`TCP :${actualPort} (chat/señalización)`);
      if (!udpOk) missing.push('UDP node.exe (WebRTC/llamadas)');
      const yes = await askYesNo(
        `firewall: faltan reglas inbound → ${missing.join(' + ')}. ¿Crearlas? (pedirá UAC) [y/N] `,
      );
      if (yes) {
        log.info('solicitando elevación…');
        const created = await requestFirewallRule(actualPort, { udp: !udpOk });
        log.info(created ? 'reglas creadas' : 'no se crearon (UAC denegado o error)');
      } else {
        log.warn('firewall sin abrir; chat/llamadas pueden fallar.');
      }
    } else {
      log.debug(`firewall: reglas TCP :${actualPort} + UDP node.exe ya existen`);
    }
  }

  const discovery = new Discovery({ peerId, tcpPort: actualPort, discoveryPort: DISCOVERY_PORT });
  await discovery.start();

  // Sink que setupCli rellena al construir readline; usado desde handleMessage
  // para imprimir eventos sin romper el prompt.
  const cliSinks: {
    chat?: (from: string, text: string) => void;
    ack?: (messageId: string, from: string) => void;
    info?: (text: string) => void;
  } = {};

  // Tie-break lexicográfico: solo el peerId menor inicia conexión saliente,
  // así evitamos sockets duplicados cuando ambos se descubren a la vez.
  discovery.on('peer-up', (p) => {
    if (peerId < p.peerId) transport.connect(p.peerId, p.host, p.port);
  });

  // Estado para Ej2 (ACK con timeout) y Ej4 (RTT EWMA).
  const pendingAcks = new Map<string, PendingAck>();
  const liveness = new Map<string, PeerLiveness>();

  // Gossip de topología: lo que CADA peer remoto nos dice que conoce.
  // Clave = peerId del informante. Valor = peerIds que él conoce
  // (sus conexiones activas + nosotros). Con esto reconstruimos el grafo
  // global aunque solo estemos conectados directamente a un subconjunto.
  const peerLists = new Map<string, { peers: string[]; ts: number }>();

  // Estado de llamadas activas. Solo una concurrente por peer remoto (simple).
  const calls = new Map<string, Call>(); // callId → Call
  const callsByPeer = new Map<string, string>(); // peerId → callId

  // Envía nuestra lista de conectados al peer dado.
  const sendPeerList = (to: string): void => {
    transport.send(to, { type: MSG.PEER_LIST, peers: transport.connectedPeers() });
  };

  // En cuanto un peer entra al pool, mandamos PEER_LIST inicial — así el
  // grafo no espera al primer tick del timer.
  transport.on('peer-connected', (pid) => sendPeerList(pid));

  transport.on('peer-disconnected', (pid) => peerLists.delete(pid));

  // ─── Server HTTP del grafo en vivo ──────────────────────────────────
  // Se crea perezosamente cuando el usuario ejecuta `graph open` por
  // primera vez. Mantenemos la referencia aquí para suscribirle eventos
  // de transporte/gossip y empujar snapshots al navegador.
  let graphServer: GraphServer | undefined;

  /** Construye el snapshot actual del grafo en formato vis-network. */
  const buildSnapshot = (): GraphSnapshot => {
    const me = peerId;
    const adj = new Map<string, Set<string>>();
    adj.set(me, new Set(transport.connectedPeers()));
    for (const [pid, info] of peerLists) adj.set(pid, new Set(info.peers));

    const nodeSet = new Set<string>(adj.keys());
    for (const targets of adj.values()) for (const t of targets) nodeSet.add(t);

    const visNodes = [...nodeSet].map((pid) => {
      const liv = liveness.get(pid);
      const rttTip = liv?.rttMs !== undefined ? `\nRTT ≈ ${liv.rttMs.toFixed(0)} ms` : '';
      return {
        id: pid,
        label: shortId(pid) + (pid === me ? '\n(tú)' : ''),
        color: pid === me
          ? { background: '#2ecc71', border: '#27ae60' }
          : { background: '#3498db', border: '#2980b9' },
        size: pid === me ? 28 : 22,
        title: pid + rttTip,
      };
    });

    const edgeMap = new Map<string, { a: string; b: string; mutual: boolean }>();
    for (const [a, targets] of adj) {
      for (const b of targets) {
        const key = a < b ? `${a}|${b}` : `${b}|${a}`;
        const existing = edgeMap.get(key);
        if (existing) existing.mutual = true; // ya vimos b→a antes, ahora a→b
        else edgeMap.set(key, { a, b, mutual: adj.get(b)?.has(a) ?? false });
      }
    }
    const visEdges = [...edgeMap.entries()].map(([id, e]) => ({
      id,
      from: e.a,
      to: e.b,
      dashes: !e.mutual,
      color: { color: e.mutual ? '#27ae60' : '#7f8c8d' },
      width: e.mutual ? 2 : 1,
    }));

    const mutuals = [...edgeMap.values()].filter((e) => e.mutual).length;
    const total = nodeSet.size;
    const maxEdges = (total * (total - 1)) / 2;
    return {
      nodes: visNodes,
      edges: visEdges,
      stats: {
        total,
        mutuals,
        oneway: edgeMap.size - mutuals,
        density: maxEdges > 0 ? (mutuals / maxEdges).toFixed(2) : '0.00',
      },
      ts: Date.now(),
    };
  };

  /** Empuja snapshot si hay server activo con clientes — barato y seguro. */
  const pushSnapshot = (): void => {
    if (!graphServer || !graphServer.hasClients()) return;
    graphServer.emit(buildSnapshot());
  };

  // Cualquier cambio observable en la topología → empujar snapshot.
  transport.on('peer-connected', pushSnapshot);
  transport.on('peer-disconnected', pushSnapshot);

  /**
   * Arranca (si no está corriendo) el server HTTP del grafo en vivo, manda
   * un snapshot inicial y abre el navegador. Llamadas siguientes solo
   * reabren la página (el server sigue corriendo y suscrito a eventos).
   */
  const openLiveGraph = async (): Promise<string> => {
    if (!graphServer) {
      graphServer = new GraphServer();
      await graphServer.start();
    }
    // Snapshot inicial: el server lo entrega a clientes que se conecten.
    graphServer.emit(buildSnapshot());
    openInBrowser(graphServer.url());
    return graphServer.url();
  };

  // ---------------------------------------------------------------------
  // Manejo de mensajes entrantes.
  // ---------------------------------------------------------------------

  transport.on('message', (from, msg: Message) => {
    handleMessage(from, msg);
  });

  transport.on('peer-disconnected', (pid) => {
    liveness.delete(pid);
    // Si había una llamada activa con ese peer, cerrarla limpiamente.
    const cid = callsByPeer.get(pid);
    if (cid) {
      const call = calls.get(cid);
      if (call) void call.closeFromRemote('peer-disconnected');
    }
  });

  // Peer descubierto por UDP pero TCP no responde → casi seguro firewall.
  // Avisamos una sola vez por peer hasta que se conecte o expire en discovery,
  // para no inundar la consola en cada reintento del backoff.
  const unreachableShown = new Set<string>();
  transport.on('peer-connected', (pid) => unreachableShown.delete(pid));
  discovery.on('peer-down', (p) => unreachableShown.delete(p.peerId));
  transport.on('peer-unreachable', (info) => {
    if (unreachableShown.has(info.peerId)) return;
    unreachableShown.add(info.peerId);
    const sid = shortId(info.peerId);
    cliSinks.info?.(
      `\x1b[33m⚠ no se puede conectar a ${sid} @ ${info.host}:${info.port} (${info.code})\x1b[0m`,
    );
    cliSinks.info?.('   probable causa: firewall bloqueando TCP entrante en el peer remoto.');
    cliSinks.info?.('   en el host remoto, abre una PowerShell como administrador y ejecuta:');
    cliSinks.info?.(
      `   \x1b[36mNew-NetFirewallRule -DisplayName "p2p-chat" -Direction Inbound -Protocol TCP -LocalPort ${info.port} -Action Allow\x1b[0m`,
    );
    cliSinks.info?.('   verifica conectividad con:');
    cliSinks.info?.(`   \x1b[36mTest-NetConnection ${info.host} -Port ${info.port}\x1b[0m`);
  });

  function handleMessage(from: string, msg: Message): void {
    switch (msg.type) {
      case MSG.BYE:
        log.debug(`bye de ${shortId(from)}`);
        break;

      case MSG.CHAT: {
        // 1) Pintar en pantalla sin romper el prompt del readline.
        cliSinks.chat?.(from, msg.text);
        // 2) Acuse de recibo INMEDIATO. Tras esto el emisor verá su ✓.
        //    Si nuestro proceso muere antes de este send, el emisor verá ✗.
        transport.send(from, { type: MSG.CHAT_ACK, messageId: msg.messageId });
        // 3) Append al historial local (history.jsonl, una línea JSON).
        void histAppend({
          ts: msg.ts,
          dir: 'in',
          peerId: from,
          messageId: msg.messageId,
          text: msg.text,
        });
        break;
      }

      case MSG.CHAT_ACK: {
        // Buscamos el messageId en la tabla de envíos pendientes; si no
        // está, llegó tarde (ya saltó el timeout) o nunca lo enviamos.
        const pending = pendingAcks.get(msg.messageId);
        if (!pending) {
          log.debug(`ack ${msg.messageId} sin pendiente — ignorado`);
          break;
        }
        // Limpiamos el timer (evita pintar ✗ retrasado) y dibujamos ✓.
        clearTimeout(pending.timer);
        pendingAcks.delete(msg.messageId);
        cliSinks.ack?.(msg.messageId, from);
        break;
      }

      case MSG.PING:
        // Responder con PONG mismo nonce. NO añadimos timestamp aquí: el
        // RTT se mide en el lado que pingó (él sabe cuándo mandó cada nonce).
      
        transport.send(from, { type: MSG.PONG, nonce: msg.nonce });
        
        break;

      case MSG.PONG: {
        // Buscar el ts del envío correspondiente. RTT = ahora - ts.
        const liv = liveness.get(from);
        if (!liv) break;
        const sentAt = liv.pendingPings.get(msg.nonce);
        if (sentAt === undefined) break; // nonce desconocido (duplicado, etc.)
        liv.pendingPings.delete(msg.nonce);
        const rtt = Date.now() - sentAt;
        // EWMA — Exponentially Weighted Moving Average:
        //
        //   R[t] = α·x[t] + (1-α)·R[t-1]
        //
        // donde x[t] es la medida cruda y R[t] el RTT suavizado. Si lo
        // desarrollas hacia atrás:
        //
        //   R[t] = α·x[t] + α(1-α)·x[t-1] + α(1-α)²·x[t-2] + ...
        //
        // → cada medida pasada pesa con un factor que decae
        // geométricamente. Con α=0.2, la medida actual contribuye 20%,
        // la anterior 16%, la de antes 12.8%, etc. Es como una media
        // móvil con "memoria" suave, sin necesidad de guardar un buffer.
        //
        // Trade-off de α:
        //   α grande (→1)  → reactivo, ruidoso, sigue picos.
        //   α pequeño (→0) → estable, lento de responder a cambios reales.
        //   0.2 es un punto medio típico para RTT (TCP usa 0.125).
        //
        // Caso base (primera medida): no hay histórico → R = x crudo.
        liv.rttMs = liv.rttMs === undefined
          ? rtt
          : RTT_EWMA_ALPHA * rtt + (1 - RTT_EWMA_ALPHA) * liv.rttMs;
        break;
      }

      case MSG.PEER_LIST: {
        // Gossip de topología. El emisor nos manda los peers que él tiene
        // conectados; lo guardamos con timestamp para que `graph` pinte el
        // estado actual. No reenviamos (un solo hop) — el destinatario debe
        // pedir su propio gossip a sus vecinos para componer la vista completa.
        peerLists.set(from, { peers: msg.peers, ts: Date.now() });
        pushSnapshot();
        break;
      }

      case MSG.CALL_OFFER: {
        // Política simple: una sola llamada concurrente por peer remoto.
        // Si ya estamos hablando con ese peer, devolvemos CALL_END(busy).
        if (callsByPeer.has(from)) {
          transport.send(from, { type: MSG.CALL_END, callId: msg.callId, reason: 'busy' });
          break;
        }
        // No construimos el Call todavía: el usuario debe decidir si
        // `answer` o `hangup`. Mientras tanto, guardamos la oferta.
        cliSinks.info?.(`\x1b[33m📞 llamada entrante de ${shortId(from)}\x1b[0m`);
        cliSinks.info?.('   usa `answer` para aceptar o `hangup` para rechazar');
        pendingIncomingOffer = { callId: msg.callId, from, sdp: msg.sdp };
        break;
      }

      case MSG.CALL_ANSWER: {
        // El caller espera la answer; se la entregamos al Call activo.
        const call = calls.get(msg.callId);
        if (!call) {
          log.warn(`CALL_ANSWER para callId desconocido: ${msg.callId}`);
          break;
        }
        void call.onAnswer(msg.sdp);
        break;
      }

      case MSG.CALL_ICE: {
        // Cada CALL_ICE = un candidato remoto. Lo aplicamos al PC; werift
        // lo probará contra los nuestros locales (trickle ICE).
        const call = calls.get(msg.callId);
        if (!call) break;
        void call.onIce(msg.candidate);
        break;
      }

      case MSG.CALL_END: {
        // Cierre limpio. Puede llegar antes de aceptar (cancela la oferta)
        // o durante la llamada (hangup remoto).
        const call = calls.get(msg.callId);
        if (call) void call.closeFromRemote(msg.reason);
        if (pendingIncomingOffer && pendingIncomingOffer.callId === msg.callId) {
          cliSinks.info?.(`📞 ${shortId(from)} canceló la llamada (${msg.reason ?? '?'})`);
          pendingIncomingOffer = undefined;
        }
        break;
      }

      default:
        break;
    }
  }

  // Oferta entrante pendiente de aceptación (un peer puede tener solo una).
  let pendingIncomingOffer: { callId: string; from: string; sdp: string } | undefined;

  // ---------------------------------------------------------------------
  // Timer de PING/PONG: por cada peer conectado, mandar un PING y dejarlo
  // pendiente hasta el PONG. Si el peer no responde, el nonce se queda en
  // pendingPings; no hace daño, el siguiente ciclo machaca su entrada.
  // ---------------------------------------------------------------------

  // Nonce monotónico global. No hace falta que sea único por peer porque
  // cada peer guarda sus propios envíos pendientes en su `liveness`.
  let nextNonce = 1;
  const pingTimer = setInterval(() => {
    // Para cada peer conectado: registra ts de envío y manda PING.
    // Si el peer no contesta antes del siguiente ciclo, no pasa nada —
    // el nonce sin respuesta se queda en pendingPings y eventualmente
    // se reemplaza. No marcamos al peer como muerto desde aquí; la fuente
    // de verdad para "está vivo" sigue siendo discovery (UDP timeout).
    for (const pid of transport.connectedPeers()) {
      let liv = liveness.get(pid);
      if (!liv) {
        // Primera vez que pingueamos a este peer → crear su entrada.
        liv = { pendingPings: new Map() };
        liveness.set(pid, liv);
      }
      const nonce = nextNonce++;
      liv.pendingPings.set(nonce, Date.now());
      transport.send(pid, { type: MSG.PING, nonce });
    }
  }, PING_INTERVAL_MS);

  // Timer de gossip PEER_LIST. Lo separamos del PING para que la frecuencia
  // pueda ser distinta: PING es liveness fina (5s); el grafo cambia menos.
  const peerListTimer = setInterval(() => {
    for (const pid of transport.connectedPeers()) sendPeerList(pid);
  }, PEER_LIST_INTERVAL_MS);

  // ---------------------------------------------------------------------
  // Helper de llamadas: monta el cableado entre Call y transport.
  // ---------------------------------------------------------------------

  function spawnCall(opts: { remotePeerId: string; callId: string; role: 'caller' | 'callee'; source?: string }): Call {
    // El Call NO conoce el transport — esto es deliberado. Le pasamos un
    // callback `signal()` y él decide qué necesita mandar (offer/answer/
    // ice/end). Aquí lo traducimos a mensajes concretos del protocolo.
    // Si mañana cambias el medio de señalización (Slack, WebSocket, ...)
    // solo este `signal` hay que tocar; call.ts no se entera.
    const call = new Call({
      callId: opts.callId,
      remotePeerId: opts.remotePeerId,
      role: opts.role,
      source: opts.source,
      // Permite desactivar ffplay (CI / sin altavoces) sin tocar código.
      playback: process.env['CALL_PLAYBACK'] !== '0',
      signal: (m) => {
        if (m.type === 'offer') transport.send(opts.remotePeerId, { type: MSG.CALL_OFFER, callId: opts.callId, sdp: m.sdp });
        else if (m.type === 'answer') transport.send(opts.remotePeerId, { type: MSG.CALL_ANSWER, callId: opts.callId, sdp: m.sdp });
        else if (m.type === 'ice') transport.send(opts.remotePeerId, { type: MSG.CALL_ICE, callId: opts.callId, candidate: m.candidate });
        else if (m.type === 'end') transport.send(opts.remotePeerId, { type: MSG.CALL_END, callId: opts.callId, reason: m.reason });
      },
    });
    // Doble índice: por callId (mensajes entrantes) y por peerId (UI / dedupe).
    calls.set(opts.callId, call);
    callsByPeer.set(opts.remotePeerId, opts.callId);

    // Eventos del Call → UI. El Call emite; aquí decidimos cómo se pinta.
    call.on('state', (s) => {
      cliSinks.info?.(`📞 llamada ${opts.callId} → ${s}`);
    });
    call.on('ended', (reason) => {
      // Limpiar índices al cerrar para que un futuro `call <peer>` funcione.
      calls.delete(opts.callId);
      callsByPeer.delete(opts.remotePeerId);
      cliSinks.info?.(`📞 llamada ${opts.callId} terminada (${reason ?? '?'})`);
    });
    return call;
  }

  setupCli({
    discovery,
    transport,
    peerId,
    sinks: cliSinks,
    state: { pendingAcks, liveness, calls, callsByPeer, peerLists },
    openLiveGraph,
    pending: {
      get incomingOffer() { return pendingIncomingOffer; },
      clear() { pendingIncomingOffer = undefined; },
    },
    spawnCall,
  });

  const shutdown = (): void => {
    log.info('cerrando…');
    clearInterval(pingTimer);
    clearInterval(peerListTimer);
    graphServer?.stop();
    for (const call of calls.values()) void call.closeFromRemote('shutdown');
    transport.broadcast({ type: MSG.BYE });
    discovery.stop();
    transport.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

interface CliCtx {
  discovery: Discovery;
  transport: Transport;
  peerId: string;
  sinks: {
    chat?: (from: string, text: string) => void;
    ack?: (messageId: string, from: string) => void;
    info?: (text: string) => void;
  };
  state: {
    pendingAcks: Map<string, PendingAck>;
    liveness: Map<string, PeerLiveness>;
    calls: Map<string, Call>;
    callsByPeer: Map<string, string>;
    peerLists: Map<string, { peers: string[]; ts: number }>;
  };
  pending: {
    readonly incomingOffer: { callId: string; from: string; sdp: string } | undefined;
    clear(): void;
  };
  spawnCall(opts: { remotePeerId: string; callId: string; role: 'caller' | 'callee'; source?: string }): Call;
  openLiveGraph(): Promise<string>;
}

function setupCli(ctx: CliCtx): void {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.setPrompt('chat> ');

  const out = (s: string): void => {
    process.stdout.write(s.endsWith('\n') ? s : s + '\n');
  };

  const refresh = (line: string): void => {
    process.stdout.write('\r\x1b[K');
    process.stdout.write(line);
    if (!line.endsWith('\n')) process.stdout.write('\n');
    rl.prompt(true);
  };

  const printMenu = (): void => {
    out('');
    out('  ┌─ menú · p2p-chat ────────────────────────────────────────┐');
    out('  │  chat <texto>             difunde mensaje a todos        │');
    out('  │  msg <peerId> <texto>     mensaje directo (con ACK ✓/✗)  │');
    out('  │  history [n]              últimos n mensajes (default 20)│');
    out('  │  peers                    peers descubiertos + RTT       │');
    out('  │  who                      peers conectados + RTT         │');
    out('  │  graph [open]             grafo ASCII | live HTML+SSE    │');
    out('  │  call <peerId> [source]   llamar (source=tone|mic|file:…)│');
    out('  │  answer                   aceptar llamada entrante       │');
    out('  │  hangup                   colgar llamada activa          │');
    out('  │  stats                    diag rx (paquetes/bytes) llam.  │');
    out('  │  firewall                 abrir regla inbound (UAC)      │');
    out('  │  menu | help | quit                                      │');
    out('  └──────────────────────────────────────────────────────────┘');
  };

  ctx.sinks.chat = (from, text) => {
    refresh(`\x1b[35m[chat][${shortId(from)}]\x1b[0m ${text}`);
  };
  ctx.sinks.ack = (messageId) => {
    refresh(`\x1b[32m✓\x1b[0m ${messageId.slice(0, 8)} entregado`);
  };
  ctx.sinks.info = (text) => {
    refresh(text);
  };

  printMenu();
  rl.prompt();

  const resolvePeer = (prefix: string): string | undefined => {
    const all = ctx.discovery.list().map((p) => p.peerId);
    const matches = all.filter((id) => id.startsWith(prefix));
    return matches.length === 1 ? matches[0] : undefined;
  };

  const fmtRtt = (peerId: string): string => {
    const liv = ctx.state.liveness.get(peerId);
    if (!liv || liv.rttMs === undefined) return '—';
    return `${liv.rttMs.toFixed(1)} ms`;
  };

  /**
   * Envío de un CHAT directo con tracking de ACK.
   *
   * Patrón general:
   *   1) Generar un messageId único (correlaciona CHAT con su ACK).
   *   2) `transport.send` devuelve false si no hay socket → fallo inmediato.
   *   3) Persistir en historial sin esperar al ACK (es nuestra copia).
   *   4) Armar un timer: si dispara antes del ACK, pinta ✗.
   *   5) Si llega el ACK (en handleMessage), se limpia el timer y pinta ✓.
   */
  const sendChat = (toPeerId: string, text: string): void => {
    const messageId = newMessageId();
    const ok = ctx.transport.send(toPeerId, {
      type: MSG.CHAT,
      messageId,
      text,
      ts: Date.now(),
    });
    if (!ok) {
      // Fallo "inmediato": el send no encontró conexión. No tiene sentido
      // esperar ACK porque ni siquiera salió de aquí.
      out(`✗ no entregado: sin conexión a ${shortId(toPeerId)}`);
      return;
    }
    void histAppend({ ts: Date.now(), dir: 'out', peerId: toPeerId, messageId, text });
    // Timer "ACK o muerte". Si el peer remoto procesa el CHAT y responde
    // a tiempo, handleMessage(CHAT_ACK) limpiará este timer. Si no, salta
    // y limpiamos pendingAcks nosotros mismos para evitar fugas.
    const timer = setTimeout(() => {
      ctx.state.pendingAcks.delete(messageId);
      refresh(`\x1b[31m✗\x1b[0m ${messageId.slice(0, 8)} no entregado (timeout)`);
    }, CHAT_ACK_TIMEOUT_MS);
    ctx.state.pendingAcks.set(messageId, { peerId: toPeerId, timer, shortMsgId: messageId.slice(0, 8) });
  };

  rl.on('line', (line) => {
    const [cmd, ...rest] = line.trim().split(/\s+/);
    void (async () => {
      try {
        switch (cmd) {
          case '':
            break;

          case 'peers': {
            const list = ctx.discovery.list();
            if (list.length === 0) { out('(no hay peers descubiertos)'); break; }
            for (const p of list) {
              const conn = ctx.transport.isConnected(p.peerId) ? 'conectado' : 'descubierto';
              out(`  ${shortId(p.peerId)}  ${p.host}:${p.port}  ${conn}  rtt=${fmtRtt(p.peerId)}`);
            }
            break;
          }

          case 'who': {
            const ids = ctx.transport.connectedPeers();
            if (ids.length === 0) { out('(sin peers conectados)'); break; }
            for (const id of ids) out(`  ${shortId(id)}  rtt=${fmtRtt(id)}`);
            break;
          }

          case 'chat': {
            const text = rest.join(' ');
            if (!text) { out('uso: chat <texto>'); break; }
            const connected = ctx.transport.connectedPeers();
            if (connected.length === 0) { out('no hay peers conectados'); break; }
            // Broadcast: enviamos por separado a cada peer para que cada uno
            // genere su propio ACK; el messageId se comparte entre todos.
            const messageId = newMessageId();
            const ts = Date.now();
            for (const id of connected) {
              ctx.transport.send(id, { type: MSG.CHAT, messageId, text, ts });
            }
            void histAppend({ ts, dir: 'out', peerId: 'broadcast', messageId, text });
            out(`(chat) → ${connected.length} peer(s)`);
            break;
          }

          case 'msg': {
            const prefix = rest[0];
            const text = rest.slice(1).join(' ');
            if (!prefix || !text) { out('uso: msg <peerId> <texto>'); break; }
            const target = resolvePeer(prefix);
            if (!target) { out(`peerId ambiguo o desconocido: ${prefix}`); break; }
            if (!ctx.transport.isConnected(target)) { out(`no hay conexión activa con ${shortId(target)}`); break; }
            sendChat(target, text);
            break;
          }

          case 'history': {
            const n = Number(rest[0] ?? 20);
            const recs = await histTail(Number.isFinite(n) && n > 0 ? n : 20);
            if (recs.length === 0) { out(`(historial vacío — ${histPath()})`); break; }
            for (const r of recs) {
              const arrow = r.dir === 'out' ? '→' : '←';
              const who = r.peerId === 'broadcast' ? '*all*' : shortId(r.peerId);
              const t = new Date(r.ts).toISOString().slice(11, 19);
              out(`  ${t} ${arrow} ${who}  ${r.text}`);
            }
            break;
          }

          case 'call': {
            const prefix = rest[0];
            const source = rest[1] ?? 'tone';
            if (!prefix) { out('uso: call <peerId> [tone|mic|mic:<spec>|file:<ruta>]'); break; }
            const target = resolvePeer(prefix);
            if (!target) { out(`peerId ambiguo o desconocido: ${prefix}`); break; }
            if (!ctx.transport.isConnected(target)) { out(`no hay conexión activa con ${shortId(target)}`); break; }
            if (ctx.state.callsByPeer.has(target)) { out(`ya hay una llamada con ${shortId(target)}`); break; }
            const callId = newCallId();
            const call = ctx.spawnCall({ remotePeerId: target, callId, role: 'caller', source });
            await call.start();
            out(`📞 llamando a ${shortId(target)} (callId=${callId}, fuente=${source})`);
            break;
          }

          case 'answer': {
            const offer = ctx.pending.incomingOffer;
            if (!offer) { out('no hay llamadas entrantes pendientes'); break; }
            if (ctx.state.callsByPeer.has(offer.from)) { out(`ya hay una llamada con ${shortId(offer.from)}`); break; }
            const source = rest[0] ?? 'tone';
            const call = ctx.spawnCall({ remotePeerId: offer.from, callId: offer.callId, role: 'callee', source });
            ctx.pending.clear();
            await call.accept(offer.sdp);
            out(`📞 aceptada (callId=${offer.callId}, fuente=${source})`);
            break;
          }

          case 'graph': {
            // `graph`           → ASCII bonito en la terminal
            // `graph open|live` → HTML interactivo en el navegador con
            //                     actualizaciones en vivo vía SSE
            const sub = rest[0];
            if (sub === 'open' || sub === 'live' || sub === 'html') {
              const url = await ctx.openLiveGraph();
              out(`📊 grafo en vivo: ${url}`);
              out('   se actualiza automáticamente cuando entran/salen peers');
            } else {
              renderGraph(out, ctx);
            }
            break;
          }

          case 'firewall': {
            if (!fwSupported()) { out('firewall: solo Windows'); break; }
            const port = ctx.transport.port();
            const tcpOk = await ruleExistsForPort(port);
            const udpOk = await udpRuleExists();
            if (tcpOk && udpOk) { out(`firewall: TCP :${port} + UDP node.exe ya existen`); break; }
            out(`firewall: solicitando UAC (TCP :${port}${!udpOk ? ' + UDP node.exe' : ''})…`);
            const ok = await requestFirewallRule(port, { udp: !udpOk });
            out(ok ? 'firewall: reglas creadas' : 'firewall: no se crearon');
            break;
          }

          case 'stats': {
            // Diagnóstico de llamada(s) activa(s). Si los paquetes/bytes
            // crecen pero no oyes nada → problema local (volumen, output
            // device de Windows, ffplay sin SDL audio). Si están a 0 →
            // problema de red/ICE/decoder.
            if (ctx.state.calls.size === 0) { out('(no hay llamadas activas)'); break; }
            for (const call of ctx.state.calls.values()) {
              const s = call.getRxStats();
              const rttLiv = ctx.state.liveness.get(call.remotePeerId);
              const rtt = rttLiv?.rttMs !== undefined ? `${rttLiv.rttMs.toFixed(0)}ms` : '—';
              out(`  callId=${call.callId}  estado=${call.getState()}  peer=${shortId(call.remotePeerId)}`);
              out(`    rx: ${s.packets} paquetes · ${s.bytes} bytes · rtt(señal)=${rtt}`);
              if (s.packets === 0) {
                out('    ⚠ 0 paquetes recibidos — revisa firewall UDP y mic del peer remoto');
              }
            }
            break;
          }

          case 'hangup': {
            if (ctx.pending.incomingOffer) {
              const o = ctx.pending.incomingOffer;
              ctx.transport.send(o.from, { type: MSG.CALL_END, callId: o.callId, reason: 'rejected' });
              ctx.pending.clear();
              out('📞 llamada entrante rechazada');
              break;
            }
            if (ctx.state.calls.size === 0) { out('no hay llamadas activas'); break; }
            for (const call of ctx.state.calls.values()) {
              await call.hangup('user');
            }
            break;
          }

          case 'menu':
            printMenu();
            break;
          case 'quit':
          case 'exit':
            rl.close();
            process.kill(process.pid, 'SIGINT');
            return;
          case 'help':
            out('comandos: peers | who | graph [open] | chat <texto> | msg <peerId> <texto> | history [n] | call <peerId> [source] | answer | hangup | stats | firewall | menu | quit');
            break;
          default:
            out(`comando desconocido: ${cmd}`);
        }
      } catch (err) {
        out(`error: ${(err as Error).message}`);
      }
      rl.prompt();
    })();
  });
}

/**
 * Construye el grafo lógico a partir del estado actual.
 *
 * Datos disponibles:
 *   - Nosotros (`ctx.peerId`) → conocemos a `transport.connectedPeers()`.
 *   - Cada peer remoto nos manda PEER_LIST (sus vecinos) → `peerLists`.
 *
 * Producimos:
 *   - `nodes`: todos los peerIds vistos.
 *   - `edges`: aristas únicas (clave canónica a|b) con flag `mutual`.
 */
function buildGraph(ctx: CliCtx): {
  nodes: string[];
  edges: Array<{ a: string; b: string; mutual: boolean }>;
  adj: Map<string, Set<string>>;
} {
  const me = ctx.peerId;
  const adj = new Map<string, Set<string>>();
  adj.set(me, new Set(ctx.transport.connectedPeers()));
  for (const [pid, info] of ctx.state.peerLists) {
    adj.set(pid, new Set(info.peers));
  }
  const nodeSet = new Set<string>(adj.keys());
  for (const targets of adj.values()) for (const t of targets) nodeSet.add(t);

  const edges: Array<{ a: string; b: string; mutual: boolean }> = [];
  const seen = new Set<string>();
  for (const [a, targets] of adj) {
    for (const b of targets) {
      const key = a < b ? `${a}|${b}` : `${b}|${a}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const mutual = adj.get(b)?.has(a) ?? false;
      edges.push({ a, b, mutual });
    }
  }
  return { nodes: [...nodeSet].sort(), edges, adj };
}

/**
 * Render ASCII bonito con colores ANSI:
 *
 *   ●  nodo "tú"        (verde)
 *   ○  otros peers      (cian)
 *   ━━ arista mutua     (verde claro, sólido)
 *   ┄┄ arista un sentido (gris, discontinuo)
 *
 * Diseño en dos partes:
 *   1) Diagrama tipo "estrella" centrado en tu peer con sus vecinos
 *      directos y RTT — la información más práctica para el usuario.
 *   2) Tabla completa de aristas (origen, destino, tipo).
 */
function renderGraph(out: (s: string) => void, ctx: CliCtx): void {
  const { nodes, edges, adj } = buildGraph(ctx);
  if (nodes.length <= 1) { out('(sin peers conocidos)'); return; }
  const me = ctx.peerId;

  const C = {
    reset: '\x1b[0m',
    bold: '\x1b[1m',
    dim: '\x1b[2m',
    green: '\x1b[32m',
    cyan: '\x1b[36m',
    yellow: '\x1b[33m',
    gray: '\x1b[90m',
  };
  const dot = (pid: string): string =>
    pid === me ? `${C.green}${C.bold}●${C.reset}` : `${C.cyan}○${C.reset}`;
  const name = (pid: string): string => {
    const s = shortId(pid);
    return pid === me ? `${C.green}${C.bold}${s}${C.reset}` : `${C.cyan}${s}${C.reset}`;
  };
  const fmtRtt = (pid: string): string => {
    const liv = ctx.state.liveness.get(pid);
    if (!liv || liv.rttMs === undefined) return `${C.dim}—${C.reset}`;
    const ms = liv.rttMs;
    const color = ms < 30 ? C.green : ms < 150 ? C.yellow : C.gray;
    return `${color}${ms.toFixed(0).padStart(3)}ms${C.reset}`;
  };

  // ── Parte 1: estrella centrada en TÚ ─────────────────────────────
  const myNeighbors = [...(adj.get(me) ?? [])].sort();
  out('');
  out(`  ${C.bold}grafo de conectividad${C.reset}  ${C.dim}(visto desde ti)${C.reset}`);
  out('');
  if (myNeighbors.length === 0) {
    out(`            ${dot(me)} ${name(me)}    ${C.dim}(sin vecinos)${C.reset}`);
  } else {
    // Pinto un "abanico" con líneas saliendo del centro.
    const radius = Math.min(myNeighbors.length, 6);
    out(`            ${dot(me)} ${name(me)}`);
    for (let i = 0; i < myNeighbors.length; i++) {
      const peer = myNeighbors[i]!;
      const mutual = adj.get(peer)?.has(me) ?? false;
      const line = mutual ? `${C.green}━━━${C.reset}` : `${C.gray}┄┄┄${C.reset}`;
      // último elemento usa esquina diferente
      const branch = i === myNeighbors.length - 1 ? '└─' : '├─';
      out(`             ${C.gray}${branch}${C.reset}${line} ${dot(peer)} ${name(peer)}   rtt=${fmtRtt(peer)}`);
    }
    void radius;
  }

  // ── Parte 2: tabla de aristas global ─────────────────────────────
  const otherEdges = edges.filter((e) => e.a !== me && e.b !== me);
  if (otherEdges.length > 0) {
    out('');
    out(`  ${C.bold}aristas entre otros peers (vía gossip)${C.reset}`);
    for (const e of otherEdges) {
      const line = e.mutual ? `${C.green}━━━${C.reset}` : `${C.gray}┄┄┄${C.reset}`;
      out(`    ${dot(e.a)} ${name(e.a)} ${line} ${dot(e.b)} ${name(e.b)}`);
    }
  }

  // ── Parte 3: métricas ────────────────────────────────────────────
  const mutuals = edges.filter((e) => e.mutual).length;
  const oneway = edges.length - mutuals;
  const n = nodes.length;
  const maxEdges = (n * (n - 1)) / 2;
  const density = maxEdges > 0 ? (mutuals / maxEdges).toFixed(2) : '0.00';
  out('');
  out(
    `  ${C.dim}nodos=${n}  mutuas=${mutuals}  unidireccionales=${oneway}  densidad=${density}${C.reset}`,
  );
  out(
    `  ${C.dim}leyenda: ${C.green}━━━${C.dim} mutua   ${C.gray}┄┄┄${C.dim} un sentido (gossip parcial)${C.reset}`,
  );
  out(`  ${C.dim}tip: \`graph open\` para verlo en el navegador${C.reset}`);
  out('');
}

/**
 * Abre una URL/fichero local en el navegador por defecto. Cross-platform:
 *   - Windows: `cmd /c start "" <file>`  ("" es el título obligatorio
 *              cuando hay un argumento con espacios; sin él, start trata el
 *              primer arg como título y no abre nada).
 *   - macOS:   `open <file>`
 *   - Linux:   `xdg-open <file>`
 */
function openInBrowser(file: string): void {
  if (process.platform === 'win32') {
    spawnDetached('cmd', ['/c', 'start', '""', file]);
  } else if (process.platform === 'darwin') {
    spawnDetached('open', [file]);
  } else {
    spawnDetached('xdg-open', [file]);
  }
}

function spawnDetached(cmd: string, args: string[]): void {
  // detached + unref para que el chat siga andando si el navegador tarda.
  try {
    const child = spawnChild(cmd, args, { detached: true, stdio: 'ignore' });
    child.unref();
  } catch {
    /* no-op: el usuario verá la ruta en stdout y puede abrirla a mano */
  }
}

bootstrap().catch((err) => {
  log.error('bootstrap falló', err);
  process.exit(1);
});
