// File: CAPA 5 — LLAMADAS A/V (WebRTC)
// Created: 2026-05-13
// Updated: 2026-05-13
// Author: Erick Hernández Silva

/**
 * CAPA 5 — LLAMADAS A/V (WebRTC)
 * ------------------------------
 * Problema: TCP es excelente para mensajería (fiable, ordenado) pero pésimo
 * para audio/vídeo en tiempo real: una sola retransmisión añade decenas de
 * milisegundos audibles. Para llamadas queremos UDP con SRTP, NAT traversal
 * con ICE/STUN y control de jitter — todo eso es WebRTC.
 *
 * Implementarlo a mano es una vida entera. Aquí usamos **werift**, una
 * implementación pura de WebRTC para Node (sin bindings nativos). werift
 * hace el trabajo "duro":
 *
 *   - ICE: descubre candidatos (host, srflx vía STUN), prueba pares,
 *     elige el camino UDP que de verdad funcione entre los dos peers.
 *   - DTLS: handshake en cleartext sobre UDP para acordar claves SRTP.
 *   - SRTP: cifra cada paquete RTP con la clave acordada.
 *
 * Nosotros aportamos dos cosas:
 *
 *   1) **Señalización**: WebRTC necesita un canal externo para que ambos
 *      peers intercambien SDP (descripción de la sesión) y candidatos ICE.
 *      Aquí ese canal es el TCP del p2p-chat que ya tenemos: enviamos
 *      `CALL_OFFER`, `CALL_ANSWER`, `CALL_ICE` por él. Cuando la conexión
 *      ICE/DTLS se ha establecido, el audio fluye **directamente** UDP a
 *      UDP entre los peers — el TCP solo sirvió para "presentarlos".
 *
 *   2) **Captura/reproducción**: werift maneja RTP, no micrófonos. Usamos
 *      `ffmpeg` como subproceso: ffmpeg captura del mic, encodea Opus, y
 *      escribe paquetes RTP a un socket UDP local; nosotros leemos cada
 *      datagrama y se lo pasamos a `track.writeRtp(buf)`. En la otra
 *      punta, `track.onReceiveRtp` nos entrega los paquetes; los
 *      reescribimos a otro socket UDP local que un `ffplay` está
 *      escuchando, descritos por un SDP que generamos al vuelo.
 *
 *      Diagrama:
 *
 *        mic ──► ffmpeg ──RTP/UDP local──► werift ──SRTP/UDP─► PEER REMOTO
 *                                          (cifra, ICE)         ┃
 *                                                                ▼
 *                                                   werift remoto descifra
 *                                                                ┃
 *        speakers ◄── ffplay ◄──RTP/UDP local── werift remoto ───┘
 *
 * 💡 Nota didáctica: la separación entre **señalización** y **media** es la
 * idea más importante de WebRTC. Cualquier canal sirve para señalizar
 * (HTTP, WebSocket, una paloma mensajera); aquí usamos nuestro propio TCP
 * P2P porque ya lo tenemos. El audio nunca pasa por él.
 */

import dgram from 'node:dgram';
import { spawn, type ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { writeFileSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  RTCPeerConnection,
  RTCRtpCodecParameters,
  MediaStreamTrack,
  type RTCIceCandidate,
} from 'werift';
import { createLogger } from './logger.js';
import { newCallId, type IceCandidatePayload } from './protocol.js';

const log = createLogger('call');

// ---------------------------------------------------------------------------
// Configuración fija.
// ---------------------------------------------------------------------------

/** Servidores STUN públicos. Solo se usan para descubrir nuestra IP pública
 *  (candidatos srflx). NO retransmiten media; eso sería TURN, que aquí no
 *  habilitamos para mantener la demo simple. Si ambos peers están detrás de
 *  NATs simétricos sin TURN, la llamada fallará — es una limitación
 *  conocida y didáctica (ver docs/NAT.md). */
const STUN_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

/** Codec único negociado: Opus a 48kHz estéreo, payload type 96. */
const OPUS_PT = 96;
const opusCodec = new RTCRtpCodecParameters({
  mimeType: 'audio/opus',
  clockRate: 48000,
  channels: 2,
  payloadType: OPUS_PT,
});

// ---------------------------------------------------------------------------
// Fuentes de media: comandos ffmpeg cross-platform.
// ---------------------------------------------------------------------------

/**
 * Devuelve los args de entrada de ffmpeg para capturar la fuente solicitada.
 * `target` es el host:port donde escribir RTP (el bridge UDP local).
 */
function buildFfmpegSenderArgs(source: string, target: { host: string; port: number }): string[] {
  // Detección de plataforma para mic real. Si no, fallback a tono sintético
  // (`tone`) o a un fichero (`file:<ruta>`).
  let inputArgs: string[];
  if (source === 'tone') {
    // Tono sinusoidal de 440 Hz, 48 kHz estéreo. Sirve para validar la
    // tubería sin necesidad de mic — útil en CI/clase.
    inputArgs = ['-re', '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000'];
  } else if (source.startsWith('file:')) {
    const file = source.slice('file:'.length);
    inputArgs = ['-re', '-i', file];
  } else if (source === 'mic') {
    inputArgs = micInputArgs();
  } else {
    // Permite override directo: cualquier cadena se pasa tal cual a ffmpeg.
    // Útil en Windows: `mic:audio=Microphone (Realtek...)`.
    if (source.startsWith('mic:')) {
      const spec = source.slice('mic:'.length);
      inputArgs = micInputArgs(spec);
    } else {
      throw new Error(`fuente desconocida: ${source} (usa tone | mic | mic:<spec> | file:<ruta>)`);
    }
  }

  return [
    ...inputArgs,
    '-ac', '2',
    '-ar', '48000',
    '-c:a', 'libopus',
    '-b:a', '64k',
    '-application', 'voip',
    '-payload_type', String(OPUS_PT),
    '-f', 'rtp',
    `rtp://${target.host}:${target.port}`,
    '-loglevel', 'warning',
  ];
}

function micInputArgs(spec?: string): string[] {
  switch (process.platform) {
    case 'darwin':
      // ":0" = primer dispositivo de entrada de audio (default mic).
      return ['-f', 'avfoundation', '-i', spec ?? ':0'];
    case 'linux':
      return ['-f', 'pulse', '-i', spec ?? 'default'];
    case 'win32':
      // DirectShow no tiene "default". El usuario DEBE pasar el nombre del
      // dispositivo: `mic:audio=Microphone (Realtek...)`.
      if (!spec) {
        throw new Error(
          'En Windows necesitas especificar el dispositivo: ' +
            '`call <peer> --source mic:audio=<nombre>`. Lista con `ffmpeg -list_devices true -f dshow -i dummy`.',
        );
      }
      return ['-f', 'dshow', '-i', spec];
    default:
      throw new Error(`mic no soportado en plataforma: ${process.platform}`);
  }
}

/**
 * Construye el SDP que ffplay leerá para reproducir el RTP entrante.
 * Es un fichero de texto que describe "voy a recibir Opus por este puerto".
 */
function buildReceiverSdp(localPort: number): string {
  return [
    'v=0',
    'o=- 0 0 IN IP4 127.0.0.1',
    's=p2p-chat',
    'c=IN IP4 127.0.0.1',
    't=0 0',
    `m=audio ${localPort} RTP/AVP ${OPUS_PT}`,
    `a=rtpmap:${OPUS_PT} opus/48000/2`,
    '',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// Una llamada.
// ---------------------------------------------------------------------------

export type CallRole = 'caller' | 'callee';
export type CallState = 'idle' | 'signaling' | 'connecting' | 'active' | 'ended';

export interface CallOpts {
  callId: string;
  remotePeerId: string;
  role: CallRole;
  /** Fuente de audio para el sender (caller). Solo aplica si role==='caller'
   *  o si el callee también quiere enviar (full-duplex). Default: 'tone'. */
  source?: string;
  /** Si false, no se intenta lanzar ffplay; los paquetes recibidos solo se
   *  cuentan. Útil cuando no hay altavoces (CI). Default: true. */
  playback?: boolean;
  /** Callback para enviar mensajes de señalización al peer remoto.
   *  La capa main.ts conecta esto al transport. */
  signal: (msg:
    | { type: 'offer'; sdp: string }
    | { type: 'answer'; sdp: string }
    | { type: 'ice'; candidate: IceCandidatePayload | null }
    | { type: 'end'; reason?: string }) => void;
}

export declare interface Call {
  on(event: 'state', listener: (s: CallState) => void): this;
  on(event: 'rx-rtp', listener: (bytes: number) => void): this;
  on(event: 'ended', listener: (reason?: string) => void): this;
}

export class Call extends EventEmitter {
  readonly callId: string;
  readonly remotePeerId: string;
  readonly role: CallRole;

  private pc!: RTCPeerConnection;
  private localTrack?: MediaStreamTrack;
  private state: CallState = 'idle';

  private senderFfmpeg?: ChildProcess;
  private senderBridge?: dgram.Socket;
  private senderBridgePort = 0;

  private playerFfplay?: ChildProcess;
  private playerBridge?: dgram.Socket;
  private playerBridgePort = 0;
  private playerSdpPath?: string;

  private rxBytes = 0;
  private rxPackets = 0;
  private readonly opts: CallOpts;

  constructor(opts: CallOpts) {
    super();
    this.callId = opts.callId;
    this.remotePeerId = opts.remotePeerId;
    this.role = opts.role;
    this.opts = opts;
  }

  // -------------------------------------------------------------------------
  // API pública.
  // -------------------------------------------------------------------------

  /** Lanza la llamada (solo caller). Crea PC, addTrack, envía CALL_OFFER. */
  async start(): Promise<void> {
    if (this.role !== 'caller') throw new Error('start() solo en caller');
    this.setupPeerConnection();
    await this.setupSender();
    this.setState('signaling');

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.opts.signal({ type: 'offer', sdp: offer.sdp });
  }

  /** Aceptar una oferta entrante (callee). Crea PC, addTrack, responde. */
  async accept(remoteSdp: string): Promise<void> {
    if (this.role !== 'callee') throw new Error('accept() solo en callee');
    this.setupPeerConnection();
    await this.setupSender();
    this.setState('signaling');

    await this.pc.setRemoteDescription({ type: 'offer', sdp: remoteSdp });
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);
    this.opts.signal({ type: 'answer', sdp: answer.sdp });
  }

  /** Procesa una respuesta SDP del callee (solo caller). */
  async onAnswer(sdp: string): Promise<void> {
    if (this.role !== 'caller') return;
    await this.pc.setRemoteDescription({ type: 'answer', sdp });
  }

  /** Aplica un candidato ICE recibido del peer remoto. */
  async onIce(candidate: IceCandidatePayload | null): Promise<void> {
    if (!candidate) return; // fin de candidatos: no se aplica
    try {
      await this.pc.addIceCandidate(candidate as RTCIceCandidate);
    } catch (err) {
      log.warn(`addIceCandidate falló: ${(err as Error).message}`);
    }
  }

  /** Cierra la llamada localmente y notifica al peer remoto. */
  async hangup(reason = 'local'): Promise<void> {
    if (this.state === 'ended') return;
    this.opts.signal({ type: 'end', reason });
    await this.teardown(reason);
  }

  /** Cierre forzado tras CALL_END remoto (no reenvía señalización). */
  async closeFromRemote(reason?: string): Promise<void> {
    if (this.state === 'ended') return;
    await this.teardown(reason ?? 'remote');
  }

  getState(): CallState {
    return this.state;
  }

  getRxStats(): { bytes: number; packets: number } {
    return { bytes: this.rxBytes, packets: this.rxPackets };
  }

  // -------------------------------------------------------------------------
  // Internos.
  // -------------------------------------------------------------------------

  private setupPeerConnection(): void {
    this.pc = new RTCPeerConnection({
      iceServers: STUN_SERVERS,
      codecs: { audio: [opusCodec] },
    });

    // Trickle ICE: cada candidato local lo enviamos al remoto en cuanto se
    // descubre. El `undefined` final indica "no hay más candidatos".
    this.pc.onIceCandidate.subscribe((c) => {
      this.opts.signal({
        type: 'ice',
        candidate: c ? { candidate: c.candidate, sdpMid: c.sdpMid, sdpMLineIndex: c.sdpMLineIndex } : null,
      });
    });

    this.pc.connectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] connectionState=${s}`);
      if (s === 'connected') this.setState('active');
      if (s === 'failed' || s === 'disconnected' || s === 'closed') {
        void this.teardown('connection-' + s);
      }
    });

    this.pc.iceConnectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] iceConnectionState=${s}`);
      if (this.state === 'signaling' && (s === 'checking' || s === 'connected')) {
        this.setState('connecting');
      }
    });

    // Track entrante: el peer remoto nos está enviando audio. Lo redirigimos
    // a ffplay (si playback) para reproducirlo por los altavoces.
    this.pc.onTrack.subscribe((track) => {
      log.info(`[${this.callId}] track entrante: kind=${track.kind}`);
      void this.setupPlayer(track);
    });
  }

  /**
   * Captura local: spawn de ffmpeg → datagramas RTP a 127.0.0.1:bridgePort
   * → reenviar cada datagrama a werift como un paquete RTP.
   */
  private async setupSender(): Promise<void> {
    this.localTrack = new MediaStreamTrack({ kind: 'audio' });
    this.pc.addTransceiver(this.localTrack, { direction: 'sendrecv' });

    // Bridge UDP local: nos atamos a un puerto efímero y ahí escribirá ffmpeg.
    this.senderBridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.senderBridge!.once('error', reject);
      this.senderBridge!.bind(0, '127.0.0.1', () => {
        const addr = this.senderBridge!.address();
        this.senderBridgePort = addr.port;
        resolve();
      });
    });

    this.senderBridge.on('message', (chunk) => {
      // Cada datagrama es ya un paquete RTP — se lo pasamos crudo a werift.
      try {
        this.localTrack?.writeRtp(chunk);
      } catch (err) {
        log.debug(`writeRtp falló: ${(err as Error).message}`);
      }
    });

    // Lanza ffmpeg apuntando al bridge.
    const source = this.opts.source ?? 'tone';
    const args = buildFfmpegSenderArgs(source, { host: '127.0.0.1', port: this.senderBridgePort });
    log.info(`[${this.callId}] ffmpeg sender: ${args.join(' ')}`);
    this.senderFfmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    this.senderFfmpeg.stderr?.on('data', (b: Buffer) => {
      const s = b.toString('utf8').trim();
      if (s) log.debug(`[ffmpeg-tx] ${s}`);
    });
    this.senderFfmpeg.on('error', (err) => {
      log.warn(`ffmpeg sender no se pudo lanzar: ${err.message}. ¿ffmpeg en PATH?`);
    });
    this.senderFfmpeg.on('exit', (code) => {
      log.debug(`[ffmpeg-tx] exit code=${code}`);
    });
  }

  /**
   * Reproducción remota: cada RtpPacket entrante lo serializamos y reenviamos
   * por UDP a 127.0.0.1:playerBridgePort, donde un ffplay está consumiendo
   * con un SDP que describe el flujo.
   */
  private async setupPlayer(track: MediaStreamTrack): Promise<void> {
    if (this.opts.playback === false) {
      // Modo sin reproducción: solo contamos paquetes para diagnóstico.
      track.onReceiveRtp.subscribe((pkt) => {
        const buf = pkt.serialize();
        this.rxBytes += buf.length;
        this.rxPackets++;
        this.emit('rx-rtp', buf.length);
      });
      return;
    }

    this.playerBridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.playerBridge!.once('error', reject);
      // Bind a 0 = puerto efímero; ffplay leerá del mismo puerto que ponemos en el SDP.
      this.playerBridge!.bind(0, '127.0.0.1', () => {
        const addr = this.playerBridge!.address();
        this.playerBridgePort = addr.port;
        resolve();
      });
    });

    // Escribir SDP a /tmp y lanzar ffplay.
    this.playerSdpPath = path.join(tmpdir(), `p2p-chat-${this.callId}-${randomBytes(3).toString('hex')}.sdp`);
    writeFileSync(this.playerSdpPath, buildReceiverSdp(this.playerBridgePort), 'utf8');

    const ffplayArgs = [
      '-protocol_whitelist', 'file,rtp,udp',
      '-fflags', 'nobuffer',
      '-flags', 'low_delay',
      '-nodisp',
      '-autoexit',
      '-loglevel', 'warning',
      '-i', this.playerSdpPath,
    ];
    log.info(`[${this.callId}] ffplay receiver: ffplay ${ffplayArgs.join(' ')}`);
    this.playerFfplay = spawn('ffplay', ffplayArgs, { stdio: ['ignore', 'ignore', 'pipe'] });
    this.playerFfplay.stderr?.on('data', (b: Buffer) => {
      const s = b.toString('utf8').trim();
      if (s) log.debug(`[ffplay-rx] ${s}`);
    });
    this.playerFfplay.on('error', (err) => {
      log.warn(`ffplay no se pudo lanzar: ${err.message}. Reproducción desactivada.`);
    });

    track.onReceiveRtp.subscribe((pkt) => {
      const buf = pkt.serialize();
      this.rxBytes += buf.length;
      this.rxPackets++;
      this.emit('rx-rtp', buf.length);
      this.playerBridge?.send(buf, this.playerBridgePort, '127.0.0.1');
    });
  }

  private setState(s: CallState): void {
    if (this.state === s) return;
    this.state = s;
    this.emit('state', s);
  }

  private async teardown(reason: string): Promise<void> {
    if (this.state === 'ended') return;
    this.setState('ended');
    log.info(`[${this.callId}] teardown: ${reason}`);

    // Subprocesos
    try { this.senderFfmpeg?.kill('SIGTERM'); } catch { /* */ }
    try { this.playerFfplay?.kill('SIGTERM'); } catch { /* */ }

    // Bridges UDP
    try { this.senderBridge?.close(); } catch { /* */ }
    try { this.playerBridge?.close(); } catch { /* */ }

    // SDP temporal
    if (this.playerSdpPath) {
      try { unlinkSync(this.playerSdpPath); } catch { /* */ }
    }

    // PC werift
    try {
      this.localTrack?.stop();
      await this.pc?.close();
    } catch {
      /* ignore */
    }

    this.emit('ended', reason);
  }
}

/** Helper para que main.ts no tenga que importar randomBytes solo por esto. */
export { newCallId };
