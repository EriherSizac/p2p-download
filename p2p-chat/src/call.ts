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

/**
 * Servidores STUN públicos (RFC 5389). Funcionan como un "espejo": le mandas
 * un BINDING REQUEST y te contesta con la IP:puerto pública desde la que vio
 * tu paquete. Esa info se transforma en un candidato ICE de tipo "srflx"
 * (server-reflexive). Ese es el candidato que un peer detrás de NAT necesita
 * publicar para que el otro extremo sepa adónde mandar UDP.
 *
 * NO confundir con TURN (RFC 5766): TURN sí retransmite media — es un relay
 * UDP de pago/auto-hospedado. Sin TURN, dos peers con NAT simétrico (cada
 * salida = puerto distinto y aleatorio) no se pueden ver: ICE fallará. Es
 * el caso real más común tras "no me funciona la llamada". Ver docs/NAT.md.
 *
 * Aquí ponemos un STUN público (Google) por simplicidad. Si tu uso es serio
 * deberías auto-hospedar tu STUN/TURN (coturn) por privacidad y SLA.
 */
const STUN_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

/**
 * Codec Opus (RFC 6716): el estándar de facto para voz en WebRTC.
 *   - 48 kHz de sample rate (mismo que CD digital, no necesita resampling
 *     porque WebRTC siempre usa 48k internamente)
 *   - 2 canales (estéreo). Para voz pura podrías bajar a 1 (mono) y reducir
 *     bitrate; lo dejamos en 2 para que también valga música/file:
 *   - Payload Type 96: en RTP los PT 0–95 están "fijos" por IANA (RFC 3551:
 *     PT 0 = PCMU, PT 8 = PCMA, etc.). El rango 96–127 es "dynamic":
 *     cada sesión negocia qué codec va en cada número. WebRTC siempre usa
 *     PT dinámicos para Opus.
 */
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

  // Args de salida de ffmpeg. Cada flag tiene su porqué:
  //   -ac 2          → 2 canales (estéreo). Debe coincidir con opusCodec.
  //   -ar 48000      → resample a 48 kHz. Si la fuente ya está a 48k, no-op.
  //   -c:a libopus   → encoder Opus.
  //   -b:a 64k       → bitrate constante 64 kbps. Voz buena ≥24k, música ≥96k.
  //   -application voip → ajusta el encoder para latencia (vs music/lowdelay).
  //                       voip activa DTX, redundancia y un perfil agresivo
  //                       en cuanto a bandas de frecuencia.
  //   -payload_type 96 → PT dinámico; tiene que coincidir con OPUS_PT en
  //                      el SDP que ofrecemos (si no, el receptor descartará
  //                      los paquetes porque no sabe qué codec son).
  //   -f rtp rtp://…  → muxer RTP. ffmpeg fragmenta el flujo Opus en
  //                      paquetes RTP (~20 ms cada uno) con su header de 12
  //                      bytes, los serializa y los manda por UDP.
  //   -loglevel warning → menos ruido en stderr; los errores aún salen.
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
 * Construye un SDP "minimal" (RFC 4566) que describe el flujo entrante para
 * ffplay. SDP = Session Description Protocol; es texto plano con líneas
 * `clave=valor`. Para ffplay solo necesitamos:
 *
 *   v=0                              versión del protocolo SDP (siempre 0)
 *   o=- 0 0 IN IP4 127.0.0.1         origin (username/sessId/version, IPv4)
 *   s=p2p-chat                       nombre de sesión (cosmético)
 *   c=IN IP4 127.0.0.1               connection data: por dónde llega
 *   t=0 0                            tiempo (0 0 = sesión permanente)
 *   m=audio <port> RTP/AVP <PT>      media line: audio en `port`, perfil RTP
 *                                    Audio/Video clásico, payload type 96
 *   a=rtpmap:96 opus/48000/2         mapeo PT→codec: PT 96 es Opus 48k 2ch
 *
 * ffplay lee este fichero, decide "abriré un socket UDP en 127.0.0.1:<port>
 * y voy a esperar paquetes RTP cuyo PT 96 son Opus estéreo". Nosotros le
 * mandaremos exactamente eso desde el bridge.
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

  /**
   * Lanza la llamada (solo caller). Pasos clásicos del "offer/answer":
   *   1) crear el RTCPeerConnection (escucha eventos ICE/track)
   *   2) preparar la fuente de audio local (track + ffmpeg)
   *   3) generar la SDP OFFER (qué codecs ofrezco, cómo recibo RTP)
   *   4) fijarla como `localDescription` — esto arranca la recolección
   *      de candidatos ICE; cada uno saldrá por `onIceCandidate`
   *   5) mandar la SDP al callee por el canal de señalización (TCP)
   */
  async start(): Promise<void> {
    if (this.role !== 'caller') throw new Error('start() solo en caller');
    this.setupPeerConnection();      // (1)
    await this.setupSender();        // (2)
    this.setState('signaling');

    const offer = await this.pc.createOffer();          // (3)
    await this.pc.setLocalDescription(offer);           // (4) — empieza ICE gathering
    this.opts.signal({ type: 'offer', sdp: offer.sdp }); // (5)
  }

  /**
   * Aceptar una oferta entrante (callee). Simétrico a start(), pero con
   * la SDP del peer remoto ya disponible:
   *   1) PC + ffmpeg local (como caller)
   *   2) setRemoteDescription(offer) → el PC sabe qué nos ofrece el otro
   *   3) createAnswer() → genera la respuesta compatible
   *   4) setLocalDescription(answer) → arranca ICE en este extremo
   *   5) mandar la answer de vuelta
   */
  async accept(remoteSdp: string): Promise<void> {
    if (this.role !== 'callee') throw new Error('accept() solo en callee');
    this.setupPeerConnection();
    await this.setupSender();
    this.setState('signaling');

    await this.pc.setRemoteDescription({ type: 'offer', sdp: remoteSdp }); // (2)
    const answer = await this.pc.createAnswer();                            // (3)
    await this.pc.setLocalDescription(answer);                              // (4)
    this.opts.signal({ type: 'answer', sdp: answer.sdp });                  // (5)
  }

  /** Procesa la respuesta SDP del callee. Tras esto, ambos extremos saben
   *  qué van a usar (codec, SSRC, etc.) y solo falta que ICE encuentre el
   *  par de candidatos válido para empezar a mover SRTP. */
  async onAnswer(sdp: string): Promise<void> {
    if (this.role !== 'caller') return;
    await this.pc.setRemoteDescription({ type: 'answer', sdp });
  }

  /** Aplica un candidato ICE remoto. Cada candidato es una pista de "yo
   *  podría recibir por aquí" (IP:puerto). werift los irá probando contra
   *  los nuestros locales hasta encontrar un par que pase el NAT. */
  async onIce(candidate: IceCandidatePayload | null): Promise<void> {
    if (!candidate) return; // null = "fin de candidatos" (no requiere acción)
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
    // El PC orquesta tres protocolos que viven sobre UDP:
    //
    //   ICE (RFC 8445): conectividad. Cada peer publica "candidatos" (IP:puerto
    //   por los que se le puede contactar). Tres tipos comunes:
    //     - host:  la IP local de la interfaz (172.20.x.x, 192.168.x.x...).
    //              Solo funciona si ambos peers están en la misma LAN.
    //     - srflx (server-reflexive): la IP:puerto pública vista por el STUN.
    //              Sirve si el NAT del peer reusa el mismo puerto para todas
    //              las conexiones salientes (cone NAT).
    //     - relay: dirección de un TURN que retransmite. Último recurso para
    //              NATs simétricos. NO usamos aquí (no hay TURN configurado).
    //   Los candidatos van por la señalización (CALL_ICE) y se prueban en pares.
    //
    //   DTLS (RFC 6347): handshake sobre UDP para acordar claves SRTP. Cada
    //   peer presenta un certificado autofirmado al vuelo; los huellas
    //   (fingerprints) se publican en la SDP, así MITM es detectable si
    //   confías en la señalización.
    //
    //   SRTP (RFC 3711): RTP cifrado con AES-CM 128 + HMAC-SHA1, claves
    //   derivadas del DTLS. Cada paquete RTP sale como un blob opaco que
    //   solo el peer remoto puede descifrar.
    this.pc = new RTCPeerConnection({
      iceServers: STUN_SERVERS,
      codecs: { audio: [opusCodec] },
    });

    // ── Evento 1: cada candidato ICE local que descubrimos ──────────────
    // Trickle ICE = no esperamos a tener todos los candidatos; mandamos
    // cada uno por la señalización en cuanto sale. Eso acelera el setup.
    // `c === undefined` significa "ya no hay más candidatos" → mandamos
    // null para que el peer remoto lo sepa.
    this.pc.onIceCandidate.subscribe((c) => {
      this.opts.signal({
        type: 'ice',
        candidate: c ? { candidate: c.candidate, sdpMid: c.sdpMid, sdpMLineIndex: c.sdpMLineIndex } : null,
      });
    });

    // ── Evento 2: cambios de estado de la conexión global ───────────────
    // "connected" = ICE pasó, DTLS pasó, los paquetes SRTP ya pueden fluir.
    // "failed/disconnected/closed" = la llamada se ha caído → limpiar.
    this.pc.connectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] connectionState=${s}`);
      if (s === 'connected') this.setState('active');
      if (s === 'failed' || s === 'disconnected' || s === 'closed') {
        void this.teardown('connection-' + s);
      }
    });

    // ── Evento 3: estado específico de ICE ──────────────────────────────
    // Útil para mostrar "conectando..." en el CLI mientras ICE elige par.
    this.pc.iceConnectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] iceConnectionState=${s}`);
      if (this.state === 'signaling' && (s === 'checking' || s === 'connected')) {
        this.setState('connecting');
      }
    });

    // ── Evento 4: track entrante (el remoto nos manda audio) ────────────
    // Cada `addTransceiver(...sendrecv)` del otro extremo dispara aquí UN
    // track. Lo encadenamos a ffplay para que lo reproduzcamos.
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
    // (a) Track de salida: representa "mi micrófono" en lenguaje WebRTC.
    //     Aún no tiene datos — los inyectaremos con writeRtp().
    this.localTrack = new MediaStreamTrack({ kind: 'audio' });
    // sendrecv = "voy a mandar Y a recibir". Si solo quisieras hablar,
    // usarías "sendonly"; si solo escuchar, "recvonly".
    this.pc.addTransceiver(this.localTrack, { direction: 'sendrecv' });

    // (b) Bridge UDP local entre ffmpeg y werift. ffmpeg no sabe enchufarse
    //     a un objeto JS; sólo sabe escribir RTP a un host:puerto. Abrimos
    //     un socket UDP en 127.0.0.1:puerto-libre y le decimos a ffmpeg
    //     "escribe ahí". Nuestra lógica lee cada datagrama y lo reinyecta
    //     en werift como paquete RTP.
    this.senderBridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.senderBridge!.once('error', reject);
      // bind(0) = el SO nos asigna un puerto libre.
      this.senderBridge!.bind(0, '127.0.0.1', () => {
        const addr = this.senderBridge!.address();
        this.senderBridgePort = addr.port;
        resolve();
      });
    });

    // (c) Cada datagrama UDP que llega es UN paquete RTP completo (RFC 3550):
    //
    //   0                   1                   2                   3
    //   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    //  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //  |V=2|P|X|  CC   |M|     PT      |       sequence number         |
    //  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //  |                           timestamp                           |
    //  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //  |           synchronization source (SSRC) identifier            |
    //  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //  |                       payload (Opus)                          |
    //  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //
    //  - V=2 fijo, M (marker) suele ser 0 para audio, PT=96 (Opus).
    //  - sequence number incrementa 1 por paquete; permite detectar pérdidas.
    //  - timestamp avanza en muestras (48000 Hz → +960 por trama de 20 ms).
    //  - SSRC identifica el flujo; werift le pone uno aleatorio al crear el
    //    track. Útil cuando hay multiplexing de varios flujos.
    //
    // ffmpeg ya nos da TODO esto correctamente formateado. Solo lo pasamos
    // tal cual a writeRtp(); werift lo encripta con SRTP y lo envía.
    this.senderBridge.on('message', (chunk) => {
      try {
        this.localTrack?.writeRtp(chunk);
      } catch (err) {
        log.debug(`writeRtp falló: ${(err as Error).message}`);
      }
    });

    // (d) Lanzamos ffmpeg apuntando al bridge. A partir de aquí, audio
    //     fluye: source → ffmpeg encode Opus → UDP local → werift → SRTP.
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
    // Modo "sin altavoces": no lanzamos ffplay. Solo contamos paquetes y
    // bytes recibidos para que el test/CI pueda validar que la conexión
    // funciona. Útil si la máquina no tiene salida de audio.
    if (this.opts.playback === false) {
      track.onReceiveRtp.subscribe((pkt) => {
        const buf = pkt.serialize();
        this.rxBytes += buf.length;
        this.rxPackets++;
        this.emit('rx-rtp', buf.length);
      });
      return;
    }

    // (a) Bridge UDP simétrico al del sender, pero al revés:
    //     werift nos da RtpPacket → lo reescribimos a UDP local →
    //     ffplay lo consume desde ese puerto.
    this.playerBridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.playerBridge!.once('error', reject);
      this.playerBridge!.bind(0, '127.0.0.1', () => {
        const addr = this.playerBridge!.address();
        this.playerBridgePort = addr.port;
        resolve();
      });
    });

    // (b) ffplay no se puede configurar por args para decir "escucha Opus
    //     en este puerto". Necesita un fichero SDP que describa el flujo
    //     entrante (codec, port, etc.). Lo generamos al vuelo y lo
    //     pasamos con -i.
    this.playerSdpPath = path.join(tmpdir(), `p2p-chat-${this.callId}-${randomBytes(3).toString('hex')}.sdp`);
    writeFileSync(this.playerSdpPath, buildReceiverSdp(this.playerBridgePort), 'utf8');

    // Flags de ffplay, cada uno con su razón:
    //   -protocol_whitelist file,rtp,udp → por seguridad ffmpeg restringe
    //      qué protocolos puede tocar un input. Como nuestro -i es un
    //      fichero .sdp que internamente hace `rtp://udp://...`, hay que
    //      autorizar los tres. Por defecto no permite rtp.
    //   -fflags nobuffer → desactiva el buffer de demuxing para reducir
    //      latencia.
    //   -flags low_delay → idem a nivel decoder.
    //   -nodisp → no abrir ventana SDL. Audio-only.
    //   -autoexit → cuando el flujo se corta, salir. Si no, se queda colgado.
    //   -loglevel warning → silenciar status; los errores sí salen.
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

    // (c) Cada paquete RTP que entra (ya descifrado por werift) lo
    //     serializamos a Buffer y se lo mandamos por UDP al ffplay.
    //     Es exactamente la operación inversa al sender.
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
