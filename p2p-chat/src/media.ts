/**
 * MEDIA — captura y reproducción de audio con ffmpeg/ffplay.
 *
 * werift maneja paquetes RTP cifrados (SRTP), pero no sabe nada de
 * micrófonos ni altavoces. Esa parte la delegamos a dos subprocesos:
 *
 *   • ffmpeg  → captura el audio de la fuente (mic/tono/fichero), lo
 *               codifica en Opus, lo empaqueta como RTP y lo escribe a
 *               un socket UDP local.
 *   • ffplay  → lee paquetes RTP desde un socket UDP local y los
 *               reproduce por los altavoces.
 *
 * Entre ffmpeg/ffplay y werift hay un "bridge": un socket UDP local
 * (127.0.0.1) que conecta el mundo de subprocesos con el mundo Node.
 *
 *     [SENDER]
 *      mic ──► ffmpeg ──UDP local──► bridge ──► track.writeRtp() ──► werift ──► red
 *
 *     [RECEIVER]
 *      red ──► werift ──► track.onReceiveRtp ──► bridge.send() ──UDP local──► ffplay ──► altavoces
 *
 * Dos clases públicas:
 *
 *   AudioSender   — lanza ffmpeg, abre bridge, expone `onPacket(cb)`.
 *   AudioReceiver — abre bridge, lanza ffplay, expone `feed(packet)`.
 *
 * Ambas exponen `stop()` para limpiar todo (proceso + socket + ficheros).
 */

import dgram from 'node:dgram';
import { spawn, type ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { writeFileSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { createLogger } from './logger.js';

const log = createLogger('media');

/**
 * Opus Payload Type 96. RTP fija PT 0–95 (PCMU=0, PCMA=8…). El rango
 * 96–127 es "dinámico": cada sesión SDP decide qué codec va en qué número.
 * WebRTC siempre usa PT dinámicos para Opus. Debe coincidir entre el
 * SDP que negociamos y los args de ffmpeg/ffplay.
 */
export const OPUS_PT = 96;

// ---------------------------------------------------------------------------
// Construcción de args ffmpeg según la fuente solicitada.
// ---------------------------------------------------------------------------

/**
 * Devuelve los args que ffmpeg necesita para LEER de la fuente pedida.
 * No incluye los args de salida (codec, RTP, etc): eso es común.
 */
function inputArgsForSource(source: string): string[] {
  // (1) Tono sintético — útil para tests/clase sin micrófono.
  if (source === 'tone') {
    return ['-re', '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000'];
  }

  // (2) Fichero — `file:ruta/al/audio.wav`.
  if (source.startsWith('file:')) {
    return ['-re', '-i', source.slice('file:'.length)];
  }

  // (3) Micrófono real. Cada SO tiene su backend distinto.
  if (source === 'mic') return micInputArgs();
  if (source.startsWith('mic:')) return micInputArgs(source.slice('mic:'.length));

  throw new Error(`fuente desconocida: ${source} (usa tone | mic | mic:<spec> | file:<ruta>)`);
}

function micInputArgs(spec?: string): string[] {
  switch (process.platform) {
    case 'darwin':
      // ":0" = primer dispositivo de entrada (mic por defecto).
      return ['-f', 'avfoundation', '-i', spec ?? ':0'];
    case 'linux':
      return ['-f', 'pulse', '-i', spec ?? 'default'];
    case 'win32':
      // DirectShow no tiene "default". El usuario debe nombrar el dispositivo:
      //   call <peer> mic:audio=<nombre>
      // Lista con: ffmpeg -list_devices true -f dshow -i dummy
      if (!spec) {
        throw new Error(
          'En Windows especifica el dispositivo: mic:audio=<nombre>. ' +
            'Lista con `ffmpeg -list_devices true -f dshow -i dummy`.',
        );
      }
      return ['-f', 'dshow', '-i', spec];
    default:
      throw new Error(`mic no soportado en plataforma: ${process.platform}`);
  }
}

/**
 * Args de salida ffmpeg comunes a cualquier fuente. Opus 48k estéreo 64kbps
 * empaquetado como RTP hacia `host:port`.
 */
function outputArgsRtp(host: string, port: number): string[] {
  return [
    '-ac', '2',                 // 2 canales (debe coincidir con SDP)
    '-ar', '48000',             // 48 kHz (resample si la fuente difiere)
    '-c:a', 'libopus',          // encoder Opus
    '-b:a', '64k',              // 64 kbps (voz buena ≥24k, música ≥96k)
    '-application', 'voip',     // ajusta perfil del encoder (latencia baja)
    '-payload_type', String(OPUS_PT),
    '-f', 'rtp', `rtp://${host}:${port}`,
    '-loglevel', 'warning',
  ];
}

// ---------------------------------------------------------------------------
// SENDER: ffmpeg → bridge UDP → callback con cada paquete RTP.
// ---------------------------------------------------------------------------

export class AudioSender {
  private bridge?: dgram.Socket;
  private bridgePort = 0;
  private ffmpeg?: ChildProcess;
  private onPacket?: (rtp: Buffer) => void;

  /**
   * Arranca el bridge UDP y lanza ffmpeg apuntando a él. Cada datagrama
   * que reciba el bridge es un paquete RTP completo (header + payload Opus)
   * tal y como lo necesita werift.
   *
   * @param source  'tone' | 'mic' | `mic:<spec>` | `file:<ruta>`
   * @param onPacket invocado por cada paquete RTP listo para enviar.
   */
  async start(source: string, onPacket: (rtp: Buffer) => void): Promise<void> {
    this.onPacket = onPacket;
    await this.openBridge();
    this.spawnFfmpeg(source);
  }

  stop(): void {
    try { this.ffmpeg?.kill('SIGTERM'); } catch { /* ignore */ }
    try { this.bridge?.close(); } catch { /* ignore */ }
    this.ffmpeg = undefined;
    this.bridge = undefined;
  }

  // --- internos ---

  private async openBridge(): Promise<void> {
    this.bridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.bridge!.once('error', reject);
      // bind(0) → el SO nos da un puerto libre.
      this.bridge!.bind(0, '127.0.0.1', () => {
        this.bridgePort = this.bridge!.address().port;
        resolve();
      });
    });
    // Cada datagrama es un paquete RTP listo: lo pasamos al callback.
    this.bridge.on('message', (chunk) => this.onPacket?.(chunk));
  }

  private spawnFfmpeg(source: string): void {
    const args = [
      ...inputArgsForSource(source),
      ...outputArgsRtp('127.0.0.1', this.bridgePort),
    ];
    log.info(`ffmpeg sender: ${args.join(' ')}`);
    this.ffmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    this.ffmpeg.stderr?.on('data', (b: Buffer) => {
      const s = b.toString('utf8').trim();
      if (s) log.debug(`[ffmpeg-tx] ${s}`);
    });
    this.ffmpeg.on('error', (err) => {
      log.warn(`ffmpeg no se pudo lanzar: ${err.message}. ¿ffmpeg en PATH?`);
    });
    this.ffmpeg.on('exit', (code) => log.debug(`[ffmpeg-tx] exit=${code}`));
  }
}

// ---------------------------------------------------------------------------
// RECEIVER: feed(packet) → bridge UDP → ffplay → altavoces.
// ---------------------------------------------------------------------------

/**
 * SDP minimal (RFC 4566) que describe el flujo entrante para ffplay.
 * ffplay no acepta "escucha Opus en UDP X" por args: necesita un fichero
 * SDP. Lo generamos al vuelo, lo guardamos en tmpdir, y lo pasamos con -i.
 *
 *   v=0                              versión SDP
 *   o=- 0 0 IN IP4 127.0.0.1         origin
 *   s=p2p-chat                       nombre sesión
 *   c=IN IP4 127.0.0.1               por dónde llega
 *   t=0 0                            sesión permanente
 *   m=audio <port> RTP/AVP <PT>      audio en `port`, payload type 96
 *   a=rtpmap:96 opus/48000/2         PT 96 = Opus 48k 2ch
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

export class AudioReceiver {
  private bridge?: dgram.Socket;
  private bridgePort = 0;
  private ffplay?: ChildProcess;
  private sdpPath?: string;

  /** Estadísticas de paquetes recibidos. */
  bytes = 0;
  packets = 0;

  /**
   * Abre el bridge UDP y lanza ffplay leyendo desde él. A partir de aquí,
   * cada `feed(buf)` reenvía un paquete RTP a ffplay.
   */
  async start(callId: string): Promise<void> {
    await this.openBridge();
    this.writeSdpFile(callId);
    this.spawnFfplay();
  }

  /** Inyecta un paquete RTP (ya descifrado por werift) hacia ffplay. */
  feed(rtp: Buffer): void {
    this.bytes += rtp.length;
    this.packets++;
    this.bridge?.send(rtp, this.bridgePort, '127.0.0.1');
  }

  stop(): void {
    try { this.ffplay?.kill('SIGTERM'); } catch { /* ignore */ }
    try { this.bridge?.close(); } catch { /* ignore */ }
    if (this.sdpPath) {
      try { unlinkSync(this.sdpPath); } catch { /* ignore */ }
    }
    this.ffplay = undefined;
    this.bridge = undefined;
    this.sdpPath = undefined;
  }

  // --- internos ---

  private async openBridge(): Promise<void> {
    this.bridge = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.bridge!.once('error', reject);
      this.bridge!.bind(0, '127.0.0.1', () => {
        this.bridgePort = this.bridge!.address().port;
        resolve();
      });
    });
  }

  private writeSdpFile(callId: string): void {
    const tag = randomBytes(3).toString('hex');
    this.sdpPath = path.join(tmpdir(), `p2p-chat-${callId}-${tag}.sdp`);
    writeFileSync(this.sdpPath, buildReceiverSdp(this.bridgePort), 'utf8');
  }

  private spawnFfplay(): void {
    // Cada flag y su porqué:
    //   -protocol_whitelist file,rtp,udp → ffmpeg restringe protocolos por
    //      seguridad. Nuestro SDP referencia rtp+udp; hay que autorizarlos.
    //   -fflags nobuffer / -flags low_delay → minimizan buffer y latencia.
    //   -nodisp                          → audio-only (no ventana SDL).
    //   -autoexit                        → al cortarse el flujo, salir.
    //   -loglevel warning                → menos ruido en stderr.
    const args = [
      '-protocol_whitelist', 'file,rtp,udp',
      '-fflags', 'nobuffer',
      '-flags', 'low_delay',
      '-nodisp',
      '-autoexit',
      '-loglevel', 'warning',
      '-i', this.sdpPath!,
    ];
    log.info(`ffplay receiver: ffplay ${args.join(' ')}`);
    this.ffplay = spawn('ffplay', args, { stdio: ['ignore', 'ignore', 'pipe'] });
    this.ffplay.stderr?.on('data', (b: Buffer) => {
      const s = b.toString('utf8').trim();
      if (s) log.debug(`[ffplay-rx] ${s}`);
    });
    this.ffplay.on('error', (err) => {
      log.warn(`ffplay no se pudo lanzar: ${err.message}. Reproducción desactivada.`);
    });
    this.ffplay.on('exit', (code) => log.debug(`[ffplay-rx] exit=${code}`));
  }
}
