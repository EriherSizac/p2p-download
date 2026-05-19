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
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
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
    case 'win32': {
      // DirectShow no expone "default". Si el usuario no pasó nombre,
      // enumeramos dispositivos y usamos el PRIMER audio listado.
      // Si pasó algo, lo respetamos (puede ser `audio=Nombre exacto`).
      const finalSpec = spec ?? `audio=${detectDefaultWindowsMic()}`;
      return ['-f', 'dshow', '-i', finalSpec];
    }
    default:
      throw new Error(`mic no soportado en plataforma: ${process.platform}`);
  }
}

/**
 * Pregunta a ffmpeg por la lista de dispositivos DirectShow y devuelve
 * el nombre del PRIMER dispositivo de audio. ffmpeg imprime esta info
 * en stderr (no stdout) — patrón clásico de ffmpeg con `-list_devices`.
 *
 * Formato típico de la salida:
 *
 *   [dshow @ 0x...]  "Microphone (Realtek Audio)"
 *   [dshow @ 0x...]     Alternative name "@device_cm_{GUID}\..."
 *
 * Vienen ANTES los de vídeo y luego los de audio. Detectamos la sección
 * "DirectShow audio devices" para no confundir.
 *
 * Se llama sincrónicamente (spawnSync) porque ocurre solo al arrancar
 * captura y es rapidísimo (<100ms). Cachea entre llamadas.
 */
let cachedWinMic: string | undefined;
function detectDefaultWindowsMic(): string {
  if (cachedWinMic) return cachedWinMic;
  const result = spawnSync('ffmpeg', ['-hide_banner', '-list_devices', 'true', '-f', 'dshow', '-i', 'dummy'], {
    encoding: 'utf8',
    windowsHide: true,
  });
  // ffmpeg sale con código != 0 al fallar el `-i dummy`, pero la lista
  // ya está en stderr. Por eso no chequeamos status.
  const stderr = result.stderr ?? '';
  // Estado: false = todavía en sección vídeo, true = ya en audio.
  let inAudioSection = false;
  let firstAudio: string | undefined;
  for (const line of stderr.split(/\r?\n/)) {
    if (/DirectShow\s+audio\s+devices/i.test(line)) { inAudioSection = true; continue; }
    if (/DirectShow\s+video\s+devices/i.test(line)) { inAudioSection = false; continue; }
    if (!inAudioSection) continue;
    // Líneas con dispositivos llevan el nombre entre comillas dobles.
    // Saltamos las líneas "Alternative name" (usan el GUID interno).
    if (/Alternative name/i.test(line)) continue;
    const m = line.match(/"([^"]+)"/);
    if (m) { firstAudio = m[1]; break; }
  }
  if (!firstAudio) {
    throw new Error(
      'No se encontró ningún dispositivo de audio DirectShow. ' +
        'Conecta un mic o pasa uno explícito: `call <peer> mic:audio=<nombre>`.',
    );
  }
  cachedWinMic = firstAudio;
  log.info(`mic Windows por defecto: "${firstAudio}"`);
  return firstAudio;
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
    log.debug(`ffmpeg sender: ${args.join(' ')}`);
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
  /** Socket que ENVÍA paquetes a ffplay. Bound a un puerto efímero local
   *  que NO es el que ffplay escucha — son sockets distintos. */
  private sender?: dgram.Socket;
  /** Puerto en el que ffplay escucha (escrito en el SDP). */
  private bridgePort = 0;
  private ffplay?: ChildProcess;
  private sdpPath?: string;

  /** Estadísticas de paquetes recibidos. */
  bytes = 0;
  packets = 0;

  /**
   * Reserva un puerto local libre para ffplay, escribe el SDP apuntando a
   * ese puerto, lanza ffplay (que hará `bind()` sobre él), y crea un
   * socket sender en OTRO puerto efímero para reenviarle paquetes RTP.
   *
   * Importante: nuestro socket NUNCA bindea al puerto del SDP. Ese es
   * exclusivo de ffplay. Si los compartiéramos, ffplay no recibiría nada
   * (UDP sin REUSEPORT no permite dos `bind()` simultáneos al mismo puerto).
   */
  async start(callId: string): Promise<void> {
    this.bridgePort = await reserveFreePort();
    this.writeSdpFile(callId);
    this.spawnFfplay();
    await this.openSender();
  }

  /** Inyecta un paquete RTP (ya descifrado por werift) hacia ffplay. */
  feed(rtp: Buffer): void {
    this.bytes += rtp.length;
    this.packets++;
    this.sender?.send(rtp, this.bridgePort, '127.0.0.1');
  }

  stop(): void {
    try { this.ffplay?.kill('SIGTERM'); } catch { /* ignore */ }
    try { this.sender?.close(); } catch { /* ignore */ }
    if (this.sdpPath) {
      try { unlinkSync(this.sdpPath); } catch { /* ignore */ }
    }
    this.ffplay = undefined;
    this.sender = undefined;
    this.sdpPath = undefined;
  }

  // --- internos ---

  private async openSender(): Promise<void> {
    // bind(0) → SO nos da puerto efímero. Solo lo usamos para .send() —
    // nunca recibimos por aquí.
    this.sender = dgram.createSocket('udp4');
    await new Promise<void>((resolve, reject) => {
      this.sender!.once('error', reject);
      this.sender!.bind(0, '127.0.0.1', () => resolve());
    });
  }

  private writeSdpFile(callId: string): void {
    const tag = randomBytes(3).toString('hex');
    this.sdpPath = path.join(tmpdir(), `p2p-chat-${callId}-${tag}.sdp`);
    writeFileSync(this.sdpPath, buildReceiverSdp(this.bridgePort), 'utf8');
  }

  // (definición de spawnFfplay justo debajo)
  private spawnFfplay(): void {
    // Cada flag y su porqué:
    //   -protocol_whitelist file,rtp,udp → ffmpeg restringe protocolos por
    //      seguridad. Nuestro SDP referencia rtp+udp; hay que autorizarlos.
    //   -fflags nobuffer / -flags low_delay → minimizan buffer y latencia.
    //   -nodisp                          → audio-only (no ventana SDL).
    //   -autoexit                        → al cortarse el flujo, salir.
    //   -loglevel error → solo errores reales. ffplay imprime un status
    //      por frame ("aq=…KB") en niveles más verbosos; con `error`
    //      desaparece. Si necesitas diagnóstico, sube a `info` aquí.
    const args = [
      '-protocol_whitelist', 'file,rtp,udp',
      '-fflags', 'nobuffer',
      '-flags', 'low_delay',
      '-nodisp',
      '-autoexit',
      '-loglevel', 'error',
      '-i', this.sdpPath!,
    ];
    // Env override SDL en Windows. Razón: el backend WASAPI de SDL2 a veces
    // no inicializa en subprocesos sin loop de mensajes (caso típico al
    // lanzar ffplay desde Node). DirectSound es más tolerante. Si el usuario
    // ya tiene SDL_AUDIODRIVER definido, respetamos su elección.
    const env = { ...process.env };
    if (process.platform === 'win32' && !env['SDL_AUDIODRIVER']) {
      env['SDL_AUDIODRIVER'] = 'directsound';
    }
    log.debug(`ffplay receiver: ffplay ${args.join(' ')}`);
    if (env['SDL_AUDIODRIVER']) log.debug(`SDL_AUDIODRIVER=${env['SDL_AUDIODRIVER']}`);
    this.ffplay = spawn('ffplay', args, { stdio: ['ignore', 'ignore', 'pipe'], env });
    // ffplay stderr → debug. Con `-loglevel error` solo sale aquí cuando
    // hay algo realmente roto, y el nivel debug evita ruido al usuario.
    this.ffplay.stderr?.on('data', (b: Buffer) => {
      const s = b.toString('utf8').trim();
      if (s) log.debug(`[ffplay] ${s}`);
    });
    this.ffplay.on('error', (err) => {
      log.warn(`ffplay no se pudo lanzar: ${err.message}. Reproducción desactivada.`);
    });
    this.ffplay.on('exit', (code) => log.debug(`[ffplay] exit code=${code}`));
  }
}

/**
 * Reserva un puerto UDP local libre: bindea temporalmente, lee el número
 * que asignó el SO, cierra el socket, devuelve el puerto.
 *
 * Race minúscula: entre `close()` y el bind de ffplay, otro proceso podría
 * llevarse el puerto. En 127.0.0.1 con asignación efímera del SO es muy
 * improbable. Aceptamos el riesgo a cambio de simplicidad.
 */
function reserveFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = dgram.createSocket('udp4');
    s.once('error', reject);
    s.bind(0, '127.0.0.1', () => {
      const port = s.address().port;
      s.close(() => resolve(port));
    });
  });
}
