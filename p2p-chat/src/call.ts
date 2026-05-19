// File: CAPA 5 — LLAMADAS A/V (WebRTC)
// Author: Erick Hernández Silva
//
// Esta capa se encarga SOLO de la parte WebRTC: peer connection, SDP
// offer/answer, ICE. La captura/reproducción real de audio (ffmpeg/ffplay)
// vive en `media.ts` para mantener cada archivo legible y enfocado.
//
// Diagrama global:
//
//   mic ──► AudioSender ──► track.writeRtp ──► werift ──SRTP/UDP──► PEER
//                                                                     │
//   altavoces ◄── AudioReceiver ◄── track.onReceiveRtp ◄── werift ◄───┘
//
// La señalización (offer/answer/ice/end) viaja por nuestro TCP del chat:
// el `signal` callback se lo pasamos desde main.ts, así Call NO depende
// del transporte. Si mañana cambias a WebSocket, solo tocas main.ts.

import { EventEmitter } from 'node:events';
import {
  RTCPeerConnection,
  RTCRtpCodecParameters,
  MediaStreamTrack,
  type RTCIceCandidate,
} from 'werift';
import { createLogger } from './logger.js';
import { newCallId, type IceCandidatePayload } from './protocol.js';
import { AudioSender, AudioReceiver, OPUS_PT } from './media.js';

const log = createLogger('call');

// ---------------------------------------------------------------------------
// Config WebRTC.
// ---------------------------------------------------------------------------

/**
 * STUN público (RFC 5389): "espejo" que devuelve tu IP:puerto pública. Es
 * lo mínimo para que un peer detrás de NAT publique candidatos srflx que
 * el otro extremo pueda usar.
 *
 * Nota: NO es TURN. TURN (RFC 5766) sí retransmite media. Sin TURN, dos
 * NATs simétricos no podrán hablarse. Para producción, auto-hospeda coturn.
 */
const STUN_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

/** Codec Opus 48k estéreo, payload type 96 dinámico — debe coincidir con media.ts */
const OPUS_CODEC = new RTCRtpCodecParameters({
  mimeType: 'audio/opus',
  clockRate: 48000,
  channels: 2,
  payloadType: OPUS_PT,
});

// ---------------------------------------------------------------------------
// Tipos públicos.
// ---------------------------------------------------------------------------

export type CallRole = 'caller' | 'callee';
export type CallState = 'idle' | 'signaling' | 'connecting' | 'active' | 'ended';

/** Mensaje que Call pide enviar al peer remoto vía el canal de señalización. */
export type SignalMsg =
  | { type: 'offer'; sdp: string }
  | { type: 'answer'; sdp: string }
  | { type: 'ice'; candidate: IceCandidatePayload | null }
  | { type: 'end'; reason?: string };

export interface CallOpts {
  callId: string;
  remotePeerId: string;
  role: CallRole;
  /** Fuente de audio local: 'tone' | 'mic' | `mic:<spec>` | `file:<ruta>`. */
  source?: string;
  /** Si false, no se lanza ffplay; solo se cuentan paquetes. Útil sin altavoces. */
  playback?: boolean;
  /** Cómo enviar señalización al peer remoto (lo conecta main.ts al transport). */
  signal: (msg: SignalMsg) => void;
}

export declare interface Call {
  on(event: 'state', listener: (s: CallState) => void): this;
  on(event: 'rx-rtp', listener: (bytes: number) => void): this;
  on(event: 'ended', listener: (reason?: string) => void): this;
}

// ---------------------------------------------------------------------------
// Call.
// ---------------------------------------------------------------------------

export class Call extends EventEmitter {
  readonly callId: string;
  readonly remotePeerId: string;
  readonly role: CallRole;

  private pc!: RTCPeerConnection;
  private localTrack?: MediaStreamTrack;
  private sender?: AudioSender;
  private receiver?: AudioReceiver;
  private state: CallState = 'idle';
  private readonly opts: CallOpts;

  constructor(opts: CallOpts) {
    super();
    this.callId = opts.callId;
    this.remotePeerId = opts.remotePeerId;
    this.role = opts.role;
    this.opts = opts;
  }

  // -------------------------------------------------------------------------
  // API pública. Una sola función por "evento" del ciclo de vida.
  // -------------------------------------------------------------------------

  /** Inicia llamada saliente (caller). */
  async start(): Promise<void> {
    if (this.role !== 'caller') throw new Error('start() solo en caller');
    this.setupPeerConnection();
    await this.setupLocalAudio();
    this.setState('signaling');

    // createOffer + setLocalDescription dispara la recolección ICE.
    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.opts.signal({ type: 'offer', sdp: offer.sdp });
  }

  /** Acepta llamada entrante (callee) con la SDP del peer. */
  async accept(remoteSdp: string): Promise<void> {
    if (this.role !== 'callee') throw new Error('accept() solo en callee');
    this.setupPeerConnection();
    await this.setupLocalAudio();
    this.setState('signaling');

    await this.pc.setRemoteDescription({ type: 'offer', sdp: remoteSdp });
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);
    this.opts.signal({ type: 'answer', sdp: answer.sdp });
  }

  /** Aplica la SDP answer del callee (solo caller). */
  async onAnswer(sdp: string): Promise<void> {
    if (this.role !== 'caller') return;
    await this.pc.setRemoteDescription({ type: 'answer', sdp });
  }

  /** Aplica un candidato ICE remoto (cualquier rol). */
  async onIce(candidate: IceCandidatePayload | null): Promise<void> {
    if (!candidate) return; // null = fin de candidatos
    try {
      await this.pc.addIceCandidate(candidate as RTCIceCandidate);
    } catch (err) {
      log.warn(`addIceCandidate falló: ${(err as Error).message}`);
    }
  }

  /** Cierre local + notificación al remoto. */
  async hangup(reason = 'local'): Promise<void> {
    if (this.state === 'ended') return;
    this.opts.signal({ type: 'end', reason });
    await this.teardown(reason);
  }

  /** Cierre tras CALL_END remoto (no reenvía señalización). */
  async closeFromRemote(reason?: string): Promise<void> {
    if (this.state === 'ended') return;
    await this.teardown(reason ?? 'remote');
  }

  getState(): CallState {
    return this.state;
  }

  getRxStats(): { bytes: number; packets: number } {
    return { bytes: this.receiver?.bytes ?? 0, packets: this.receiver?.packets ?? 0 };
  }

  // -------------------------------------------------------------------------
  // Construcción interna.
  // -------------------------------------------------------------------------

  /**
   * Crea el RTCPeerConnection y suscribe sus 4 eventos clave:
   *   1) onIceCandidate    → mandar cada candidato local al peer (trickle ICE)
   *   2) connectionState   → 'connected' = SRTP fluye; 'failed' = cerrar
   *   3) iceConnectionState → señaliza UI "conectando…"
   *   4) onTrack           → llegó un track remoto → arrancar receiver
   */
  private setupPeerConnection(): void {
    this.pc = new RTCPeerConnection({
      iceServers: STUN_SERVERS,
      codecs: { audio: [OPUS_CODEC] },
    });

    this.pc.onIceCandidate.subscribe((c) => {
      this.opts.signal({
        type: 'ice',
        candidate: c
          ? { candidate: c.candidate, sdpMid: c.sdpMid, sdpMLineIndex: c.sdpMLineIndex }
          : null,
      });
    });

    this.pc.connectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] connectionState=${s}`);
      if (s === 'connected') this.setState('active');
      if (s === 'failed' || s === 'disconnected' || s === 'closed') {
        void this.teardown(`connection-${s}`);
      }
    });

    this.pc.iceConnectionStateChange.subscribe((s) => {
      log.debug(`[${this.callId}] iceConnectionState=${s}`);
      if (this.state === 'signaling' && (s === 'checking' || s === 'connected')) {
        this.setState('connecting');
      }
    });

    this.pc.onTrack.subscribe((track) => {
      log.info(`[${this.callId}] track entrante: kind=${track.kind}`);
      void this.startReceiver(track);
    });
  }

  /**
   * Arranca el track local + ffmpeg sender. Cada paquete que sale del sender
   * se inyecta en el track con `writeRtp`. werift lo cifra (SRTP) y lo manda.
   */
  private async setupLocalAudio(): Promise<void> {
    this.localTrack = new MediaStreamTrack({ kind: 'audio' });
    this.pc.addTransceiver(this.localTrack, { direction: 'sendrecv' });

    this.sender = new AudioSender();
    // Default mic — más útil que `tone` para llamadas reales. `tone` queda
    // disponible si lo pasas explícitamente como source (testing/clase).
    await this.sender.start(this.opts.source ?? 'mic', (rtp) => {
      try {
        this.localTrack?.writeRtp(rtp);
      } catch (err) {
        log.debug(`writeRtp falló: ${(err as Error).message}`);
      }
    });
  }

  /**
   * Arranca ffplay para el track entrante. Cada paquete RTP recibido se
   * envía al receiver. En modo `playback=false`, solo contamos paquetes.
   */
  private async startReceiver(track: MediaStreamTrack): Promise<void> {
    if (this.opts.playback === false) {
      // Sin altavoces: contar paquetes y emitir métrica.
      track.onReceiveRtp.subscribe((pkt) => {
        const buf = pkt.serialize();
        this.emit('rx-rtp', buf.length);
      });
      return;
    }

    this.receiver = new AudioReceiver();
    await this.receiver.start(this.callId);
    track.onReceiveRtp.subscribe((pkt) => {
      const buf = pkt.serialize();
      this.receiver?.feed(buf);
      this.emit('rx-rtp', buf.length);
    });
  }

  // -------------------------------------------------------------------------
  // Estado + cierre.
  // -------------------------------------------------------------------------

  private setState(s: CallState): void {
    if (this.state === s) return;
    this.state = s;
    this.emit('state', s);
  }

  private async teardown(reason: string): Promise<void> {
    if (this.state === 'ended') return;
    this.setState('ended');
    log.info(`[${this.callId}] teardown: ${reason}`);

    this.sender?.stop();
    this.receiver?.stop();
    try {
      this.localTrack?.stop();
      await this.pc?.close();
    } catch {
      /* ignore */
    }
    this.emit('ended', reason);
  }
}

export { newCallId };
