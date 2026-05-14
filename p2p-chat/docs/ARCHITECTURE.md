# Arquitectura — p2p-chat

Cinco capas independientes; cada una resuelve **un único problema** y es
agnóstica de las superiores. La separación es deliberadamente pedagógica.

## Vista general

```
┌─────────────────────────────────────────────────────────┐
│  CLI (main.ts)                                          │  ← comandos del usuario
├─────────────────────────────────────────────────────────┤
│  Capa 5: Llamadas A/V (call.ts)                         │  ← WebRTC vía werift + ffmpeg
├─────────────────────────────────────────────────────────┤
│  Capa 4: Protocolo (protocol.ts)                        │  ← HELLO/CHAT/CHAT_ACK/PING/PONG/CALL_*/BYE
├─────────────────────────────────────────────────────────┤
│  Capa 3: Framing (framing.ts)                           │  ← length-prefix
├─────────────────────────────────────────────────────────┤
│  Capa 2: Transporte (transport.ts)                      │  ← TCP + pool
├─────────────────────────────────────────────────────────┤
│  Capa 1: Descubrimiento (discovery.ts)                  │  ← UDP broadcast
└─────────────────────────────────────────────────────────┘
                       Sistema operativo
```

| Capa | Problema | Solución |
|------|----------|----------|
| 1 — Descubrimiento | "¿Quién más está vivo en mi LAN?" | Broadcast UDP periódico con `{peerId, tcpPort}`. |
| 2 — Transporte | "Necesito un canal fiable, ordenado, bidireccional con cada peer." | TCP con servidor + cliente y un pool indexado por `peerId`. |
| 3 — Framing | "TCP es bytes, no mensajes." | Cada mensaje va prefijado con su longitud (uint32 BE). |
| 4 — Protocolo | "Necesito tipos de mensaje y serialización." | Byte de tipo + payload JSON. |
| 5 — Llamadas A/V | "Mensajería va por TCP pero audio en tiempo real no aguanta retransmisiones." | WebRTC sobre UDP/SRTP, señalizado por la capa 4. |

## Funcionalidades implementadas

Cada una corresponde a una idea concreta — no son ejercicios, están todas
funcionando. Léelas como una guía de qué buscar en el código.

### 1. `chat <texto>` — broadcast a todos los conectados

Genera un `messageId`, codifica `MSG.CHAT` y lo manda por separado a cada
peer del pool. Cada receptor responde con un `CHAT_ACK` (ver más abajo).

```
peer A                                peer B
──────                                ──────
discovery: tx anuncio UDP cada 3s
transport: net.connect → HELLO  ─►   accept + HELLO
                                ◄─   HELLO de vuelta

cli: chat hola
  for peer in connectedPeers():
    transport.send(peer, CHAT)  ─►   handleMessage(CHAT)
                                      → cliSinks.chat(from, text)
                                      → render con \r\x1b[K + rl.prompt(true)
                                ◄─   CHAT_ACK
  cliSinks.ack(messageId)            (no acción especial)
```

### 2. `msg <peerId> <texto>` — mensaje directo 1-a-1

Resuelve un prefijo a peerId completo con `resolvePeer()`, valida que esté
conectado y manda un único `CHAT`. Si el `transport.send` devuelve `false`
(buffer roto, peer caído) avisa al usuario. Ver
[src/main.ts](../src/main.ts), `case 'msg':`.

### 3. CHAT_ACK con timeout — ✓ entregado / ✗ no entregado

Cada `CHAT` enviado se registra en un `Map<messageId, {timer, peerId}>`. Al
recibir `CHAT_ACK`, se limpia el timer y se pinta `✓`. Si el timer salta
antes (3 s por defecto), se pinta `✗ no entregado (timeout)`.

```
emisor                       receptor
──────                       ────────
send(CHAT, messageId)   ─►   render
pendingAcks[id] = timer ◄─   send(CHAT_ACK, messageId)
clearTimeout(timer)
✓ messageId
```

Las constantes están en [src/main.ts](../src/main.ts):
`CHAT_ACK_TIMEOUT_MS = 3_000`.

### 4. `history [n]` — persistencia local

Cada CHAT enviado o recibido se persiste en `./history.jsonl` con
`{ts, dir, peerId, messageId, text}`. Una línea por mensaje permite
append O(1). El comando `history [n]` lee el fichero y muestra las
últimas `n` líneas (default 20). Ver [src/history.ts](../src/history.ts).

### 5. PING/PONG con RTT EWMA

Cada 5 s, por cada peer conectado, se envía un `PING` con `nonce` único.
El receptor responde con `PONG` (mismo nonce). El emisor mide
`RTT = ahora - tsEnvío` y actualiza una media móvil exponencial:

```
RTT_t = α·RTT_medido + (1-α)·RTT_{t-1}    con α = 0.2
```

Esto suaviza picos transitorios y converge rápido a la realidad. El
`rtt` se muestra en `peers` y `who`. Ver
[src/main.ts](../src/main.ts), `pingTimer` y `handleMessage(MSG.PONG)`.

### 6. Llamadas A/V — WebRTC sobre UDP/SRTP (capa 5)

Esta es la pieza grande. Resumen del por qué:

- **TCP es pésimo para audio en tiempo real**. Una retransmisión TCP
  introduce un parón audible (>50 ms), y TCP retransmite por construcción.
- **Lo que queremos**: UDP, con paquetes cifrados (SRTP), saliendo por NAT
  con la ayuda de STUN (descubrir nuestra IP pública) y, en último caso,
  TURN (relé si no hay agujero UDP entre los peers).
- **Implementarlo a mano** llevaría meses. Usamos
  [`werift`](https://github.com/shinyoshiaki/werift-webrtc), una
  implementación pura TypeScript del stack WebRTC para Node.

Diagrama de cómo se conectan las piezas:

```
mic (sistema operativo)
  ▼
ffmpeg subprocess                                   ffplay subprocess
  ▼ encode Opus, escribe RTP a UDP local              ▲ decode Opus desde RTP
127.0.0.1:senderBridgePort                            127.0.0.1:playerBridgePort
  ▼                                                   ▲
dgram socket (call.ts)                              dgram socket (call.ts)
  ▼ writeRtp(buf)                                     ▲ playerBridge.send(buf)
werift MediaStreamTrack ──── SRTP/UDP ────►  werift MediaStreamTrack (peer remoto)
                              [ICE, DTLS]
```

El intercambio de **SDP** y **candidatos ICE** se hace por la capa 4 con
los mensajes `CALL_OFFER`, `CALL_ANSWER`, `CALL_ICE`, `CALL_END`. Una vez
ICE/DTLS está listo, **el audio nunca toca el TCP**: viaja directo UDP a
UDP entre los peers.

#### Flujo completo de una llamada

```
caller (A)                                callee (B)
──────────                                ──────────
cli: call <B> tone
  spawnCall(role=caller)
  pc.addTransceiver(audio, sendrecv)
  ffmpeg lavfi sine → bridge UDP → track.writeRtp
  pc.createOffer
  CALL_OFFER(callId, sdp)             ─►  handleMessage(CALL_OFFER)
                                          cliSinks.info: 📞 llamada entrante
                                          (espera `answer`)
                                          cli: answer
                                          spawnCall(role=callee)
                                          pc.addTransceiver(audio)
                                          pc.setRemoteDescription(offer)
                                          pc.createAnswer
                                  ◄────── CALL_ANSWER(callId, sdp)
  pc.setRemoteDescription(answer)
  pc.onIceCandidate ──── CALL_ICE ─►  ◄─── CALL_ICE  ─── pc.onIceCandidate
  (trickle bidireccional)
  pc.connectionState=connected            pc.connectionState=connected
  RTP fluye UDP↔UDP (cifrado SRTP)
  ↓
  ffplay reproduce en el otro extremo

cli: hangup
  CALL_END(callId, 'user')             ─►  closeFromRemote
  pc.close, kill ffmpeg, kill ffplay
```

#### Fuentes de audio soportadas

`call <peerId> <source>`:

- `tone` (default) — tono 440 Hz generado por `lavfi`. Útil para validar la
  tubería sin micrófono. Funciona en todas las plataformas.
- `mic` — captura del micrófono por defecto:
  - macOS: `-f avfoundation -i :0`
  - Linux: `-f pulse -i default`
  - Windows: requiere especificar el dispositivo:
    `mic:audio=Microphone (Realtek...)`. Lista los dispositivos con
    `ffmpeg -list_devices true -f dshow -i dummy`.
- `file:<ruta>` — streamea un fichero de audio existente.

#### Vídeo

La misma máquina sirve para vídeo: añade un segundo transceiver con
`kind: 'video'` y un segundo ffmpeg que capture/encode H264. Lo dejamos
fuera del scope para no enmarañar la demo. La señalización es idéntica.

#### Dependencias externas

- `werift` ya está en `package.json`.
- `ffmpeg` y `ffplay` deben estar en el `PATH`. La aplicación se inicia
  igualmente si no están, pero `call` reportará el error.

## Decisiones de diseño

- **Tie-break lexicográfico al conectar**: cuando A y B se descubren a la
  vez, solo el de `peerId` menor inicia la conexión saliente. Evita sockets
  duplicados.
- **JSON para todos los mensajes**: chat es texto, las llamadas mueven SDP
  (texto) y candidatos ICE (texto). El audio binario va aparte por SRTP.
- **`messageId` por mensaje**: habilita ACKs, history y futuras
  correlaciones (read-receipt, edit, delete) sin cambios invasivos.
- **`callId` por llamada**: permite multiplexar varias llamadas y separar
  los CALL_ICE de cada una.
- **`cliSinks.chat / .ack / .info`**: separa el render del CLI de la lógica
  de red. `handleMessage` no conoce readline, solo invoca el sink.
- **Señalización sobre TCP, media sobre UDP**: clásico de WebRTC. El TCP
  P2P propio sirve perfectamente como canal de señalización.
- **STUN sí, TURN no**: usamos STUN público (Google). Sin TURN, peers con
  NAT simétricos no conseguirán abrir el canal UDP — limitación didáctica.
  Ver [docs/NAT.md](../../docs/NAT.md).

## Por qué no compartir código con `p2p-files`

Los proyectos comparten estructura conceptual (las primeras 4 capas) pero
**duplican** la implementación a propósito. Razones pedagógicas:

- Cada proyecto se puede leer en aislamiento sin abrir un tercer paquete
  "core".
- El protocolo de cada uno solo contiene los mensajes que necesita; no hay
  ramas muertas que confundan al alumno.
- Los alumnos pueden modificar uno sin temor a romper el otro.
