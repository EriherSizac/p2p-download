# Protocolo — p2p-chat

## Framing

Cada mensaje viaja sobre TCP precedido de un prefijo de longitud:

```
┌──────────────┬──────────────────────────────┐
│ length (u32) │ payload (length bytes)       │
└──────────────┴──────────────────────────────┘
   big-endian       contenido del mensaje
```

`length` es un entero de 32 bits big-endian. El receptor acumula bytes y,
cuando reúne `4 + length`, entrega un mensaje completo a la capa superior.
Tamaño máximo: 4 MB. Suficiente: la SDP de una llamada Opus mono cabe en
< 4 KB y los candidatos ICE en bytes.

## Mensajes

El primer byte del payload identifica el tipo. Versión actual del protocolo:
**2** (anunciada en `HELLO.version`).

### Mensajería + liveness

| Tipo  | Nombre    | Estructura del payload                                |
|-------|-----------|--------------------------------------------------------|
| 0x01  | HELLO     | JSON `{peerId: string, version: number}`              |
| 0x02  | CHAT      | JSON `{messageId: string, text: string, ts: number}`  |
| 0x03  | CHAT_ACK  | JSON `{messageId: string}`                            |
| 0x04  | BYE       | (vacío)                                                |
| 0x05  | PING      | JSON `{nonce: number}`                                |
| 0x06  | PONG      | JSON `{nonce: number}`                                |

### Señalización de llamadas WebRTC

| Tipo  | Nombre        | Estructura del payload                                                          |
|-------|---------------|---------------------------------------------------------------------------------|
| 0x10  | CALL_OFFER    | JSON `{callId: string, sdp: string}`                                            |
| 0x11  | CALL_ANSWER   | JSON `{callId: string, sdp: string}`                                            |
| 0x12  | CALL_ICE      | JSON `{callId: string, candidate: {candidate, sdpMid?, sdpMLineIndex?} \| null}` |
| 0x13  | CALL_END      | JSON `{callId: string, reason?: string}`                                        |

`callId` es un id hex aleatorio (6 bytes) que distingue varias llamadas
concurrentes. `candidate: null` significa "fin de candidatos".

## Identidades

- `peerId` es el hex de `SHA-256(randomBytes(32))`, regenerado en cada
  arranque.
- `messageId` es un id hex aleatorio (8 bytes) generado por el emisor;
  correlaciona `CHAT` ↔ `CHAT_ACK` y aparece en `history.jsonl`.
- `callId` es un id hex aleatorio (6 bytes) por llamada.

⚠️ No hay autenticación criptográfica: cualquiera puede afirmar tener un
peerId arbitrario. Para uso real haría falta firmar HELLO/CHAT/CALL_OFFER
con un keypair Ed25519. Lo mismo aplica a las SDPs — un MITM en el canal
de señalización podría hacer su propia llamada al otro extremo.

## Apertura de conexión (handshake)

1. La capa de transporte abre/acepta el socket TCP.
2. Cada extremo envía inmediatamente un `HELLO` con su `peerId`.
3. Hasta no recibir un `HELLO`, los demás mensajes se descartan.
4. Si llega un `HELLO` con un `peerId` ya conocido, se cierra la conexión
   nueva (no permitimos sockets duplicados).

## Liveness (PING/PONG)

Cada 5 s, por cada peer conectado, se envía un `PING(nonce)`. El receptor
responde con `PONG(nonce)`. El emisor calcula
`RTT = ahora - tsEnvío` y mantiene un EWMA con α=0.2:

```
RTT_t = α·RTT_medido + (1-α)·RTT_{t-1}
```

Si el `PONG` no llega antes del siguiente ciclo, no se considera fallo
"duro" — el peer puede estar simplemente con latencia alta. El sistema
de descubrimiento (UDP timeout 10 s) sigue siendo la fuente de verdad
para "está vivo".

## Llamadas A/V (capa 5)

WebRTC separa **señalización** de **media**. Aquí:

- **Señalización**: viaja por la capa 4 (este protocolo) sobre TCP. Es
  texto SDP + candidatos ICE, sólo se mueve durante el setup.
- **Media**: viaja por SRTP/UDP **directamente** entre los peers, fuera
  de este protocolo. werift es el responsable.

### Estados de una llamada

```
              CALL_OFFER
   idle ──────────────────► signaling
                                │
                                │ CALL_ANSWER
                                ▼
                            connecting
                                │
                                │ ICE/DTLS completos
                                ▼
                              active
                                │
                                │ CALL_END  o  fallo de conexión
                                ▼
                              ended
```

### Ejemplo CALL_OFFER (truncado)

```
type:     10
payload:  {"callId":"a1b2c3","sdp":"v=0\r\no=- ...\r\nm=audio 9 UDP/TLS/RTP/SAVPF 96\r\n..."}
```

Frame en el cable (length=N, big-endian):

```
00 00 0N 8E 10 7B 22 63  61 6C 6C 49 64 22 3A 22  ...
```

### Trickle ICE

Cada candidato se manda en cuanto se descubre, sin esperar a tenerlos
todos:

```
A → B:  CALL_ICE { candidate: "candidate:1 1 udp ... host" }
A → B:  CALL_ICE { candidate: "candidate:2 1 udp ... srflx ..." }
...
A → B:  CALL_ICE { candidate: null }      // fin
```

werift consume cada candidato vía `pc.addIceCandidate()`.

## Compatibilidad

Este es protocolo **v2**. La v1 (sin PING/PONG ni CALL_*) no es compatible:
un peer v1 hablando con v2 verá tipos desconocidos y cerrará la conexión.
Mantenemos el campo `version` en `HELLO` para que en el futuro se pueda
negociar.
