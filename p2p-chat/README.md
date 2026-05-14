# p2p-chat

Chat P2P didáctico con **llamadas de audio reales sobre WebRTC**.

Hermano de [`p2p-files`](../p2p-files). Comparten estructura por capas pero
la implementación está **deliberadamente duplicada** para que cada proyecto
se pueda estudiar y modificar sin acoplamiento.

## Estructura

```
src/
├── main.ts            # CLI (readline) y orquestación
├── discovery.ts       # CAPA 1 — descubrimiento UDP
├── transport.ts       # CAPA 2 — servidor + cliente TCP
├── framing.ts         # CAPA 3 — length-prefix framing
├── protocol.ts        # CAPA 4 — HELLO/CHAT/CHAT_ACK/PING/PONG/CALL_*/BYE
├── call.ts            # CAPA 5 — WebRTC (werift) + ffmpeg
├── history.ts         # Persistencia JSONL del chat
└── logger.ts          # Logger con niveles
docs/                  # ARCHITECTURE, PROTOCOL, EXERCISES
```

## Cómo correrlo

```bash
npm install
npm run dev          # arranca un peer
```

En otra terminal (o en otra máquina de la misma LAN), arranca un segundo
peer y se descubrirán automáticamente.

Para llamadas A/V necesitas `ffmpeg` y `ffplay` en el `PATH`:

- macOS: `brew install ffmpeg`
- Linux: `apt install ffmpeg` (incluye ffplay)
- Windows: descarga de [ffmpeg.org](https://ffmpeg.org/) y añade al PATH.

Variables de entorno:

| Var              | Default        | Descripción                                  |
|------------------|----------------|----------------------------------------------|
| `TCP_PORT`       | `0` (random)   | Puerto TCP                                   |
| `DISCOVERY_PORT` | `41235`        | Puerto UDP de descubrimiento                 |
| `LOG_LEVEL`      | `info`         | `debug` \| `info` \| `warn` \| `error`       |
| `CALL_PLAYBACK`  | `1`            | `0` para no lanzar ffplay (modo silencioso)  |

> El default 41235 es distinto al de `p2p-files` (41234) para que ambos
> enjambres puedan coexistir en la misma LAN sin verse entre sí.

Comandos del CLI:

```
peers                       # peers descubiertos + RTT
who                         # peers conectados + RTT
chat <texto>                # broadcast a todos los conectados
msg <peerId> <texto>        # mensaje directo (con ACK ✓/✗)
history [n]                 # últimos n mensajes del historial local
call <peerId> [source]      # iniciar llamada A/V (source=tone|mic|mic:<spec>|file:<ruta>)
answer                      # aceptar llamada entrante
hangup                      # colgar la llamada (o rechazar entrante)
menu | help | quit
```

## Documentación

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — las 5 capas y flujo de chat + llamada.
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — tabla de mensajes (mensajería + señalización WebRTC).
- [docs/EXERCISES.md](docs/EXERCISES.md) — el único ejercicio: transferencia de archivos.
- [../docs/NAT.md](../docs/NAT.md) — limitaciones de descubrimiento UDP y por qué STUN sí pero TURN no.
