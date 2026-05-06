# p2p-chat

Chat P2P didáctico en LAN. Solo módulos nativos de Node.

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
├── protocol.ts        # CAPA 4 — HELLO / CHAT / CHAT_ACK / BYE
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

Variables de entorno:

| Var              | Default        | Descripción                                  |
|------------------|----------------|----------------------------------------------|
| `TCP_PORT`       | `0` (random)   | Puerto TCP                                   |
| `DISCOVERY_PORT` | `41235`        | Puerto UDP de descubrimiento                 |
| `LOG_LEVEL`      | `info`         | `debug` \| `info` \| `warn` \| `error`       |

> El default 41235 es distinto al de `p2p-files` (41234) para que ambos
> enjambres puedan coexistir en la misma LAN sin verse entre sí.

Comandos del CLI:

```
peers                 # peers descubiertos
who                   # peers con handshake completado
chat <texto>          # broadcast a todos los conectados
msg <peerId> <texto>  # mensaje directo (ejercicio)
menu | help | quit
```

## Documentación

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — capas y flujo de un chat.
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — tabla de mensajes.
- [docs/EXERCISES.md](docs/EXERCISES.md) — ejercicios.
- [../docs/NAT.md](../docs/NAT.md) — por qué solo funciona en LAN.
