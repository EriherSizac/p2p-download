# p2p-files

Aplicación CLI **didáctica** de compartir archivos peer-to-peer al estilo
BitTorrent. Solo módulos nativos de Node.

Hermano de [`p2p-chat`](../p2p-chat). Comparten estructura por capas pero la
implementación está **deliberadamente duplicada** para que cada proyecto se
pueda estudiar y modificar sin acoplamiento.

## Estructura

```
src/
├── main.ts            # CLI (readline) y orquestación de capas
├── discovery.ts       # CAPA 1 — descubrimiento UDP en LAN
├── transport.ts       # CAPA 2 — servidor + cliente TCP
├── framing.ts         # CAPA 3 — length-prefix framing
├── protocol.ts        # CAPA 4 — codec de mensajes tipados
├── manifest.ts        # Manifiestos: piezas + hashes
├── store.ts           # Persistencia de piezas + bitfield + reanudación
├── index-files.ts     # Index de ./shared/
├── peer-state.ts      # Estado por peer remoto
├── scheduler.ts       # Lógica BitTorrent: rarest-first, paralelismo
├── bitfield.ts        # Helpers de bitfield (set/get/popcount/base64)
└── logger.ts          # Logger con niveles
docs/                  # ARCHITECTURE, PROTOCOL, EXERCISES
scripts/demo.ts        # Demo: 3 peers en localhost
```

## Cómo correrlo

```bash
npm install
npm run dev          # arranca un peer
npm run demo         # 3 peers en localhost con transferencia automática
```

Variables de entorno:

| Var              | Default        | Descripción                                  |
|------------------|----------------|----------------------------------------------|
| `TCP_PORT`       | `0` (random)   | Puerto TCP donde sirve este peer             |
| `DISCOVERY_PORT` | `41234`        | Puerto UDP para broadcast de descubrimiento  |
| `SHARED_DIR`     | `./shared`     | Carpeta de archivos publicados               |
| `DOWNLOAD_DIR`   | `./downloads`  | Carpeta de descargas                         |
| `LOG_LEVEL`      | `info`         | `debug` \| `info` \| `warn` \| `error`       |

Comandos del CLI:

```
peers                 # peers descubiertos en la LAN
list <peerId>         # catálogo de archivos de un peer
share                 # listar mis archivos publicados
share <ruta>          # publicar un archivo (no lo mueve)
unshare <nombre>      # retirar del catálogo
search [nombre]       # buscar en peers conectados (substring)
get <peerId> <name>   # descarga un archivo
status                # progreso de descargas + peers conectados
menu | help | quit
```

## Documentación

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — capas y flujo end-to-end de
  un `get`.
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — tabla de mensajes con ejemplos hex.
- [docs/EXERCISES.md](docs/EXERCISES.md) — ejercicios para alumnos.
- [../docs/NAT.md](../docs/NAT.md) — discusión de NAT, STUN, TURN, ICE, DHT.
