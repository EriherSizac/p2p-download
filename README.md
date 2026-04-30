# p2p-download

Aplicación CLI **didáctica** de compartir archivos peer-to-peer al estilo BitTorrent,
escrita en Node.js + TypeScript usando **únicamente módulos nativos** (`net`, `dgram`,
`crypto`, `fs`, …). Sin dependencias de runtime.

> Material de clase para alumnos avanzados. El código está pensado para **enseñar la
> arquitectura P2P**, no para esconderla detrás de una librería. Cada capa vive en su
> propio archivo con un comentario de cabecera explicando qué problema resuelve y por qué.

## Objetivos pedagógicos

1. Entender cómo se descubren peers sin servidor central (broadcast UDP en LAN).
2. Ver por qué TCP necesita *framing* aplicativo (length-prefix) para transportar mensajes.
3. Diseñar un protocolo binario simple con tipos de mensaje discriminados.
4. Implementar transferencia paralela multi-peer con verificación por hash, bitfield,
   reanudación y selección rarest-first (lo esencial de BitTorrent).
5. Discutir las limitaciones que aparecen al salir de la LAN (NAT, hole punching, DHT…).

## Estructura del repo

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
└── logger.ts          # Logger con niveles
docs/                  # ARCHITECTURE, PROTOCOL, NAT, EXERCISES
shared/                # Archivos que este peer publica
downloads/             # Archivos descargados (+ manifiestos y bitfields)
scripts/demo.ts        # Demo: 3 peers en el mismo proceso
```

## Cómo correrlo

```bash
npm install
npm run dev          # arranca un peer
npm run demo         # arranca 3 peers en un único proceso
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
peers                 # lista peers descubiertos en la LAN
list <peerId>         # pide el catálogo de archivos de un peer
get <peerId> <name>   # descarga un archivo
status                # progreso de descargas y peers conectados
msg <peerId> <texto>  # mensajería directa (ejercicio para alumnos)
quit                  # cierre limpio
```

## Documentación

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — capas y flujo end-to-end de un `get`.
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — tabla de mensajes con ejemplos.
- [docs/NAT.md](docs/NAT.md) — por qué esto solo funciona en LAN. Lectura para clase.
- [docs/EXERCISES.md](docs/EXERCISES.md) — ejercicios para alumnos.

## Ejercicios

El código del profesor está completo y funcional **excepto** los puntos marcados con
`TODO(ALUMNO)`. Esos huecos son los ejercicios — ver [docs/EXERCISES.md](docs/EXERCISES.md).
