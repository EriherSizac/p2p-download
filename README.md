# p2p-download

Material **didáctico** sobre redes peer-to-peer en LAN, en Node.js + TypeScript,
usando **únicamente módulos nativos** (`net`, `dgram`, `crypto`, `fs`, …).
Sin dependencias de runtime.

> Para alumnos avanzados. El código está pensado para **enseñar la arquitectura
> P2P**, no para esconderla detrás de una librería. Cada capa vive en su propio
> archivo con un comentario de cabecera explicando qué problema resuelve y
> por qué.

## Dos proyectos hermanos, intencionalmente desacoplados

```
p2p-download/
├── p2p-files/   ← compartir archivos al estilo BitTorrent
└── p2p-chat/    ← mensajería directa entre peers
```

Cada uno es un proyecto **autónomo** (su propio `package.json`, `tsconfig`,
`src/`, `docs/`). Ambos comparten la misma estructura por capas
(descubrimiento → transporte → framing → protocolo) pero **duplican** la
implementación a propósito:

- Cada proyecto se lee en aislamiento sin saltar a un tercer paquete "core".
- El protocolo de cada uno solo contiene los mensajes que necesita.
- Los alumnos pueden modificar uno sin temor a romper el otro.

## Objetivos pedagógicos

1. Cómo se descubren peers sin servidor central (broadcast UDP en LAN).
2. Por qué TCP necesita *framing* aplicativo (length-prefix).
3. Diseño de un protocolo simple con tipos de mensaje discriminados.
4. (`p2p-files`) Transferencia paralela multi-peer con verificación por hash,
   bitfield, reanudación y selección rarest-first.
5. (`p2p-chat`) Mensajería 1-a-N y 1-a-1 con ACKs (ejercicio).
6. Limitaciones al salir de la LAN: NAT, hole punching, DHT, IPv6.

## Cómo correrlo

```bash
# Compartir archivos
cd p2p-files
npm install
npm run dev          # un peer
npm run demo         # 3 peers en localhost con transferencia automática

# Chat
cd ../p2p-chat
npm install
npm run dev          # arranca dos o más en LAN para chatear
```

Por defecto cada proyecto usa un puerto UDP de descubrimiento distinto
(`p2p-files`: 41234, `p2p-chat`: 41235) para que ambos enjambres puedan
coexistir en la misma LAN sin colisionar.

## Documentación

Cada proyecto trae su propia documentación:

- [p2p-files/docs/ARCHITECTURE.md](p2p-files/docs/ARCHITECTURE.md)
- [p2p-files/docs/PROTOCOL.md](p2p-files/docs/PROTOCOL.md)
- [p2p-files/docs/EXERCISES.md](p2p-files/docs/EXERCISES.md)
- [p2p-chat/docs/ARCHITECTURE.md](p2p-chat/docs/ARCHITECTURE.md)
- [p2p-chat/docs/PROTOCOL.md](p2p-chat/docs/PROTOCOL.md)
- [p2p-chat/docs/EXERCISES.md](p2p-chat/docs/EXERCISES.md)

Doc transversal (común a ambos):

- [docs/NAT.md](docs/NAT.md) — por qué esto solo funciona en LAN, qué haría
  falta para WAN (STUN, TURN, ICE, DHT, IPv6). Lectura para clase.
