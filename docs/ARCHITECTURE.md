# Arquitectura

El sistema está dividido en capas bien separadas. Cada capa resuelve **un único
problema** y es agnóstica de las superiores. Esta separación es deliberadamente
pedagógica: el alumno debe poder leer una capa sin entender las demás.

## Vista general

```
┌─────────────────────────────────────────────────────────┐
│  CLI (main.ts)                                          │  ← comandos del usuario
├─────────────────────────────────────────────────────────┤
│  Scheduler (scheduler.ts)                               │  ← política BitTorrent
│    + FileIndex / PieceStore / PeerState                 │
├─────────────────────────────────────────────────────────┤
│  Capa 4: Protocolo (protocol.ts)                        │  ← mensajes tipados
├─────────────────────────────────────────────────────────┤
│  Capa 3: Framing (framing.ts)                           │  ← length-prefix
├─────────────────────────────────────────────────────────┤
│  Capa 2: Transporte (transport.ts)                      │  ← TCP + pool
├─────────────────────────────────────────────────────────┤
│  Capa 1: Descubrimiento (discovery.ts)                  │  ← UDP broadcast
└─────────────────────────────────────────────────────────┘
                       Sistema operativo
```

### ¿Qué resuelve cada capa?

| Capa | Problema | Solución |
|------|----------|----------|
| 1 — Descubrimiento | "¿Quién más está vivo en mi LAN?" | Broadcast UDP periódico con `{peerId, tcpPort}`. |
| 2 — Transporte | "Necesito un canal fiable, ordenado, bidireccional con cada peer." | TCP con servidor + cliente y un pool indexado por `peerId`. |
| 3 — Framing | "TCP es bytes, no mensajes." | Cada mensaje va prefijado con su longitud (uint32 BE). |
| 4 — Protocolo | "Necesito tipos de mensaje y serialización." | Byte de tipo + payload (JSON o JSON+binario para PIECE). |
| Index/Store | "Necesito identificar archivos y piezas, persistir el progreso." | `fileId = SHA-256(contenido)`, hash por pieza, bitfield en disco. |
| Scheduler | "¿Qué pieza pedir, a quién, cuántas a la vez?" | Rarest-first + límites de paralelismo + timeouts. |

## Flujo end-to-end de `get <peer> <archivo>`

```
peer A (descarga)                        peer B (sirve)
─────────────────                        ──────────────
discovery: rx anuncio UDP de B           discovery: tx anuncio UDP
transport: net.connect → HELLO  ─────►   accept + HELLO
                                ◄─────   HELLO de vuelta

cli: list <B>
  send LIST                     ─────►
                                ◄─────   LIST_REPLY [{fileId, name, size, …}]
cli: get <B> <name>
  send MANIFEST(fileId)         ─────►
                                ◄─────   MANIFEST_REPLY {pieceHashes, fileHash}
scheduler.startDownload(manifest)
  open PieceStore (.part + .bitfield)
  broadcast HAVE(bitfield_local)─────►
                                ◄─────   HAVE(bitfield_B)  (B también lo manda al conectarse)

scheduler.kick():
  rarity[i] = #peers con pieza i
  pick rarest, peer con slot libre
  send REQUEST(fileId, i)       ─────►
                                ◄─────   PIECE(fileId, i, bytes)
  store.writePiece(i, bytes)
    verifica SHA-256 == manifest.pieceHashes[i]
  broadcast HAVE(bitfield_local)─────►   (anuncia que ya tenemos esa pieza)
  ... bucle hasta isComplete() ...

store.finalize():
  verifica SHA-256(file) == manifest.fileHash
  rename .part → name real
  unlink .bitfield
index.add(manifest, finalPath)
broadcast HAVE(bitfield_completo)
```

## Decisiones de diseño y por qué

- **JSON en la mayoría de mensajes**: introspección fácil, fácil de extender. El
  coste extra (~5–20%) es irrelevante salvo para PIECE, que es híbrido.
- **PIECE con cabecera JSON + payload binario**: evita el ~33% de overhead de
  base64 dentro de JSON.
- **Tie-break lexicográfico al conectar**: cuando A descubre B y B descubre A
  simultáneamente, solo el de `peerId` menor inicia la conexión saliente. Evita
  dobles sockets.
- **Bitfield en disco como sidecar**: separar datos (`.part`) de estado
  (`.bitfield`) hace la reanudación trivial.
- **Rarest-first**: maximiza la "salud" del enjambre. Si un peer único tiene
  una pieza rara y se desconecta, esa pieza desaparece. Pidiéndola primero la
  replicamos cuanto antes.
- **Sin endgame mode**: en BitTorrent al final se piden las últimas piezas a
  varios peers a la vez. Aquí no lo implementamos para no complicar el código —
  es un buen ejercicio extra.
- **Manifiestos servidos bajo demanda**: cada peer guarda los manifiestos de
  sus propios archivos, pero solo los envía a quien los pida explícitamente.
