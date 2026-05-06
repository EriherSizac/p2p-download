# Arquitectura — p2p-chat

Cuatro capas independientes; cada una resuelve **un único problema** y es
agnóstica de las superiores. La separación es deliberadamente pedagógica.

## Vista general

```
┌─────────────────────────────────────────────────────────┐
│  CLI (main.ts)                                          │  ← comandos del usuario
├─────────────────────────────────────────────────────────┤
│  Capa 4: Protocolo (protocol.ts)                        │  ← HELLO / CHAT / CHAT_ACK / BYE
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

## Flujo de un `chat <texto>` broadcast

```
peer A (emisor)                          peer B / C (receptores)
───────────────                          ───────────────────────
discovery: tx anuncio UDP cada 3s
transport: net.connect → HELLO  ─────►   accept + HELLO
                                ◄─────   HELLO de vuelta

cli: chat hola
  for peer in connectedPeers():
    transport.send(peer, CHAT)  ─────►   handleMessage(CHAT)
                                          → cliSinks.chat(from, text)
                                          → render con \r\x1b[K + rl.prompt(true)

(ejercicio) receptor responde CHAT_ACK ─►  emisor correlaciona por messageId
```

## Decisiones de diseño

- **Tie-break lexicográfico al conectar**: cuando A y B se descubren a la
  vez, solo el de `peerId` menor inicia la conexión saliente. Evita sockets
  duplicados.
- **JSON para todos los mensajes**: chat es texto, no hay payload binario;
  JSON es introspectable y suficiente.
- **`messageId` por mensaje**: pequeño coste para habilitar ACKs y otras
  futuras correlaciones (read-receipt, edit, delete) sin cambios de
  protocolo invasivos.
- **`cliSinks.chat`**: permite separar el render del CLI de la lógica de
  red. `handleMessage` no conoce readline, solo invoca el sink. setupCli
  lo rellena con un renderer que no rompe el prompt.

## Por qué no compartir código con `p2p-files`

Los proyectos comparten estructura conceptual (las 4 capas) pero
**duplican** la implementación a propósito. Razones pedagógicas:

- Cada proyecto se puede leer en aislamiento sin abrir un tercer paquete
  "core".
- El protocolo de cada uno solo contiene los mensajes que necesita; no hay
  ramas muertas que confundan al alumno.
- Los alumnos pueden modificar uno sin temor a romper el otro.
