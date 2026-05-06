# Ejercicios — p2p-chat

El código del profesor está completo y funcional **excepto** los puntos
marcados con `TODO(ALUMNO)`. Estos ejercicios extienden la mensajería.

## Ejercicio 1 — Mensajería directa (obligatorio)

**Objetivo**: implementar el comando `msg <peerId> <texto>` que envía un
CHAT a un único peer (en lugar del broadcast de `chat`).

**Archivos a tocar**: [src/main.ts](../src/main.ts) (case `'msg':`).

**Pista**:
- Usa `resolvePeer(prefix)` para resolver un prefijo a peerId completo.
- Construye `{ type: MSG.CHAT, messageId: newMessageId(), text, ts: Date.now() }`.
- `transport.send(peerId, msg)` devuelve `false` si no hay conexión —
  notifícalo al usuario.

**Criterio de aceptación**: con tres peers conectados (A, B, C) un `msg <B>
hola` desde A debe llegar solo a B; C no debe ver nada.

## Ejercicio 2 — Acuse de recibo con timeout (intermedio)

**Objetivo**: garantizar que un CHAT ha sido recibido, o reportar fallo
tras 3 s.

**Lo que añadir**:

1. En el receptor (handler `MSG.CHAT`): tras renderizar, responder con
   `{ type: MSG.CHAT_ACK, messageId: msg.messageId }`.
2. En el emisor: mantener `Map<messageId, {timer, peerId}>` por mensaje
   enviado. Al recibir `CHAT_ACK`, limpiar el timer y mostrar `✓` en la UI;
   si el timer dispara antes, mostrar `✗ no entregado`.

**Criterio de aceptación**:
- Si ambos peers están vivos: el emisor ve `✓` instantáneo.
- Si el receptor mata su proceso justo después de leer: el emisor ve `✗`
  tras 3 s.

**Extra**: pintar el mensaje enviado en gris hasta confirmarse, en blanco
una vez confirmado.

## Ejercicio 3 — Historial persistente (intermedio)

**Objetivo**: guardar todos los CHATs (enviados y recibidos) en un fichero
`./history.jsonl` (un JSON por línea). Comando `history [n]` muestra los
últimos `n` (default 20).

**Pistas**:
- Crear `src/history.ts` con `append(record)` y `tail(n)` usando
  `fs.appendFile` / lectura por streams.
- Registrar también el peerId del otro extremo y la dirección
  (`in` / `out`).

## Ejercicio 4 — PING/PONG y latencia (avanzado)

**Objetivo**: medir el round-trip a cada peer y mostrarlo en `peers`.

**Lo que añadir**:

1. Tipos `PING = 0x05`, `PONG = 0x06` con payload `{nonce: number}`.
2. Cada 5 s, cada peer envía un PING a cada conexión activa. Al recibir
   PING se responde con PONG (mismo nonce).
3. Mantener un EWMA (media móvil exponencial) de RTT por peer.
4. Mostrar el RTT en `peers` y `who`.

---

## Reglas

- No introduzcas dependencias nuevas.
- Cuando añadas un tipo de mensaje, actualiza
  [docs/PROTOCOL.md](PROTOCOL.md).
- Nada de mezclar lógica con `p2p-files`. Si quieres reutilizar algo,
  cópialo: este repo está deliberadamente desacoplado.
