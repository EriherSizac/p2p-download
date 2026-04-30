# Ejercicios

Estos huecos en el código están marcados con `TODO(ALUMNO)`. El resto del
sistema está completo y funcional — los ejercicios son piezas pequeñas pero
representativas de patrones reales en programación de red.

## Ejercicio 1 — Mensajería entre peers (obligatorio)

**Objetivo**: implementar el comando `msg <peerId> <texto>` que envía un
mensaje de chat a un peer conectado, y mostrarlo en su consola sin romper el
prompt de readline.

**Archivos a tocar**:

- [src/protocol.ts](../src/protocol.ts): verifica que el case `MSG.CHAT` en
  `decode()` funciona con el codec genérico (debería). Asegúrate de que el
  `encode()` para CHAT solo serializa `{text, ts}` y nada más.
- [src/main.ts](../src/main.ts):
  - Comando `msg`: parsea `rest[0]` como prefijo de peerId, valida que esté
    conectado (`transport.isConnected`), construye el `Message` de tipo CHAT
    con `Date.now()` y llama a `transport.send`.
  - Handler de mensajes entrantes: cuando llega un CHAT, imprime
    `[chat][<peerIdCorto>] <texto>` sin romper el prompt. Pista:
    `process.stdout.write('\r' + ...)` y luego `rl.prompt(true)`.

**Criterio de aceptación**:
- Dos peers conectados; uno escribe `msg <id> hola`. El otro lo ve impreso
  inmediatamente, sin perder el prompt si estaba escribiendo.
- Si el peer destino no está conectado, mostrar un error claro.

## Ejercicio 2 — Broadcast de chat (intermedio)

**Objetivo**: comando `msgall <texto>` que envía CHAT a todos los peers
conectados.

**Archivos a tocar**: solo [src/main.ts](../src/main.ts).

**Pistas**:
- `transport.connectedPeers()` devuelve los IDs.
- `transport.broadcast()` ya existe y manda un mensaje a todos.

**Extra**: añadir un comando `who` que muestre solo los peers conectados (no
los meramente descubiertos), con sus IPs y puertos.

## Ejercicio 3 — Acuse de recibo (avanzado)

**Objetivo**: garantizar que un mensaje de chat ha sido recibido por la otra
parte. Si no llega ACK en 3 segundos, marcar el mensaje como "no entregado".

**Lo que añadir**:

1. Nuevo tipo de mensaje `CHAT_ACK = 0x0C` en [src/protocol.ts](../src/protocol.ts)
   con payload `{messageId: string}`.
2. Modificar el envío de CHAT para que incluya un `messageId` (genera uno con
   `crypto.randomUUID()` o similar).
3. En el receptor: al recibir CHAT, responder inmediatamente con CHAT_ACK.
4. En el emisor: mantener un `Map<messageId, {resolve, reject, timer}>` por
   conexión. Resolver al recibir ACK; reject por timeout.

**Criterio de aceptación**:
- Si ambos peers están vivos: el emisor ve confirmación instantánea.
- Si el receptor se desconecta justo después de leer pero antes de imprimir:
  el ACK no llega, el emisor recibe error de "no entregado" tras 3 s.

**Extra**: integrar con la UI para que el mensaje enviado aparezca en gris
hasta confirmarse, en blanco una vez confirmado.

## Ejercicio 4 — Métricas de red (opcional)

**Objetivo**: medir la latencia round-trip a cada peer conectado y mostrarla
en `peers`.

**Cambios sugeridos**:

1. Nuevos tipos `PING = 0x0D`, `PONG = 0x0E`. Payload: `{nonce: number}`.
2. Cada 5 s, cada peer envía un PING a cada conexión activa. Al recibir PING
   se responde inmediatamente con PONG (mismo nonce).
3. Mantener un EWMA (media móvil exponencial) de RTT por peer en
   [src/peer-state.ts](../src/peer-state.ts).
4. Mostrar el RTT en el comando `peers`.

**Extra avanzado**: usar el RTT como tie-break en
[src/scheduler.ts](../src/scheduler.ts) cuando varios peers tienen una pieza
candidata. Preferir el de menor latencia para REQUESTs urgentes.

---

## Sobre el desarrollo de los ejercicios

- No introduzcas dependencias nuevas. Todo se puede hacer con módulos
  nativos.
- Cuando añadas un tipo de mensaje, actualiza [docs/PROTOCOL.md](PROTOCOL.md).
- Si la lógica nueva ocupa más de ~30 líneas, considera ponerla en un módulo
  aparte para no contaminar `main.ts` o `scheduler.ts`.
