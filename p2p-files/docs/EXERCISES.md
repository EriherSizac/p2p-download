# Ejercicios — p2p-files

El código del profesor está completo y funcional. Estos ejercicios extienden
la transferencia de archivos. Para mensajería entre peers, ver el proyecto
hermano `p2p-chat`.

## Ejercicio 1 — Endgame mode (intermedio)

**Objetivo**: cuando queden ≤ 2 piezas pendientes, pedir cada pieza a *todos*
los peers que la tengan en lugar de a uno solo. La primera respuesta válida
gana; las demás se descartan al recibirlas (o se cancelan si tu protocolo lo
soportara).

**Por qué importa**: la pieza más lenta del enjambre dicta cuándo termina la
descarga. En endgame mode forzamos paralelismo extra para evitar que un
único peer lento bloquee el final.

**Archivos a tocar**: [src/scheduler.ts](../src/scheduler.ts).

**Criterio de aceptación**: en una demo con un peer "lento" simulado (añade
un `setTimeout` artificial al servir), el endgame debe completar la descarga
antes que sin él.

## Ejercicio 2 — Choking / unchoking simplificado (avanzado)

**Objetivo**: implementar un mecanismo tit-for-tat. Cada peer **estrangula**
(no responde a REQUESTs) a los peers que no le han contribuido nada, salvo
un slot de "optimistic unchoke" que rota cada 30 s.

**Pistas**:
- Llevar contador de bytes recibidos por peer en
  [src/peer-state.ts](../src/peer-state.ts).
- Añadir tipos `CHOKE` / `UNCHOKE` al protocolo.
- Reescribir `enqueueUpload` para respetar el estado choke/unchoke.

**Criterio de aceptación**: dos peers que solo descargan (no comparten)
ven cómo otros les estrangulan; un peer que sí sirve recibe slots.

## Ejercicio 3 — Verificación al arrancar (fácil)

**Objetivo**: añadir un comando `verify <fileId>` que recalcule el bitfield
desde disco usando `PieceStore.verifyOnLoad` y reporte cuántas piezas eran
realmente válidas vs. lo que el sidecar `.bitfield` decía.

**Por qué importa**: si el proceso se mata sin flush, el bitfield puede
quedar desincronizado con el contenido del `.part`.

**Archivos a tocar**: [src/main.ts](../src/main.ts) (comando) +
[src/store.ts](../src/store.ts) (ya existe `verifyOnLoad`).

## Ejercicio 4 — Prioridad de piezas (avanzado)

**Objetivo**: permitir descargar primero ciertos rangos de un archivo (p.ej.
para reproducir vídeo en streaming). Añadir API
`scheduler.prioritizePieces(fileId, range)` y modificar la selección para
sesgar la prioridad sin romper rarest-first cuando la zona prioritaria esté
saturada.

**Pista**: en `kick()`, ordenar por `(priority, rarity)` en lugar de solo
rarity.

## Ejercicio 5 — Soporte IPv6 (fácil-intermedio)

Hoy el descubrimiento usa `udp4` y broadcast IPv4. Implementa una variante
con `udp6` y multicast IPv6 (`ff02::1`, link-local "all nodes"). Solo será
visible para hosts IPv6 en el mismo segmento, pero es la dirección "moderna"
del problema y elimina NAT.

---

## Reglas

- No introduzcas dependencias nuevas.
- Cuando añadas un tipo de mensaje, actualiza
  [docs/PROTOCOL.md](PROTOCOL.md).
- Si la lógica nueva pasa de ~30 líneas, considera ponerla en un módulo
  aparte para no contaminar `main.ts` o `scheduler.ts`.
