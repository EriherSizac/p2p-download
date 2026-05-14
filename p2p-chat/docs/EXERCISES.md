# Ejercicios — p2p-chat

El código del profesor cubre mensajería completa (broadcast + directo + ACK +
historial + RTT) y **llamadas de audio reales sobre WebRTC** usando `werift`
+ `ffmpeg`. Ver [ARCHITECTURE.md](ARCHITECTURE.md) para la explicación de
cada pieza ya implementada.

Queda **un único ejercicio**: transferencia de archivos. Es deliberadamente
abierto: integra lo aprendido en mensajería y señalización.

## Ejercicio — Transferencia de archivos P2P

**Objetivo**: enviar un fichero arbitrario de un peer a otro a través de la
red P2P, con verificación de integridad y barra de progreso.

**Comando esperado**:

```
send <peerId> <ruta>      # emisor
accept-file               # receptor — acepta el envío pendiente
files                     # lista envíos en curso (entrantes y salientes)
```

### Tarea mínima (obligatoria)

1. Define cuatro mensajes nuevos en [src/protocol.ts](../src/protocol.ts):

   | Tipo  | Nombre        | Payload                                                       |
   |-------|---------------|---------------------------------------------------------------|
   | 0x20  | FILE_OFFER    | `{transferId, name, size, sha256}`                            |
   | 0x21  | FILE_ACCEPT   | `{transferId}` (o `FILE_REJECT` simétrico)                    |
   | 0x22  | FILE_CHUNK    | `{transferId, seq, data: base64}`                             |
   | 0x23  | FILE_DONE     | `{transferId}`                                                |

2. Emisor: tras `FILE_OFFER`, espera `FILE_ACCEPT`. Lee el fichero en
   bloques de 64 KB y mándalos como `FILE_CHUNK` con `seq` incremental.
   Al terminar manda `FILE_DONE`.

3. Receptor: tras aceptar, abre un fichero destino en `./downloads/` y
   acumula los chunks por `seq`. Cuando llega `FILE_DONE`, recalcula
   SHA-256 y compara con el del OFFER. Si coincide, deja el fichero en
   `./downloads/<name>`; si no, lo borra y avisa.

4. Muestra progreso (`bytes recibidos / total`) en el receptor cada 200 ms
   sin romper el prompt — reutiliza el patrón `cliSinks.info` que ya usa
   chat.

### Criterio de aceptación

- `send <peer> ./algo.bin` con `./algo.bin` de 5 MB completa en ambos peers
  sin errores. SHA-256 verificado.
- Si el receptor mata su proceso a la mitad, el emisor lo detecta (el
  `transport.send` devuelve `false`) y cancela la transferencia limpiamente.

### Bonus (intermedio)

- **Backpressure**: si `transport.send` devuelve `false` por buffer lleno,
  pausa la lectura del fichero (`stream.pause()`) y reanuda en `'drain'`.
- **Multiplexing**: que dos transferencias simultáneas no se interrumpan.
  Ya tienes `transferId` para distinguirlas.
- **Resume**: si la conexión cae a mitad, al reconectarse el receptor anuncia
  `FILE_RESUME(transferId, byteOffset)` y el emisor reanuda desde ahí.

### Bonus (avanzado): usar un DataChannel WebRTC

El proyecto ya tiene WebRTC montado para audio. Un `RTCDataChannel`
viaja por el mismo transporte SRTP/UDP que el audio y es mucho más
rápido que TCP para LAN/WAN con NAT. Pista:

```ts
// En src/call.ts — durante setupPeerConnection
const dc = pc.createDataChannel('files', { ordered: true });
dc.onMessage.subscribe((msg) => /* recibir chunks */);
dc.send(chunkBuffer);
```

Reutiliza la señalización CALL_OFFER/ANSWER/ICE para abrir el peer
connection cuando no haya llamada activa, o multiplexa el datachannel
sobre una llamada existente.

## Reglas

- No introduzcas dependencias nuevas más allá de las que ya hay en
  `package.json` (mensajería sigue usando solo módulos nativos; WebRTC
  ya está cubierto por `werift`).
- Cuando añadas un tipo de mensaje, **actualiza**
  [docs/PROTOCOL.md](PROTOCOL.md).
- Nada de mezclar lógica con `p2p-files`. Si quieres reutilizar algo,
  cópialo: este repo está deliberadamente desacoplado.
