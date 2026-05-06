# Protocolo

## Framing

Cada mensaje viaja sobre TCP precedido de un prefijo de longitud:

```
┌──────────────┬──────────────────────────────┐
│ length (u32) │ payload (length bytes)       │
└──────────────┴──────────────────────────────┘
   big-endian       contenido del mensaje
```

`length` es un entero de 32 bits big-endian (`Buffer.writeUInt32BE`). El receptor
acumula bytes y, cuando reúne `4 + length`, entrega un mensaje completo a la capa
superior.

Tamaño máximo de frame: 4 MB. Protege contra basura/DoS y deja margen sobrado
para una pieza de 256 KB + cabecera.

## Mensajes

El primer byte del payload identifica el tipo:

| Tipo  | Nombre           | Estructura del payload (después del byte de tipo)                       |
|-------|------------------|--------------------------------------------------------------------------|
| 0x01  | HELLO            | JSON `{peerId: string, version: number}`                                 |
| 0x02  | LIST             | (vacío)                                                                  |
| 0x03  | LIST_REPLY       | JSON `{files: FileSummary[]}`                                            |
| 0x04  | MANIFEST         | JSON `{fileId: string}`                                                  |
| 0x05  | MANIFEST_REPLY   | JSON `{manifest: FileManifest}`                                          |
| 0x06  | HAVE             | JSON `{fileId: string, bitfield: base64}`                                |
| 0x07  | REQUEST          | JSON `{fileId: string, pieceIndex: number}`                              |
| 0x08  | PIECE            | uint32BE headerLen + JSON `{fileId, pieceIndex}` + bytes de la pieza     |
| 0x09  | ERROR            | JSON `{code: string, message: string}`                                   |
| 0x0A  | BYE              | (vacío)                                                                  |

### `FileSummary`

```ts
{ fileId: string, name: string, size: number, pieceSize: number, numPieces: number }
```

### `FileManifest`

```ts
FileSummary & { pieceHashes: string[] /* hex SHA-256 */, fileHash: string /* hex */ }
```

## Identidad

`peerId` es el hex de `SHA-256(randomBytes(32))`, regenerado en cada arranque.
`fileId` es el hex de `SHA-256(contenido)` — el archivo *es* su id.

⚠️ No hay autenticación criptográfica: cualquiera puede afirmar tener un peerId
arbitrario. Para uso real haría falta firmar los HELLO con un keypair Ed25519.

## Ejemplo: HELLO de A → B

Supongamos `peerId = "abcd1234…"` y versión 1.

```
type:    01
payload: {"peerId":"abcd1234…","version":1}      (JSON, 51 bytes)
```

Frame en el cable (length=52, big-endian):

```
00 00 00 34 01 7B 22 70  65 65 72 49 64 22 3A 22  ...
└──length──┘ │ └──────────── JSON sin comillas externas ────…
            type
```

## Ejemplo: PIECE de B → A (pieza 0, fileId `f00d…`, 4 bytes de datos `DEAD BEEF`)

```
type:      08
headerLen: 00 00 00 22                                ← uint32BE = 34
header:    {"fileId":"f00d…","pieceIndex":0}         ← 34 bytes JSON
data:      DE AD BE EF                                ← bytes crudos
```

El receptor decodifica `headerLen`, extrae los 34 bytes de JSON, y trata el
resto del frame como bytes crudos sin escapar. Esto evita el overhead de
codificar binario en JSON.

## Apertura de conexión (handshake)

1. La capa de transporte abre/acepta el socket TCP.
2. Cada extremo envía inmediatamente un `HELLO` con su `peerId`.
3. Hasta no recibir un `HELLO`, los demás mensajes se descartan y la conexión
   se cierra ante cualquier basura.
4. Si llega un `HELLO` con un `peerId` ya conocido, se cierra la conexión nueva
   (no permitimos sockets duplicados).
5. Si llega un `HELLO` con nuestro propio `peerId` (auto-conexión en localhost),
   se cierra.

## Anuncios HAVE

Tras conectarse, cada extremo envía un `HAVE` por cada archivo que tiene
(completo o en progreso) con su bitfield actual. Cuando una pieza se completa
durante una descarga, se difunde un `HAVE` con el bitfield actualizado a todos
los peers conectados.

## Errores

Mensaje `ERROR` con `{code, message}`. Códigos usados:
- `NOT_FOUND` — el `fileId` solicitado no está disponible.
- `PIECE_NOT_AVAILABLE` — el `pieceIndex` pedido aún no lo tenemos.
