# Protocolo — p2p-chat

## Framing

Cada mensaje viaja sobre TCP precedido de un prefijo de longitud:

```
┌──────────────┬──────────────────────────────┐
│ length (u32) │ payload (length bytes)       │
└──────────────┴──────────────────────────────┘
   big-endian       contenido del mensaje
```

`length` es un entero de 32 bits big-endian. El receptor acumula bytes y,
cuando reúne `4 + length`, entrega un mensaje completo a la capa superior.
Tamaño máximo: 4 MB.

## Mensajes

El primer byte del payload identifica el tipo. Este protocolo es
intencionalmente pequeño — solo lo necesario para mensajería:

| Tipo  | Nombre    | Estructura del payload                                    |
|-------|-----------|------------------------------------------------------------|
| 0x01  | HELLO     | JSON `{peerId: string, version: number}`                   |
| 0x02  | CHAT      | JSON `{messageId: string, text: string, ts: number}`       |
| 0x03  | CHAT_ACK  | JSON `{messageId: string}`  *(ejercicio)*                  |
| 0x04  | BYE       | (vacío)                                                    |

`messageId` es un id corto aleatorio (8 bytes hex) generado por el emisor;
sirve para correlacionar un CHAT con su CHAT_ACK.

## Identidad

`peerId` es el hex de `SHA-256(randomBytes(32))`, regenerado en cada
arranque. ⚠️ No hay autenticación criptográfica: cualquiera puede afirmar
tener un peerId arbitrario. Para uso real haría falta firmar los HELLO/CHAT
con un keypair Ed25519.

## Apertura de conexión (handshake)

1. La capa de transporte abre/acepta el socket TCP.
2. Cada extremo envía inmediatamente un `HELLO` con su `peerId`.
3. Hasta no recibir un `HELLO`, los demás mensajes se descartan.
4. Si llega un `HELLO` con un `peerId` ya conocido, se cierra la conexión
   nueva (no permitimos sockets duplicados).

## Ejemplo: CHAT broadcast

```
type:     02
payload:  {"messageId":"a1b2c3d4e5f60718","text":"hola","ts":1730000000000}
```

Frame en el cable (length=70, big-endian):

```
00 00 00 46 02 7B 22 6D  65 73 73 61 67 65 49 64  ...
```
