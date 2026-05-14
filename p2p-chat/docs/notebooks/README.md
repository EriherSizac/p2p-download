# Notebooks — p2p-chat

Cuatro notebooks Jupyter que explican el proyecto **capa por capa**, con
diagramas, simulaciones ejecutables y referencias cruzadas al código.

Recomendado leerlos en orden:

| # | Notebook | Contenido |
|---|----------|-----------|
| 01 | [Descubrimiento y Transporte](01-descubrimiento-y-transporte.ipynb) | UDP broadcast, GC de peers, TCP pool, tie-break, handshake HELLO |
| 02 | [Framing y Protocolo](02-framing-y-protocolo.ipynb) | length-prefix vs separadores, parser stateful, tabla de tipos, codec |
| 03 | [Mensajería avanzada](03-mensajeria-ack-history-rtt.ipynb) | CHAT_ACK con timeout, JSONL append, PING/PONG, **EWMA con plots** |
| 04 | [Llamadas WebRTC](04-llamadas-webrtc.ipynb) | ICE/DTLS/SRTP, señalización vs media, RTP packet, bridge ffmpeg |

## Requisitos

```bash
pip install jupyter matplotlib
jupyter notebook
```

Los notebooks usan kernel Python 3. No necesitas Node para leerlos — el
código TS solo aparece como referencia en celdas markdown. Las
simulaciones (parser de frames, EWMA, decodificación de RTP) sí son
ejecutables y permiten experimentar con los parámetros.

## Por qué Python y no Node

Pedagogía: matplotlib + numpy producen mejores gráficas para visualizar
EWMA, y `struct` es más limpio que Buffer para parsing binario. Los
conceptos (framing, RTT, ICE) son agnósticos del lenguaje; el código de
producción está en TypeScript.
