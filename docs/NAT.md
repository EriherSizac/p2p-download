# Por qué esto solo funciona en LAN (y qué haría falta para WAN)

> Este documento es **lectura para discusión en clase**. No hay código asociado.

## El problema en una frase

El descubrimiento usa **broadcast UDP** y los anuncios viajan a la dirección
`255.255.255.255`. Los routers domésticos **no reenvían** broadcast hacia la
WAN. Por tanto, dos peers en redes domésticas distintas nunca se "ven".

Pero incluso si el descubrimiento funcionara mágicamente (p.ej. con un tracker
o un DHT), seguiríamos teniendo el problema de la **conectividad**: los peers
detrás de NAT no son alcanzables directamente desde Internet.

## NAT en 90 segundos

NAT (*Network Address Translation*) traduce `IP_privada:puerto` ↔ `IP_pública:puerto`
en la frontera del router doméstico. El router mantiene una tabla con las
"sesiones salientes" y abre un puerto temporal para el retorno.

```
[Cliente 192.168.1.10:54321] ──out──► [Router 200.100.5.5:60001] ──out──► [Servidor 1.2.3.4:80]
                              ◄─in─                                ◄─in─
```

Para tráfico **iniciado desde dentro** funciona perfecto: el router asocia el
puerto efímero de salida con el flujo y deja entrar las respuestas.

Para tráfico **iniciado desde fuera**: el router no tiene una entrada que
asociar. El paquete entra al WAN, no hay regla, lo descarta. Esto rompe el
modelo P2P puro: nadie puede *iniciar* una conexión hacia un peer NATeado.

### Tipos de NAT (relevante para hole punching)

- **Full-cone**: cualquier IP externa puede usar el mapeo una vez establecido.
  Hole punching trivial.
- **Restricted-cone / port-restricted**: solo la IP/puerto destino original
  puede responder. Necesita coordinación.
- **Symmetric**: el puerto público cambia según el destino. *Hole punching no
  funciona*. Hace falta relay.

## Mecanismos para WAN

### Tracker / Servidor de rendezvous

Un servidor central mantiene "qué peers están vivos y dónde". No transporta
los datos — solo presenta a los peers. Los peers se conectan luego entre
ellos. **Pero**: si están NATeados, presentarse no basta.

BitTorrent tradicional usa esto (`tracker HTTP/UDP`).

### DHT (Distributed Hash Table)

Como Kademlia (BitTorrent Mainline DHT, IPFS). Reemplaza al tracker central
por una red distribuida que indexa "quién tiene qué". Necesita peers iniciales
("bootstrap nodes") y peers no NATeados como puntos de entrada.

### STUN

Protocolo simple: un peer pregunta a un servidor STUN público "¿qué IP/puerto
ves de mí?". Le permite descubrir su IP pública y puerto NATeado. Funciona con
NATs no symmetric.

### TURN

Cuando hole punching falla (NAT symmetric en alguno de los extremos), un
servidor TURN actúa de **relay** — todo el tráfico pasa por él. Coste alto,
pero es la única alternativa fiable.

### ICE

"Marco" que combina STUN+TURN+conexión directa: prueba todas las candidatas
posibles en paralelo y se queda con la mejor. WebRTC lo usa.

### Hole punching UDP (y TCP)

Dos peers, A y B, simultáneamente envían paquetes el uno al otro a través de
un servidor de rendezvous. Cuando A envía a la IP pública de B, su NAT crea
una "puerta" para tráfico de retorno. Si B también acaba de enviar a la IP
pública de A, ambos NATs tienen una entrada que coincide → conexión directa.

```
A ─UDP→ [NAT_A] ──→ NAT_B ✗ (no rule yet)
B ─UDP→ [NAT_B] ──→ NAT_A ✗ (no rule yet)
        ↓ ahora ambos NATs tienen una entrada saliente
A ─UDP→ [NAT_A] ──→ [NAT_B] ✓
B ─UDP→ [NAT_B] ──→ [NAT_A] ✓     ← conexión bidireccional viva
```

TCP hole punching es similar pero más frágil (depende del comportamiento del
NAT con SYNs simultáneos).

### IPv6

En IPv6 cada dispositivo tiene IP pública. NAT desaparece. Los firewalls
siguen siendo un problema, pero no la traducción. Es la solución "real" a
largo plazo.

### Otras opciones prácticas

- **mDNS/Bonjour** sobre la propia LAN (alternativa a nuestro broadcast UDP).
- **Tailscale / WireGuard** — VPN sobre la cual el broadcast UDP simplemente
  funciona, porque la red lógica es plana.
- **Relays vía Cloudflare Tunnel / ngrok** — para servir, no para P2P puro.

## Pregunta para clase

> ¿Por qué BitTorrent funciona en la práctica pese al NAT? Pista: hay siempre
> "seeders" en datacenters con IP pública que actúan como puntos de entrada.
> Sin ellos el enjambre se segmenta.
