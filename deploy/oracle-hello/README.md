# Ensayo mínimo del deploy en Oracle VM

Antes de desplegar el proyecto entero (o tu app de productividad), practica el flujo de
Oracle con esto: **un contenedor web trivial detrás de Caddy con HTTPS**. Si esto te
funciona, ya dominas el 90% de la parte "nube" (lo demás es solo más contenedores).

## Pasos

1. Crea la VM Ubuntu del Always Free y anota su **IP pública** (ver
   `../DEPLOY-oracle.md`, secciones 1 y 2 — incluida la apertura de puertos 80/443 en la
   Security List de OCI).

2. Conéctate por SSH e instala Docker (si no lo tienes):
   ```bash
   curl -fsSL https://get.docker.com | sudo sh
   sudo usermod -aG docker $USER && newgrp docker
   ```

3. Abre el firewall del sistema de la VM (las imágenes OCI lo traen cerrado):
   ```bash
   sudo iptables -I INPUT 5 -p tcp --dport 80 -j ACCEPT
   sudo iptables -I INPUT 6 -p tcp --dport 443 -j ACCEPT
   ```

4. Copia esta carpeta a la VM (o clona el repo) y levántalo usando tu IP con nip.io:
   ```bash
   cd deploy/oracle-hello
   DOMAIN=<IP_PUBLICA>.nip.io docker compose up -d
   ```

5. Abre en el navegador: `https://<IP_PUBLICA>.nip.io`
   Deberías ver la respuesta de la app de prueba, con candado (HTTPS válido).

## Qué has validado con esto

- Cuenta OCI + VM arrancada.
- Reglas de ingress 80/443 en la Security List (OCI).
- Firewall del sistema abierto (iptables) — el punto donde casi todos se atascan.
- Resolución por nip.io + emisión automática de certificado TLS por Caddy.

Cuando esto funcione, repetir con el stack real (`../DEPLOY-oracle.md`) es lo mismo pero
con más servicios. Para tu app de productividad, cambia la imagen `web` por la tuya.

## Limpieza

```bash
docker compose down
```
