# 🚀 Guía de Despliegue en Producción - API E-commerce

Guía completa para desplegar la API E-commerce en entornos de producción.

---

## 📋 Tabla de Contenidos

- [Requisitos Previos](#-requisitos-previos)
- [Preparación del Entorno](#-preparación-del-entorno)
- [Despliegue con Docker](#-despliegue-con-docker)
- [Configuración de Nginx](#-configuración-de-nginx)
- [SSL/TLS (HTTPS)](#-ssltls-https)
- [Migraciones de Base de Datos](#-migraciones-de-base-de-datos)
- [Monitoreo y Logs](#-monitoreo-y-logs)
- [Backup y Recuperación](#-backup-y-recuperación)
- [Troubleshooting](#-troubleshooting)

---

## ✅ Requisitos Previos

### Servidor

- **SO**: Ubuntu 20.04 LTS o superior (recomendado)
- **RAM**: Mínimo 4GB, recomendado 8GB+
- **CPU**: Mínimo 2 cores, recomendado 4+ cores
- **Disco**: Mínimo 20GB SSD
- **Red**: IP pública o dominio configurado

### Software

- Docker 20.10+
- Docker Compose 2.0+
- Git
- (Opcional) Nginx para reverse proxy
- (Opcional) Certbot para SSL/TLS

---

## 🛠️ Preparación del Entorno

### 1. Actualizar el Sistema

```bash
sudo apt update && sudo apt upgrade -y
```

### 2. Instalar Docker

```bash
# Instalar Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# Activar Docker
sudo systemctl enable docker
sudo systemctl start docker

# Verificar instalación
docker --version
```

### 3. Instalar Docker Compose

```bash
# Descargar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Dar permisos de ejecución
sudo chmod +x /usr/local/bin/docker-compose

# Verificar instalación
docker-compose --version
```

### 4. Configurar Firewall

```bash
# Permitir SSH (importante!)
sudo ufw allow 22/tcp

# Permitir HTTP y HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Habilitar firewall
sudo ufw enable

# Verificar estado
sudo ufw status
```

---

## 🐳 Despliegue con Docker

### 1. Clonar el Repositorio

```bash
# Crear directorio para la aplicación
sudo mkdir -p /opt/ecommerce-api
cd /opt/ecommerce-api

# Clonar repositorio
git clone <url-del-repositorio> .

# O copiar archivos vía SCP
scp -r /local/path usuario@servidor:/opt/ecommerce-api
```

### 2. Configurar Variables de Entorno

```bash
# Copiar plantilla de producción
cp .env.production.example .env.production

# Editar con valores reales
nano .env.production
```

**Configuración de Producción** (`.env.production`):

```env
# ===== BASE DE DATOS =====
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_prod
POSTGRES_USER=postgres
POSTGRES_PASSWORD=TU_PASSWORD_SEGURO_AQUI_123!

# Pool de Conexiones (para 400+ usuarios)
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=100
DB_POOL_TIMEOUT=10
DB_POOL_RECYCLE=3600

# ===== REDIS =====
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_ENABLED=true
REDIS_CACHE_TTL=300
REDIS_MAX_CONNECTIONS=50

# ===== UVICORN =====
UVICORN_WORKERS=4
UVICORN_HOST=0.0.0.0
UVICORN_PORT=8000
UVICORN_LOG_LEVEL=info

# ===== RATE LIMITING =====
RATE_LIMIT_ENABLED=true
RATE_LIMIT_CALLS=100
RATE_LIMIT_PERIOD=60

# ===== CORS =====
CORS_ORIGINS=https://tudominio.com,https://www.tudominio.com

# ===== LOGGING =====
LOG_LEVEL=INFO
LOG_FORMAT=json
```

### 3. Levantar los Servicios

```bash
# Construir y levantar en segundo plano
docker-compose -f docker-compose.production.yaml up -d --build

# Verificar que estén corriendo
docker-compose -f docker-compose.production.yaml ps

# Ver logs
docker-compose -f docker-compose.production.yaml logs -f
```

### 4. Verificar el Despliegue

```bash
# Health check
curl http://localhost:8000/health_check

# Deberías ver:
# {"status":"healthy", "checks": {...}}
```

### 5. Configurar Auto-inicio

```bash
# Crear servicio systemd
sudo nano /etc/systemd/system/ecommerce-api.service
```

**Contenido del archivo**:

```ini
[Unit]
Description=E-commerce API
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/ecommerce-api
ExecStart=/usr/local/bin/docker-compose -f docker-compose.production.yaml up -d
ExecStop=/usr/local/bin/docker-compose -f docker-compose.production.yaml down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

**Habilitar el servicio**:

```bash
sudo systemctl daemon-reload
sudo systemctl enable ecommerce-api
sudo systemctl start ecommerce-api
sudo systemctl status ecommerce-api
```

---

## 🌐 Configuración de Nginx

### 1. Instalar Nginx

```bash
sudo apt install nginx -y
sudo systemctl enable nginx
sudo systemctl start nginx
```

### 2. Configurar Reverse Proxy

```bash
# Crear configuración del sitio
sudo nano /etc/nginx/sites-available/ecommerce-api
```

**Contenido del archivo**:

```nginx
upstream ecommerce_backend {
    # Balanceo de carga entre workers
    least_conn;
    server localhost:8000 max_fails=3 fail_timeout=30s;
}

server {
    listen 80;
    listen [::]:80;
    server_name tudominio.com www.tudominio.com;

    # Logging
    access_log /var/log/nginx/ecommerce-api-access.log;
    error_log /var/log/nginx/ecommerce-api-error.log;

    # Limits
    client_max_body_size 10M;
    client_body_timeout 12;
    client_header_timeout 12;
    send_timeout 10;

    # Security Headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # API Proxy
    location / {
        proxy_pass http://ecommerce_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Health Check (no auth required)
    location /health_check {
        proxy_pass http://ecommerce_backend/health_check;
        access_log off;
    }
}
```

**Habilitar el sitio**:

```bash
# Crear enlace simbólico
sudo ln -s /etc/nginx/sites-available/ecommerce-api /etc/nginx/sites-enabled/

# Verificar configuración
sudo nginx -t

# Recargar Nginx
sudo systemctl reload nginx
```

---

## 🔒 SSL/TLS (HTTPS)

### 1. Instalar Certbot

```bash
sudo apt install certbot python3-certbot-nginx -y
```

### 2. Obtener Certificado SSL

```bash
# Generar certificado (sigue las instrucciones)
sudo certbot --nginx -d tudominio.com -d www.tudominio.com

# Certbot automáticamente:
# 1. Genera el certificado
# 2. Modifica la configuración de Nginx
# 3. Configura renovación automática
```

### 3. Verificar Renovación Automática

```bash
# Probar renovación (dry run)
sudo certbot renew --dry-run

# Ver timer de renovación
sudo systemctl status certbot.timer
```

### 4. Configuración HTTPS Final

Certbot modificará tu configuración de Nginx automáticamente. El resultado será:

```nginx
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name tudominio.com www.tudominio.com;

    ssl_certificate /etc/letsencrypt/live/tudominio.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/tudominio.com/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    # ... resto de la configuración
}

server {
    # Redirect HTTP to HTTPS
    listen 80;
    listen [::]:80;
    server_name tudominio.com www.tudominio.com;
    return 301 https://$server_name$request_uri;
}
```

---

## 🗄️ Migraciones de Base de Datos

### 1. Aplicar Migraciones en Producción

```bash
# Conectar al contenedor de la API
docker exec -it ecommerce_api_prod /bin/bash

# Dentro del contenedor:
alembic upgrade head

# Verificar versión actual
alembic current

# Salir
exit
```

### 2. Crear Nueva Migración

```bash
# 1. Modificar modelos en local
# 2. Generar migración
alembic revision --autogenerate -m "Descripción del cambio"

# 3. Revisar archivo generado en alembic/versions/
# 4. Commitear a Git
git add alembic/versions/*
git commit -m "Add migration: descripción"

# 5. Desplegar en producción
git pull
docker-compose -f docker-compose.production.yaml restart api
docker exec -it ecommerce_api_prod alembic upgrade head
```

### 3. Rollback de Migración

```bash
# Revertir última migración
docker exec -it ecommerce_api_prod alembic downgrade -1

# Revertir a versión específica
docker exec -it ecommerce_api_prod alembic downgrade <revision_id>
```

---

## 📊 Monitoreo y Logs

### 1. Ver Logs en Tiempo Real

```bash
# Todos los servicios
docker-compose -f docker-compose.production.yaml logs -f

# Solo API
docker-compose -f docker-compose.production.yaml logs -f api

# Solo PostgreSQL
docker-compose -f docker-compose.production.yaml logs -f postgres

# Últimas 100 líneas
docker-compose -f docker-compose.production.yaml logs --tail=100 api
```

### 2. Logs Persistentes

Los logs se almacenan en:

```
/opt/ecommerce-api/logs/
├── app.log          # Logs de aplicación
├── error.log        # Solo errores
└── access.log       # Accesos HTTP
```

### 3. Rotación de Logs

```bash
# Crear configuración de logrotate
sudo nano /etc/logrotate.d/ecommerce-api
```

**Contenido**:

```
/opt/ecommerce-api/logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 root root
    sharedscripts
    postrotate
        docker-compose -f /opt/ecommerce-api/docker-compose.production.yaml restart api > /dev/null
    endscript
}
```

### 4. Monitoreo de Salud

**Script de monitoreo**:

```bash
#!/bin/bash
# /opt/ecommerce-api/health_check.sh

HEALTH_URL="http://localhost:8000/health_check"
RESPONSE=$(curl -s $HEALTH_URL)
STATUS=$(echo $RESPONSE | jq -r '.status')

if [ "$STATUS" != "healthy" ]; then
    echo "⚠️  API is unhealthy: $RESPONSE"
    # Enviar alerta (email, Slack, etc.)
    # Reiniciar servicios si es necesario
fi
```

**Agregar a crontab**:

```bash
# Ejecutar cada 5 minutos
*/5 * * * * /opt/ecommerce-api/health_check.sh
```

---

## 💾 Backup y Recuperación

### 1. Backup de Base de Datos

**Script de backup automático**:

```bash
#!/bin/bash
# /opt/ecommerce-api/backup_db.sh

BACKUP_DIR="/opt/backups/ecommerce-db"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/ecommerce_db_$DATE.sql.gz"

# Crear directorio si no existe
mkdir -p $BACKUP_DIR

# Realizar backup
docker exec ecommerce_postgres_prod pg_dump -U postgres ecommerce_prod | gzip > $BACKUP_FILE

# Mantener solo últimos 7 días
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete

echo "✅ Backup completado: $BACKUP_FILE"
```

**Agregar a crontab (diario a las 2 AM)**:

```bash
0 2 * * * /opt/ecommerce-api/backup_db.sh
```

### 2. Restaurar Backup

```bash
# Descomprimir y restaurar
gunzip < /opt/backups/ecommerce-db/ecommerce_db_20251118_020000.sql.gz | \
docker exec -i ecommerce_postgres_prod psql -U postgres -d ecommerce_prod
```

### 3. Backup de Volúmenes Docker

```bash
# Listar volúmenes
docker volume ls

# Backup de volumen PostgreSQL
docker run --rm \
  -v ecommerce_postgres_data:/data \
  -v /opt/backups:/backup \
  alpine tar czf /backup/postgres_volume_$(date +%Y%m%d).tar.gz -C /data .

# Restaurar volumen
docker run --rm \
  -v ecommerce_postgres_data:/data \
  -v /opt/backups:/backup \
  alpine tar xzf /backup/postgres_volume_20251118.tar.gz -C /data
```

---

## 🔧 Troubleshooting

### Problema 1: Contenedores no inician

**Diagnóstico**:

```bash
docker-compose -f docker-compose.production.yaml ps
docker-compose -f docker-compose.production.yaml logs
```

**Soluciones comunes**:

```bash
# Reiniciar servicios
docker-compose -f docker-compose.production.yaml restart

# Reconstruir imágenes
docker-compose -f docker-compose.production.yaml up -d --build

# Limpiar y reiniciar
docker-compose -f docker-compose.production.yaml down -v
docker-compose -f docker-compose.production.yaml up -d
```

### Problema 2: API retorna 502 Bad Gateway

**Causas**:
- API no está corriendo
- Nginx no puede conectar al backend

**Solución**:

```bash
# Verificar que API esté corriendo
docker ps | grep ecommerce_api

# Verificar health check
curl http://localhost:8000/health_check

# Ver logs de Nginx
sudo tail -f /var/log/nginx/ecommerce-api-error.log

# Reiniciar Nginx
sudo systemctl restart nginx
```

### Problema 3: Base de datos llena

**Diagnóstico**:

```bash
# Ver tamaño de base de datos
docker exec -it ecommerce_postgres_prod psql -U postgres -c "
SELECT pg_size_pretty(pg_database_size('ecommerce_prod'));
"

# Ver tamaño por tabla
docker exec -it ecommerce_postgres_prod psql -U postgres -d ecommerce_prod -c "
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

**Soluciones**:

- Aumentar espacio en disco
- Limpiar datos antiguos
- Configurar particionado de tablas

---

## ✅ Checklist de Despliegue

### Pre-Despliegue

- [ ] Servidor preparado (4GB RAM, 2 cores, 20GB SSD)
- [ ] Docker y Docker Compose instalados
- [ ] Firewall configurado (22, 80, 443)
- [ ] Dominio apuntando a la IP del servidor
- [ ] Variables de entorno configuradas
- [ ] Certificado SSL obtenido

### Despliegue

- [ ] Código clonado/copiado al servidor
- [ ] Docker Compose levantado
- [ ] Health check respondiendo correctamente
- [ ] Nginx configurado y funcionando
- [ ] SSL/HTTPS funcionando
- [ ] Migraciones aplicadas

### Post-Despliegue

- [ ] Logs rotando correctamente
- [ ] Backups automáticos configurados
- [ ] Monitoreo de salud configurado
- [ ] Auto-inicio habilitado (systemd)
- [ ] Pruebas de carga ejecutadas
- [ ] Documentación actualizada

---

**¡Despliegue completado!** 🎉 Tu API está lista para producción.

**Documento actualizado**: 2025-11-18
**Versión**: 2.0
**Mantenedor**: Equipo de DevOps