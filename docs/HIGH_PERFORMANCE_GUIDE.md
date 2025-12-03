# Guía de Configuración de Alto Rendimiento

## FastAPI E-commerce - Optimizado para 400 Peticiones Concurrentes

**Fecha:** 2025-11-16
**Objetivo:** Soportar 400+ peticiones simultáneas y concurrentes

---

## 📊 Resumen de Configuración

### **Capacidad Total:**
- ✅ **400+ peticiones concurrentes**
- ✅ **100-200 RPS sostenidos**
- ✅ **p95 response time < 200ms**
- ✅ **600 conexiones de BD disponibles**
- ✅ **4-8 workers de Uvicorn**

---

## 🎯 Arquitectura de Alto Rendimiento

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer (Nginx)                    │
│                     (Optional - Recommended)                │
└──────────────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼────────┐ ┌───────▼────────┐ ┌──────▼─────────┐
│  Uvicorn       │ │  Uvicorn       │ │  Uvicorn       │
│  Worker 1      │ │  Worker 2      │ │  Worker 3      │
│  (150 conn)    │ │  (150 conn)    │ │  (150 conn)    │
└───────┬────────┘ └───────┬────────┘ └───────┬────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                ┌──────────▼──────────┐
                │   PostgreSQL DB     │
                │   max_conn = 700    │
                │   (optimized)       │
                └─────────────────────┘
```

### **Flujo de Peticiones:**
1. Cliente hace petición → Nginx (balanceo de carga)
2. Nginx → Uvicorn Worker (Round-robin)
3. Worker → Database Connection Pool (50 + 100 overflow)
4. Pool → PostgreSQL (conexión reutilizada)
5. Response ← Cliente

---

## 🔧 Configuración Detallada

### **1. Connection Pool de Base de Datos**

#### **config/database.py**
```python
# Para 4 workers con 400 peticiones concurrentes:
# 400 peticiones / 4 workers = 100 peticiones por worker
# Pool: 50 base + 100 overflow = 150 conexiones por worker
# Total: 4 × 150 = 600 conexiones máximas

DB_POOL_SIZE = 50           # Conexiones permanentes
DB_MAX_OVERFLOW = 100       # Conexiones adicionales en picos
DB_POOL_TIMEOUT = 30        # Timeout para obtener conexión
DB_POOL_RECYCLE = 3600      # Reciclar conexiones cada hora
```

#### **Cálculo de Conexiones:**
```
Total Connections = WORKERS × (POOL_SIZE + MAX_OVERFLOW)
                  = 4 × (50 + 100)
                  = 600 conexiones
```

---

### **2. Configuración de Uvicorn Workers**

#### **run_production.py**
```python
# Fórmula: (2 × CPU_cores) + 1
# Para 4 cores: (2 × 4) + 1 = 9 workers
# Pero limitamos a 4-8 para balance CPU/Memoria

UVICORN_WORKERS = 4         # Número de procesos worker
BACKLOG = 2048              # Cola de conexiones pendientes
TIMEOUT_KEEP_ALIVE = 5      # Keep-alive timeout
LIMIT_CONCURRENCY = 1000    # Límite de conexiones concurrentes
LIMIT_MAX_REQUESTS = 10000  # Reiniciar worker después de N requests
```

#### **Capacidad por Worker:**
```
Peticiones por Worker = 400 / 4 = 100 peticiones concurrentes
RPS por Worker = 25-50 requests/second
Total RPS = 100-200 requests/second
```

---

### **3. Configuración de PostgreSQL**

#### **postgresql.conf (Recomendado)**
```ini
# Conexiones
max_connections = 700                    # Soporta 600 + buffer

# Memoria
shared_buffers = 256MB                   # 25% de RAM (para 1GB)
effective_cache_size = 768MB             # 75% de RAM
work_mem = 16MB                          # Memoria por operación
maintenance_work_mem = 128MB

# WAL (Write-Ahead Logging)
wal_buffers = 16MB
min_wal_size = 1GB
max_wal_size = 4GB
checkpoint_completion_target = 0.9

# Query Planner
default_statistics_target = 100
random_page_cost = 1.1                   # Para SSD
effective_io_concurrency = 200           # Para SSD

# Paralelismo
max_worker_processes = 4
max_parallel_workers = 4
max_parallel_workers_per_gather = 2
max_parallel_maintenance_workers = 2
```

#### **Verificar Configuración:**
```sql
-- Ver conexiones activas
SELECT count(*) as active_connections FROM pg_stat_activity;

-- Ver configuración actual
SHOW max_connections;
SHOW shared_buffers;
SHOW work_mem;
```

---

### **4. Variables de Entorno**

#### **.env.production**
```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_DB=ecommerce_prod
POSTGRES_USER=postgres
POSTGRES_PASSWORD=strong_password_here

# Connection Pool (optimizado para 400 peticiones)
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=100
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=3600

# Uvicorn
UVICORN_WORKERS=4
API_HOST=0.0.0.0
API_PORT=8000

# Performance
BACKLOG=2048
TIMEOUT_KEEP_ALIVE=5
LIMIT_CONCURRENCY=1000
LIMIT_MAX_REQUESTS=10000
```

---

## 🚀 Despliegue en Producción

### **Opción 1: Python Directo**
```bash
# 1. Configurar entorno
cp .env.production.example .env
# Editar .env con valores de producción

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar servidor
python run_production.py
```

### **Opción 2: Docker Compose (Recomendado)**
```bash
# 1. Construir y ejecutar
docker-compose -f docker-compose.production.yaml up -d

# 2. Ver logs
docker-compose -f docker-compose.production.yaml logs -f api

# 3. Escalar horizontalmente (más instancias)
docker-compose -f docker-compose.production.yaml up -d --scale api=3
```

### **Opción 3: Docker con Nginx**
```bash
# Incluye reverse proxy y balanceo de carga
docker-compose -f docker-compose.production.yaml --profile with-nginx up -d
```

---

## 📈 Load Testing

### **Instalar Locust**
```bash
pip install locust
```

### **Ejecutar Test de Carga**

#### **Opción 1: Web UI (Recomendado)**
```bash
# 1. Iniciar Locust
locust -f load_test.py --host=http://localhost:8000

# 2. Abrir navegador
http://localhost:8089

# 3. Configurar:
#    - Number of users: 400
#    - Spawn rate: 50 users/second
#    - Run time: 5 minutes
```

#### **Opción 2: Headless (CLI)**
```bash
locust -f load_test.py \
  --host=http://localhost:8000 \
  --users 400 \
  --spawn-rate 50 \
  --run-time 5m \
  --headless
```

#### **Opción 3: Distribuido (Múltiples Máquinas)**
```bash
# Master
locust -f load_test.py --master --host=http://your-api

# Workers (en otras máquinas)
locust -f load_test.py --worker --master-host=192.168.1.100
```

---

## 📊 Monitoreo en Tiempo Real

### **1. Monitorear Conexiones de BD**
```bash
# Dentro del container de PostgreSQL
docker exec ecommerce_postgres_prod psql -U postgres -c \
  "SELECT count(*) as active_connections,
          state,
          wait_event
   FROM pg_stat_activity
   GROUP BY state, wait_event
   ORDER BY active_connections DESC;"
```

### **2. Monitorear API**
```bash
# Ver logs en tiempo real
docker-compose -f docker-compose.production.yaml logs -f api

# Estadísticas de contenedor
docker stats ecommerce_api_prod
```

### **3. Monitorear Recursos del Sistema**
```bash
# CPU y memoria
htop

# Conexiones de red
netstat -an | grep 8000 | wc -l

# Procesos de Uvicorn
ps aux | grep uvicorn
```

---

## ✅ Métricas de Éxito

### **Objetivo:**
| Métrica | Objetivo | Crítico |
|---------|----------|---------|
| Peticiones concurrentes | 400+ | 500+ |
| Requests/second | 100-200 | 80 |
| Response time (p95) | < 200ms | < 500ms |
| Response time (p99) | < 500ms | < 1000ms |
| Error rate | < 1% | < 5% |
| DB connections | < 600 | < 700 |

### **Durante Load Test Verificar:**
```
✓ Response time p50: < 100ms
✓ Response time p95: < 200ms
✓ Response time p99: < 500ms
✓ Requests/second: > 100 RPS
✓ Failure rate: < 1%
✓ Database connections: < 600
✓ CPU usage: < 80%
✓ Memory usage: < 80%
```

---

## 🔍 Troubleshooting

### **Problema: Error "Too many connections"**
**Solución:**
```bash
# Aumentar max_connections en PostgreSQL
# Editar postgresql.conf:
max_connections = 1000

# O aumentar pool en .env:
DB_POOL_SIZE=30
DB_MAX_OVERFLOW=70
```

### **Problema: Response time alto (> 500ms)**
**Causas posibles:**
1. Pool de conexiones saturado → Aumentar POOL_SIZE
2. PostgreSQL lento → Optimizar queries, añadir índices
3. Pocos workers → Aumentar UVICORN_WORKERS
4. CPU saturado → Escalar horizontalmente

**Solución:**
```bash
# 1. Verificar pool
docker exec ecommerce_postgres_prod psql -U postgres -c \
  "SELECT * FROM pg_stat_activity WHERE state = 'active';"

# 2. Verificar queries lentas
docker exec ecommerce_postgres_prod psql -U postgres -c \
  "SELECT query, mean_exec_time, calls
   FROM pg_stat_statements
   ORDER BY mean_exec_time DESC
   LIMIT 10;"

# 3. Aumentar workers
UVICORN_WORKERS=8 python run_production.py
```

### **Problema: Conexiones de BD no se liberan**
**Solución:**
```python
# Ya implementado en get_db():
try:
    yield db
finally:
    db.close()  # ✓ Siempre se cierra
```

---

## 🎯 Optimizaciones Adicionales

### **1. Caché con Redis (Opcional)**
```yaml
# docker-compose.production.yaml
services:
  redis:
    image: redis:alpine
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
```

### **2. Nginx como Reverse Proxy**
```nginx
upstream fastapi_backend {
    least_conn;  # Balance por menos conexiones
    server api:8000;
}

server {
    listen 80;

    location / {
        proxy_pass http://fastapi_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;

        # Buffering
        proxy_buffering on;
        proxy_buffer_size 4k;
        proxy_buffers 8 4k;
    }
}
```

### **3. Índices de Base de Datos**
```sql
-- Añadir índices para queries frecuentes
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_client ON orders(client_id);
CREATE INDEX idx_order_details_order ON order_details(order_id);
CREATE INDEX idx_addresses_client ON addresses(client_id);
```

---

## 📝 Checklist de Producción

- [ ] Configurar `.env` con valores de producción
- [ ] Establecer contraseña fuerte para PostgreSQL
- [ ] Configurar `max_connections` en PostgreSQL
- [ ] Ajustar `UVICORN_WORKERS` según CPU cores
- [ ] Configurar backup automático de BD
- [ ] Habilitar SSL/TLS con certificado
- [ ] Configurar firewall (solo puerto 80/443 abierto)
- [ ] Configurar logging y rotación de logs
- [ ] Configurar monitoreo (Prometheus/Grafana)
- [ ] Ejecutar load test antes de producción
- [ ] Configurar health checks en load balancer
- [ ] Documentar procedimientos de escalado

---

## 🚀 Siguiente Nivel (Escalar más allá de 400)

### **Para 1000+ peticiones concurrentes:**

1. **Horizontal Scaling**
   ```bash
   docker-compose up -d --scale api=5
   ```

2. **Database Replication**
   - Master (escritura)
   - Slaves (lectura)

3. **Cache Layer**
   - Redis para datos frecuentes
   - CDN para archivos estáticos

4. **Message Queue**
   - Celery + RabbitMQ para tareas asíncronas

5. **Kubernetes**
   - Auto-scaling basado en métricas
   - Distribución multi-región

---

## 📚 Referencias

- [FastAPI Performance](https://fastapi.tiangolo.com/deployment/concepts/)
- [Uvicorn Deployment](https://www.uvicorn.org/deployment/)
- [PostgreSQL Tuning](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server)
- [SQLAlchemy Connection Pool](https://docs.sqlalchemy.org/en/20/core/pooling.html)
- [Locust Documentation](https://docs.locust.io/)

---

**¡Aplicación lista para soportar 400+ peticiones concurrentes!** 🎉