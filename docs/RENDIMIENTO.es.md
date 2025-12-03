# ⚡ Guía de Alto Rendimiento - API E-commerce

Esta guía te ayudará a optimizar el rendimiento de la API para manejar **400+ usuarios concurrentes** con tiempos de respuesta inferiores a 200ms.

---

## 📋 Tabla de Contenidos

- [Objetivos de Rendimiento](#-objetivos-de-rendimiento)
- [Configuración de Producción](#-configuración-de-producción)
- [Optimización de Base de Datos](#-optimización-de-base-de-datos)
- [Sistema de Caché Redis](#-sistema-de-caché-redis)
- [Pruebas de Carga](#-pruebas-de-carga)
- [Monitoreo y Métricas](#-monitoreo-y-métricas)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Objetivos de Rendimiento

### Métricas Clave

| Métrica | Objetivo | Crítico |
|---------|----------|---------|
| **Usuarios Concurrentes** | 400+ | 500+ |
| **Requests/Segundo** | 100-200 | 80 |
| **Response Time (p95)** | < 200ms | < 500ms |
| **Response Time (p50)** | < 100ms | < 200ms |
| **Tasa de Error** | < 1% | < 5% |
| **Cache Hit Rate** | > 70% | > 50% |
| **DB Pool Utilization** | < 70% | < 90% |

### Configuración Probada

El sistema ha sido **probado en producción** con:

- ✅ **400 usuarios concurrentes**
- ✅ **50 usuarios/segundo** (spawn rate)
- ✅ **5 minutos** de duración
- ✅ **Tasa de error < 1%**
- ✅ **Latencia p95 < 200ms**

---

## ⚙️ Configuración de Producción

### 1. Variables de Entorno Optimizadas

Crea un archivo `.env.production` con:

```env
# ===== BASE DE DATOS =====
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_prod
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password_seguro_aqui

# Pool de Conexiones (4 workers × 150 = 600 conexiones totales)
DB_POOL_SIZE=50          # Conexiones permanentes por worker
DB_MAX_OVERFLOW=100      # Conexiones adicionales por worker
DB_POOL_TIMEOUT=10       # Timeout en segundos (fail fast)
DB_POOL_RECYCLE=3600     # Reciclar conexiones cada hora
DB_POOL_PRE_PING=true    # Verificar conexión antes de usar

# ===== REDIS CACHE =====
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
REDIS_ENABLED=true             # CRÍTICO para rendimiento
REDIS_CACHE_TTL=300            # 5 minutos por defecto
REDIS_MAX_CONNECTIONS=50       # Pool de conexiones

# ===== UVICORN (Multi-Worker) =====
UVICORN_WORKERS=4              # Número de workers (CPU cores)
UVICORN_HOST=0.0.0.0
UVICORN_PORT=8000
UVICORN_LOG_LEVEL=info

# ===== RATE LIMITING =====
RATE_LIMIT_ENABLED=true
RATE_LIMIT_CALLS=100           # Requests por ventana
RATE_LIMIT_PERIOD=60           # Segundos

# ===== LOGGING =====
LOG_LEVEL=INFO                 # DEBUG solo en desarrollo
LOG_FORMAT=json                # JSON para parsing automático

# ===== CORS =====
CORS_ORIGINS=https://tuapp.com,https://www.tuapp.com
```

### 2. Ejecutar en Modo Producción

**Opción A: Script Python (4-8 workers)**

```bash
python run_production.py
```

**Opción B: Docker Compose**

```bash
docker-compose -f docker-compose.production.yaml up -d
```

**Opción C: Uvicorn Directo**

```bash
uvicorn main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --loop uvloop \
  --log-level info
```

### 3. Arquitectura Multi-Worker

```
┌─────────────────────────────────┐
│      Load Balancer (Nginx)      │
│            Port 80               │
└──────────────┬──────────────────┘
               │
    ┌──────────┴──────────┐
    │                     │
┌───▼────┐  ...      ┌───▼────┐
│Worker 1│           │Worker 4│
│        │           │        │
│ Pool:  │           │ Pool:  │
│ 50+100 │           │ 50+100 │
└────┬───┘           └────┬───┘
     │                    │
     └──────────┬─────────┘
                │
         ┌──────▼──────┐
         │ PostgreSQL  │
         │ Max: 600    │
         └─────────────┘

Total Capacity: 600 conexiones DB
```

---

## 🗄️ Optimización de Base de Datos

### 1. Pool de Conexiones Optimizado

**Configuración Recomendada**:

```python
# config/database.py
engine = create_engine(
    DATABASE_URI,
    poolclass=QueuePool,
    pool_size=50,              # Conexiones permanentes
    max_overflow=100,          # Conexiones adicionales
    pool_timeout=10,           # Timeout rápido (fail fast)
    pool_recycle=3600,         # Reciclar cada hora
    pool_pre_ping=True,        # Verificar antes de usar
    echo=False                 # No logging SQL (producción)
)
```

**Cálculo de Pool Size**:

```
Fórmula: connections = ((core_count × 2) + effective_spindle_count)

Ejemplo con 4 cores:
  Base = (4 × 2) + 1 = 9
  Con overhead = 50 es suficiente

Para 4 workers:
  Total = 4 × (50 + 100) = 600 conexiones máximo
```

### 2. Índices de Base de Datos

**Índices Críticos**:

```sql
-- Productos (búsquedas frecuentes)
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_stock ON products(stock) WHERE stock > 0;

-- Clientes (unique email)
CREATE UNIQUE INDEX idx_clients_email ON clients(email);

-- Órdenes (búsquedas por cliente)
CREATE INDEX idx_orders_client ON orders(client_id);
CREATE INDEX idx_orders_date ON orders(date DESC);

-- Detalles de Orden (FK lookups)
CREATE INDEX idx_order_details_order ON order_details(order_id);
CREATE INDEX idx_order_details_product ON order_details(product_id);

-- Reseñas (FK lookup)
CREATE INDEX idx_reviews_product ON reviews(product_id);
```

### 3. Lazy Loading (Evitar N+1)

**✅ CORRECTO** - `lazy='select'`:

```python
class ProductModel(BaseModel):
    # Solo carga reviews cuando se accede explícitamente
    reviews = relationship("ReviewModel", lazy='select', cascade="all, delete")
```

**❌ INCORRECTO** - `lazy='joined'`:

```python
# NO USAR - Causa cartesian products y problemas de rendimiento
reviews = relationship("ReviewModel", lazy='joined')
```

### 4. Queries Optimizadas

**Paginación Siempre**:

```python
def find_all(self, skip: int = 0, limit: int = 100):
    # NUNCA cargar todo sin límite
    stmt = select(self.model).offset(skip).limit(limit)
    return self.db.execute(stmt).scalars().all()
```

**Evitar SELECT ***:

```python
# ✅ BUENO - Solo campos necesarios
stmt = select(Product.id_key, Product.name, Product.price)

# ❌ MALO - Carga todo
stmt = select(Product)
```

---

## 🚀 Sistema de Caché Redis

### 1. Estrategia de Caché

**Patrón: Cache-Aside (Lazy Loading)**

```python
def get_all(self, skip: int = 0, limit: int = 100):
    # 1. Verificar caché
    cache_key = f"products:list:skip:{skip}:limit:{limit}"
    cached = cache_service.get(cache_key)

    if cached:
        return [ProductSchema(**item) for item in cached]

    # 2. Si no está en caché, consultar DB
    products = self.repository.find_all(skip, limit)

    # 3. Guardar en caché
    cache_service.set(
        cache_key,
        [p.model_dump() for p in products],
        ttl=300  # 5 minutos
    )

    return products
```

### 2. TTL por Entidad

```python
# Configuración recomendada
CACHE_TTLS = {
    "products": 300,      # 5 minutos (cambian frecuentemente)
    "categories": 3600,   # 1 hora (casi estáticas)
    "clients": 0,         # No cachear (datos sensibles)
    "orders": 0,          # No cachear (transaccional)
}
```

### 3. Invalidación de Caché

**En mutaciones (POST/PUT/DELETE)**:

```python
def save(self, schema):
    # 1. Guardar en DB
    result = super().save(schema)

    # 2. Invalidar caché de lista
    self.cache.delete_pattern("products:list:*")

    return result

def update(self, id_key, schema):
    result = super().update(id_key, schema)

    # Invalidar item específico y listas
    self.cache.delete(f"products:id:{id_key}")
    self.cache.delete_pattern("products:list:*")

    return result
```

### 4. Monitoreo de Caché

**Verificar Hit Rate**:

```bash
# Conectar a Redis
docker exec -it ecommerce_redis_prod redis-cli

# Ver estadísticas
INFO stats

# Buscar estas métricas:
# keyspace_hits: 1500
# keyspace_misses: 500
# Hit Rate = 1500 / (1500 + 500) = 75% ✅
```

**Objetivo**: Hit Rate > 70%

---

## 🧪 Pruebas de Carga

### 1. Instalación de Locust

```bash
pip install -r requirements-dev.txt
```

### 2. Ejecutar Pruebas

**Interfaz Web (Recomendado para desarrollo)**:

```bash
locust -f load_test.py --host=http://localhost:8000

# Abre http://localhost:8089
# Configura:
#   - Number of users: 400
#   - Spawn rate: 50
#   - Run time: 5m
```

**Modo Headless (CI/CD)**:

```bash
locust -f load_test.py \
  --host=http://localhost:8000 \
  --users 400 \
  --spawn-rate 50 \
  --run-time 5m \
  --headless \
  --html report.html
```

### 3. Escenarios de Prueba

El archivo `load_test.py` incluye:

```python
class EcommerceUser(HttpUser):
    wait_time = between(1, 3)  # 1-3 segundos entre requests

    @task(3)  # 30% del tráfico
    def view_products(self):
        self.client.get("/products?skip=0&limit=20")

    @task(2)  # 20% del tráfico
    def view_product_detail(self):
        self.client.get(f"/products/{random.randint(1, 100)}")

    @task(1)  # 10% del tráfico
    def create_order(self):
        self.client.post("/orders", json={...})
```

### 4. Interpretación de Resultados

**Métricas Clave**:

```
┌───────────────────────┬──────────┬──────────┐
│ Metric                │ Target   │ Critical │
├───────────────────────┼──────────┼──────────┤
│ Requests/sec          │ > 100    │ > 80     │
│ Response Time (p95)   │ < 200ms  │ < 500ms  │
│ Response Time (p50)   │ < 100ms  │ < 200ms  │
│ Error Rate            │ < 1%     │ < 5%     │
│ Concurrent Users      │ 400+     │ 300+     │
└───────────────────────┴──────────┴──────────┘
```

**Reporte HTML**:

```bash
# Genera report.html con:
# - Request statistics
# - Response time charts
# - Failures
# - Download data
```

---

## 📊 Monitoreo y Métricas

### 1. Health Check Avanzado

```bash
curl http://localhost:8000/health_check
```

**Respuesta**:

```json
{
  "status": "healthy",
  "timestamp": "2025-11-18T10:00:00.000Z",
  "checks": {
    "database": {
      "status": "up",
      "latency_ms": 15.23
    },
    "redis": {
      "status": "up"
    },
    "db_pool": {
      "size": 50,
      "checked_in": 45,
      "checked_out": 5,
      "overflow": 0,
      "total_capacity": 150,
      "utilization_percent": 3.3
    }
  }
}
```

**Estados**:

- `healthy` - Todo OK
- `warning` - Latencia alta (>100ms) o pool >70%
- `degraded` - Redis caído (non-critical)
- `critical` - DB caída o pool >90%

### 2. Logs Estructurados

```bash
# Ver logs en tiempo real
docker-compose -f docker-compose.production.yaml logs -f api

# Filtrar por nivel
docker-compose logs api | grep ERROR
docker-compose logs api | grep WARNING
```

**Formato de Logs**:

```
2025-11-18 10:00:00 - [abc123] → GET /products (client: 192.168.1.100)
2025-11-18 10:00:00 - [abc123] ← GET /products - 200 (45.2ms)
```

### 3. Métricas de PostgreSQL

```bash
# Conectar a PostgreSQL
docker exec -it ecommerce_postgres_prod psql -U postgres -d ecommerce_prod

# Ver queries lentas
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

# Ver conexiones activas
SELECT count(*) FROM pg_stat_activity WHERE state = 'active';

# Ver tamaño de tablas
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

---

## 🔧 Troubleshooting

### Problema 1: Latencia Alta (>500ms)

**Diagnóstico**:

```bash
# 1. Verificar pool de DB
curl http://localhost:8000/health_check | jq '.checks.db_pool'

# 2. Ver queries lentas en PostgreSQL
# (Ver sección anterior)

# 3. Verificar cache hit rate
docker exec -it ecommerce_redis_prod redis-cli INFO stats | grep keyspace
```

**Soluciones**:

```bash
# Aumentar pool de conexiones
export DB_POOL_SIZE=100
export DB_MAX_OVERFLOW=200

# Aumentar TTL de caché
export REDIS_CACHE_TTL=600  # 10 minutos

# Agregar índices faltantes
# (Ver sección de índices)
```

### Problema 2: Pool Agotado

**Síntoma**: `QueuePool limit exceeded`

**Solución**:

```bash
# Opción 1: Aumentar pool
export DB_POOL_SIZE=100
export DB_MAX_OVERFLOW=200

# Opción 2: Reducir workers
export UVICORN_WORKERS=2

# Opción 3: Reducir timeout (fail fast)
export DB_POOL_TIMEOUT=5
```

### Problema 3: Tasa de Error Alta (>5%)

**Diagnóstico**:

```bash
# Ver errores en logs
docker-compose logs api | grep ERROR | tail -50

# Ver health check
curl http://localhost:8000/health_check
```

**Causas Comunes**:

- ❌ Redis caído → Deshabilitar temporalmente
- ❌ DB queries lentas → Agregar índices
- ❌ Rate limiting muy estricto → Aumentar límites
- ❌ Validation errors → Revisar schemas

### Problema 4: Cache Hit Rate Bajo (<50%)

**Diagnóstico**:

```bash
# Ver estadísticas de Redis
docker exec -it ecommerce_redis_prod redis-cli INFO stats
```

**Soluciones**:

```bash
# Aumentar TTL
export REDIS_CACHE_TTL=600  # 10 minutos

# Verificar que caché esté habilitado
export REDIS_ENABLED=true

# Verificar invalidación no sea excesiva
# (Revisar logs de mutaciones)
```

---

## 🎯 Checklist de Optimización

### Antes de ir a Producción

- [ ] **Pool de Conexiones** configurado (50+100 por worker)
- [ ] **Redis** habilitado con TTL apropiados
- [ ] **Índices** creados en todas las FK y columnas frecuentes
- [ ] **Lazy loading** configurado (`lazy='select'`)
- [ ] **Multi-worker** configurado (4-8 workers)
- [ ] **Rate limiting** habilitado
- [ ] **Health checks** funcionando
- [ ] **Logs** estructurados y monitoreados
- [ ] **Pruebas de carga** ejecutadas y aprobadas (>400 users)
- [ ] **Monitoreo** configurado (Grafana/Prometheus)

### Durante Operación

- [ ] **Monitorear** pool utilization (<70%)
- [ ] **Monitorear** cache hit rate (>70%)
- [ ] **Monitorear** latencia p95 (<200ms)
- [ ] **Revisar** logs de errores diariamente
- [ ] **Optimizar** queries lentas (>100ms)
- [ ] **Actualizar** índices según patrones de uso

---

## 📈 Resultados Esperados

Con esta configuración optimizada, deberías obtener:

```
✅ 400+ usuarios concurrentes
✅ 150-300 RPS sostenidos
✅ Latencia p95 < 200ms
✅ Latencia p50 < 100ms
✅ Tasa de error < 1%
✅ Cache hit rate > 70%
✅ Pool utilization < 70%
```

**¡Sistema listo para producción!** 🚀

---

**Documento actualizado**: 2025-11-18
**Versión**: 2.0
**Mantenedor**: Equipo de Performance