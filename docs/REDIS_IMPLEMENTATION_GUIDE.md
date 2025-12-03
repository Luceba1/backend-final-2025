# Guía de Implementación de Redis - FastAPI E-commerce

**Fecha:** 2025-11-16
**Objetivo:** Cache de alto rendimiento y rate limiting

---

## 🎯 Resumen Ejecutivo

Redis ha sido integrado en el proyecto para:
1. ✅ **Reducir latencia** de peticiones de lectura (p95: 165ms → < 50ms esperado)
2. ✅ **Disminuir carga en PostgreSQL** (70-80% reducción para lecturas)
3. ✅ **Rate limiting** (protección contra abuso: 100 req/min por IP)
4. ✅ **Escalabilidad** para > 400 peticiones concurrentes

---

## 📦 Archivos Modificados/Creados

### **Nuevos Archivos (4):**
1. ✅ `config/redis_config.py` - Configuración y conexión de Redis
2. ✅ `services/cache_service.py` - Servicio de caché con métodos útiles
3. ✅ `middleware/rate_limiter.py` - Middleware de rate limiting
4. ✅ `REDIS_IMPLEMENTATION_GUIDE.md` - Esta documentación

### **Archivos Modificados (7):**
5. ✅ `docker-compose.production.yaml` - Agregado servicio Redis
6. ✅ `requirements.txt` - Agregada dependencia `redis==5.0.1`
7. ✅ `.env.production.example` - Variables de entorno de Redis
8. ✅ `main.py` - Middleware y eventos de startup/shutdown
9. ✅ `services/product_service.py` - Cache en productos
10. ✅ `services/category_service.py` - Cache en categorías
11. ✅ (pendiente) `load_test.py` - Actualizar para probar cache

---

## 🏗️ Arquitectura con Redis

```
┌─────────────────────────────────────────────────────────────┐
│                    Cliente / Load Balancer                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────┐
        │  Rate Limiter Middleware         │
        │  (100 req/min por IP)           │
        └──────────────┬───────────────────┘
                       │
        ┌──────────────▼──────────────┐
        │   FastAPI Controllers       │
        └──────────────┬──────────────┘
                       │
        ┌──────────────▼──────────────┐
        │   Services (con cache)      │
        └──┬────────────────────────┬─┘
           │                        │
           ▼                        ▼
    ┌──────────┐            ┌───────────┐
    │  Redis   │            │PostgreSQL │
    │  Cache   │            │  Database │
    └──────────┘            └───────────┘

    - Lecturas: Intenta Redis primero, fallback a PostgreSQL
    - Escrituras: Actualiza PostgreSQL + invalida cache
```

---

## ⚙️ Configuración Detallada

### **1. Docker Compose (docker-compose.production.yaml)**

```yaml
redis:
  image: redis:7-alpine
  container_name: ecommerce_redis_prod
  command:
    - "redis-server"
    - "--maxmemory"
    - "256mb"                      # Límite de memoria
    - "--maxmemory-policy"
    - "allkeys-lru"                # Política de evicción (LRU)
    - "--appendonly"
    - "yes"                        # Persistencia AOF
  volumes:
    - redis_data_prod:/data
  ports:
    - "6379:6379"
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 10s
```

**Explicación de Configuración:**
- `maxmemory: 256mb` - Límite de RAM para Redis
- `allkeys-lru` - Elimina claves menos usadas cuando se llena
- `appendonly: yes` - Persiste datos en disco (opcional para cache)

### **2. Variables de Entorno (.env.production.example)**

```bash
# Redis Connection
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=                    # Opcional (dejar vacío si no hay password)
REDIS_MAX_CONNECTIONS=50

# Cache Configuration
REDIS_ENABLED=true                 # true/false para habilitar/deshabilitar
REDIS_CACHE_TTL=300                # TTL por defecto: 5 minutos

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_CALLS=100               # Máximo 100 requests
RATE_LIMIT_PERIOD=60               # Por cada 60 segundos
```

---

## 🔧 Funcionalidades Implementadas

### **1. Cache de Productos**

**Archivo:** `services/product_service.py`

**Endpoints cacheados:**
- `GET /products` - Lista de productos (TTL: 5 minutos)
- `GET /products/{id}` - Producto individual (TTL: 5 minutos)

**Claves de cache:**
```python
"products:list:skip:0:limit:10"     # Lista paginada
"products:id:123"                    # Producto individual
```

**Invalidación automática:**
- `POST /products` - Invalida cache de listas
- `PUT /products/{id}` - Invalida cache del producto + listas
- `DELETE /products/{id}` - Invalida cache del producto + listas

**Ejemplo de uso:**
```python
# Primera petición: Cache MISS (va a PostgreSQL)
GET /products?skip=0&limit=10  → 80ms

# Segunda petición: Cache HIT (viene de Redis)
GET /products?skip=0&limit=10  → 5ms ✅ 16x más rápido
```

### **2. Cache de Categorías**

**Archivo:** `services/category_service.py`

**TTL extendido:** 1 hora (categorías raramente cambian)

**Claves de cache:**
```python
"categories:list:skip:0:limit:100"
"categories:id:5"
```

**Ventaja:** Cache más agresivo porque categorías son casi estáticas.

### **3. Rate Limiting**

**Archivo:** `middleware/rate_limiter.py`

**Funcionamiento:**
```python
# Límite: 100 requests por 60 segundos por IP

Request #1  → Permitido  (X-RateLimit-Remaining: 99)
Request #2  → Permitido  (X-RateLimit-Remaining: 98)
...
Request #100 → Permitido  (X-RateLimit-Remaining: 0)
Request #101 → HTTP 429 TOO MANY REQUESTS ❌
```

**Headers de respuesta:**
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 42
X-RateLimit-Reset: 60
Retry-After: 60
```

**Endpoints excluidos:**
- `/health_check` - No tiene rate limiting

**Configurar por endpoint:**
```python
from middleware.rate_limiter import EndpointRateLimiter

rate_limiter = EndpointRateLimiter(calls=10, period=60)

@app.get("/expensive-operation")
@rate_limiter
async def expensive_operation(request: Request):
    return {"status": "ok"}
```

---

## 🚀 Cómo Usar

### **Opción 1: Docker Compose (Recomendado)**

```bash
# 1. Iniciar servicios (PostgreSQL + Redis + API)
docker-compose -f docker-compose.production.yaml up -d

# 2. Ver logs
docker-compose -f docker-compose.production.yaml logs -f api

# 3. Verificar que Redis esté corriendo
docker exec ecommerce_redis_prod redis-cli ping
# Respuesta esperada: PONG

# 4. Ver estadísticas de cache
docker exec ecommerce_redis_prod redis-cli INFO stats
```

### **Opción 2: Local (desarrollo)**

```bash
# 1. Instalar Redis localmente
# Windows (WSL): sudo apt-get install redis-server
# Mac: brew install redis
# Linux: sudo apt-get install redis-server

# 2. Iniciar Redis
redis-server

# 3. En otra terminal, iniciar API
pip install -r requirements.txt
python main.py

# 4. Verificar conexión
redis-cli ping
```

---

## 📊 Monitoreo y Debugging

### **1. Comandos Redis Útiles**

```bash
# Conectar a Redis
docker exec -it ecommerce_redis_prod redis-cli

# Dentro de redis-cli:
PING                             # Verificar conexión
KEYS *                           # Ver todas las claves (NO en producción!)
KEYS products:*                  # Ver claves de productos
GET products:id:123              # Ver contenido de una clave
TTL products:id:123              # Ver tiempo restante (segundos)
DEL products:id:123              # Eliminar clave manualmente
FLUSHDB                          # Limpiar toda la cache (cuidado!)
INFO stats                       # Estadísticas de uso
INFO memory                      # Uso de memoria
DBSIZE                           # Número de claves
```

### **2. Monitorear Cache Hit Rate**

```bash
# Dentro de redis-cli
INFO stats | grep keyspace

# Buscar:
# keyspace_hits: 1000     # Cache HIT
# keyspace_misses: 200    # Cache MISS
# Hit rate = hits / (hits + misses) = 1000 / 1200 = 83.3% ✅
```

**Objetivo:** Hit rate > 70% para endpoints de lectura

### **3. Ver Logs de Cache**

```bash
# Ver logs de la aplicación
docker-compose -f docker-compose.production.yaml logs -f api | grep -i cache

# Verás:
# Cache HIT: products:list:skip:0:limit:10
# Cache MISS: products:id:999
# Invalidated 5 product list cache entries
```

### **4. Debugging de Rate Limiting**

```bash
# Ver claves de rate limiting
docker exec ecommerce_redis_prod redis-cli KEYS "rate_limit:*"

# Ver contador de una IP
docker exec ecommerce_redis_prod redis-cli GET "rate_limit:192.168.1.100"

# Ver TTL
docker exec ecommerce_redis_prod redis-cli TTL "rate_limit:192.168.1.100"
```

---

## 🧪 Testing con Cache

### **Comparar Performance CON vs SIN Cache**

```bash
# 1. Ejecutar load test CON cache
locust -f load_test.py --host=http://localhost:8000 \
  --users 400 --spawn-rate 50 --run-time 5m --headless

# Métricas esperadas CON cache:
# p50: ~20ms (antes: 40ms)
# p95: ~60ms (antes: 165ms)
# p99: ~100ms (antes: 224ms)

# 2. Para probar SIN cache, deshabilitar temporalmente:
# En .env:
REDIS_ENABLED=false

# Reiniciar y volver a correr load test
```

### **Probar Cache Manualmente**

```bash
# 1. Primera petición (cache MISS)
curl -X GET "http://localhost:8000/products?skip=0&limit=10" -w "\nTime: %{time_total}s\n"
# Time: 0.085s

# 2. Segunda petición (cache HIT)
curl -X GET "http://localhost:8000/products?skip=0&limit=10" -w "\nTime: %{time_total}s\n"
# Time: 0.008s ✅ 10x más rápido

# 3. Invalidar cache creando un producto
curl -X POST "http://localhost:8000/products" \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","price":10,"stock":5,"category_id":1}'

# 4. Siguiente petición será cache MISS de nuevo
curl -X GET "http://localhost:8000/products?skip=0&limit=10" -w "\nTime: %{time_total}s\n"
# Time: 0.082s (va a DB de nuevo)
```

### **Probar Rate Limiting**

```bash
# Script para generar 150 requests rápidas
for i in {1..150}; do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/products
done

# Primeras 100: HTTP 200
# Siguientes 50: HTTP 429 (Too Many Requests) ✅
```

---

## 🎯 Patrones de Cache Implementados

### **1. Cache-Aside (Lazy Loading)**

```python
def get_product(id: int):
    # 1. Intentar obtener de cache
    cached = cache.get(f"product:{id}")
    if cached:
        return cached

    # 2. Si no existe, consultar DB
    product = db.query(Product).get(id)

    # 3. Guardar en cache
    cache.set(f"product:{id}", product, ttl=300)

    return product
```

**Ventajas:**
- Solo cachea datos que se solicitan
- Fácil de implementar
- Resiliente (si falla cache, va a DB)

### **2. Write-Through con Invalidación**

```python
def update_product(id: int, data: dict):
    # 1. Actualizar en BD
    product = db.query(Product).get(id)
    product.update(data)
    db.commit()

    # 2. Invalidar cache (no actualizar directamente)
    cache.delete(f"product:{id}")
    cache.delete_pattern("products:list:*")

    return product
```

**Ventajas:**
- Cache siempre consistente
- Simple (no hay cache stale)

---

## ⚡ Optimizaciones Adicionales

### **1. Cache de Queries Complejas**

```python
# Ejemplo: Productos más vendidos
class ProductService:
    def get_top_selling(self, limit: int = 10):
        cache_key = f"products:top_selling:limit:{limit}"

        cached = self.cache.get(cache_key)
        if cached:
            return cached

        # Query compleja y pesada
        top_products = db.query(Product)\
            .join(OrderDetail)\
            .group_by(Product.id)\
            .order_by(func.sum(OrderDetail.quantity).desc())\
            .limit(limit)\
            .all()

        # Cache por 1 hora (cambia raramente)
        self.cache.set(cache_key, top_products, ttl=3600)

        return top_products
```

### **2. Cache de Sesiones (Futuro)**

```python
# Guardar sesión de usuario en Redis
def create_session(user_id: int, token: str):
    session_data = {
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
        "cart": []
    }

    cache.set(
        f"session:{token}",
        session_data,
        ttl=86400  # 24 horas
    )
```

### **3. Cache de Agregaciones**

```python
# Cache de estadísticas
def get_dashboard_stats():
    cache_key = "stats:dashboard"

    cached = cache.get(cache_key)
    if cached:
        return cached

    stats = {
        "total_products": db.query(Product).count(),
        "total_orders": db.query(Order).count(),
        "total_revenue": db.query(func.sum(Order.total)).scalar()
    }

    # Cache por 10 minutos
    cache.set(cache_key, stats, ttl=600)

    return stats
```

---

## 🚨 Troubleshooting

### **Problema 1: "Redis connection failed"**

```bash
# Verificar que Redis esté corriendo
docker ps | grep redis

# Verificar logs de Redis
docker logs ecommerce_redis_prod

# Verificar conectividad
docker exec ecommerce_redis_prod redis-cli ping

# Solución: Si no responde, reiniciar
docker-compose -f docker-compose.production.yaml restart redis
```

### **Problema 2: "OOM (Out of Memory)"**

```bash
# Ver uso de memoria
docker exec ecommerce_redis_prod redis-cli INFO memory

# Si used_memory > maxmemory:
# 1. Aumentar límite en docker-compose.yaml
maxmemory: 512mb  # Cambiar de 256mb a 512mb

# 2. O forzar limpieza
docker exec ecommerce_redis_prod redis-cli FLUSHDB
```

### **Problema 3: Cache no se invalida**

```bash
# Verificar que las claves existen
docker exec ecommerce_redis_prod redis-cli KEYS "products:*"

# Ver TTL de una clave
docker exec ecommerce_redis_prod redis-cli TTL "products:id:123"

# Limpiar manualmente
docker exec ecommerce_redis_prod redis-cli DEL "products:id:123"

# Verificar logs de invalidación
docker-compose logs -f api | grep "Invalidated"
```

### **Problema 4: Rate limiting muy estricto**

```bash
# Ajustar en .env
RATE_LIMIT_CALLS=200           # Aumentar de 100 a 200
RATE_LIMIT_PERIOD=60

# O deshabilitar temporalmente
RATE_LIMIT_ENABLED=false

# Reiniciar API
docker-compose restart api
```

---

## 📈 Métricas de Éxito

### **Antes de Redis:**
| Métrica | Valor |
|---------|-------|
| Response time p50 | ~40ms |
| Response time p95 | ~165ms |
| Response time p99 | ~224ms |
| PostgreSQL carga | 600 conexiones activas |
| Throughput | ~150-300 RPS |

### **Después de Redis (Esperado):**
| Métrica | Valor | Mejora |
|---------|-------|--------|
| Response time p50 | ~15ms | 62% ✅ |
| Response time p95 | ~50ms | 70% ✅ |
| Response time p99 | ~100ms | 55% ✅ |
| PostgreSQL carga | ~150 conexiones | 75% ✅ |
| Throughput | ~400-600 RPS | 2x ✅ |
| Cache hit rate | > 70% | N/A |

### **Cómo Medir:**

```bash
# 1. Load test SIN cache
REDIS_ENABLED=false
locust -f load_test.py --headless --users 400 --spawn-rate 50 --run-time 5m

# 2. Load test CON cache
REDIS_ENABLED=true
locust -f load_test.py --headless --users 400 --spawn-rate 50 --run-time 5m

# 3. Comparar resultados
```

---

## ✅ Checklist de Producción

- [x] Redis configurado en docker-compose.production.yaml
- [x] Variables de entorno configuradas en .env
- [x] Rate limiting habilitado
- [x] Cache implementado en ProductService
- [x] Cache implementado en CategoryService
- [x] Middleware de rate limiting activo
- [x] Logging de cache configurado
- [ ] Monitoreo de cache hit rate (recomendado: Prometheus/Grafana)
- [ ] Alertas de OOM en Redis (opcional)
- [ ] Backup/persistencia de Redis configurado (AOF ya habilitado)
- [ ] SSL/TLS para conexión Redis (si Redis está en servidor externo)

---

## 🔮 Próximos Pasos (Opcional)

### **Para Escalar Más Allá:**

1. **Redis Cluster** (múltiples nodos)
2. **Redis Sentinel** (alta disponibilidad)
3. **Cache de sesiones** (JWT tokens en Redis)
4. **Pub/Sub** (notificaciones en tiempo real)
5. **Leaderboards** (con Redis Sorted Sets)
6. **Full-text search** con RediSearch
7. **Rate limiting por usuario** (no solo IP)

---

## 📚 Referencias

- [Redis Official Docs](https://redis.io/docs/)
- [FastAPI Caching Best Practices](https://fastapi.tiangolo.com/advanced/middleware/)
- [Redis Caching Patterns](https://redis.io/docs/manual/patterns/)
- [Rate Limiting Algorithms](https://redis.io/glossary/rate-limiting/)

---

**¡Redis implementado con éxito!** 🎉

**Impacto esperado:**
- ✅ Reducción de latencia: 60-70%
- ✅ Reducción de carga en PostgreSQL: 70-80%
- ✅ Protección contra abuso con rate limiting
- ✅ Listo para escalar a 600+ peticiones concurrentes