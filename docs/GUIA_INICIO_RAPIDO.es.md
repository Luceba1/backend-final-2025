# 🚀 Guía de Inicio Rápido - API E-commerce

Esta guía te ayudará a levantar el proyecto en **menos de 5 minutos**.

---

## 📋 Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:

- **Docker Desktop** (versión 20.10+)
- **Docker Compose** (versión 2.0+)
- **Git** (para clonar el repositorio)

O alternativamente:

- **Python 3.11+**
- **PostgreSQL 13+**
- **Redis 7+**

---

## ⚡ Opción 1: Inicio Rápido con Docker (Recomendado)

### Paso 1: Clonar el Repositorio

```bash
git clone <url-del-repositorio>
cd apipython-main
```

### Paso 2: Levantar los Servicios

```bash
# Modo desarrollo
docker-compose up --build

# Modo producción (optimizado para 400+ usuarios concurrentes)
docker-compose -f docker-compose.production.yaml up -d
```

### Paso 3: Verificar que Todo Funcione

```bash
# Verificar el estado de salud de la API
curl http://localhost:8000/health_check

# Respuesta esperada:
# {
#   "status": "healthy",
#   "checks": {
#     "database": {"status": "up"},
#     "redis": {"status": "up"}
#   }
# }
```

### Paso 4: Acceder a la Documentación Interactiva

Abre tu navegador en:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

¡Listo! 🎉 Ya tienes la API funcionando.

---

## 🐍 Opción 2: Instalación Local con Python

### Paso 1: Crear Entorno Virtual

```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# En Windows:
venv\Scripts\activate
# En Linux/Mac:
source venv/bin/activate
```

### Paso 2: Instalar Dependencias

```bash
pip install -r requirements.txt
```

### Paso 3: Configurar Variables de Entorno

Crea un archivo `.env` basado en `.env.example`:

```bash
# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

Edita el archivo `.env` con tu configuración:

```env
# Base de Datos
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password_aqui

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_ENABLED=true

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_CALLS=100
RATE_LIMIT_PERIOD=60
```

### Paso 4: Iniciar PostgreSQL y Redis

```bash
# Con Docker (recomendado)
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:13
docker run -d --name redis -p 6379:6379 redis:7-alpine

# O instalarlos localmente según tu sistema operativo
```

### Paso 5: Ejecutar Migraciones de Base de Datos

```bash
# Opción 1: Auto-crear tablas (desarrollo)
python main.py
# Las tablas se crean automáticamente

# Opción 2: Usar Alembic (producción)
alembic upgrade head
```

### Paso 6: Iniciar el Servidor

```bash
# Modo desarrollo (1 worker)
python main.py

# Modo producción (múltiples workers)
python run_production.py
```

### Paso 7: Verificar la Instalación

```bash
# Verificar salud del sistema
curl http://localhost:8000/health_check

# Ver documentación
# Abre http://localhost:8000/docs en tu navegador
```

---

## 🎯 Primeros Pasos con la API

### 1. Crear una Categoría

```bash
curl -X POST http://localhost:8000/categories \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Electrónicos"
  }'
```

**Respuesta**:
```json
{
  "id_key": 1,
  "name": "Electrónicos"
}
```

### 2. Crear un Producto

```bash
curl -X POST http://localhost:8000/products \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Laptop Dell XPS 13",
    "price": 1299.99,
    "stock": 50,
    "category_id": 1
  }'
```

**Respuesta**:
```json
{
  "id_key": 1,
  "name": "Laptop Dell XPS 13",
  "price": 1299.99,
  "stock": 50,
  "category_id": 1
}
```

### 3. Listar Todos los Productos

```bash
curl http://localhost:8000/products
```

### 4. Crear un Cliente

```bash
curl -X POST http://localhost:8000/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Juan",
    "lastname": "Pérez",
    "email": "juan.perez@example.com",
    "telephone": "+525512345678"
  }'
```

### 5. Crear un Pedido Completo

#### Paso 1: Crear la Factura
```bash
curl -X POST http://localhost:8000/bills \
  -H "Content-Type: application/json" \
  -d '{
    "bill_number": "BILL-2025-001",
    "discount": 0,
    "total": 1299.99,
    "payment_type": "card"
  }'
```

#### Paso 2: Crear el Pedido
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "total": 1299.99,
    "delivery_method": 3,
    "status": 1,
    "client_id": 1,
    "bill_id": 1
  }'
```

#### Paso 3: Agregar Detalles del Pedido
```bash
curl -X POST http://localhost:8000/order_details \
  -H "Content-Type: application/json" \
  -d '{
    "quantity": 1,
    "price": 1299.99,
    "order_id": 1,
    "product_id": 1
  }'
```

---

## 📊 Verificar el Estado del Sistema

### Health Check Completo

```bash
curl http://localhost:8000/health_check | python -m json.tool
```

**Respuesta Ejemplo**:
```json
{
  "status": "healthy",
  "timestamp": "2025-11-18T02:00:00.000Z",
  "checks": {
    "database": {
      "status": "up",
      "latency_ms": 12.5
    },
    "redis": {
      "status": "up"
    },
    "db_pool": {
      "size": 50,
      "checked_in": 45,
      "checked_out": 5,
      "utilization_percent": 10.0
    }
  }
}
```

### Verificar Servicios Docker

```bash
# Ver estado de contenedores
docker-compose ps

# Ver logs de la API
docker-compose logs api

# Ver logs en tiempo real
docker-compose logs -f api
```

---

## 🧪 Ejecutar las Pruebas

### Todas las Pruebas (189 tests)

```bash
pytest tests/ -v
```

### Con Reporte de Cobertura

```bash
pytest tests/ --cov=. --cov-report=html
# Abre htmlcov/index.html en tu navegador
```

### Pruebas Específicas

```bash
# Pruebas de modelos
pytest tests/test_models.py -v

# Pruebas de servicios
pytest tests/test_services.py -v

# Pruebas de endpoints
pytest tests/test_controllers.py -v

# Pruebas de integración
pytest tests/test_integration.py -v
```

### Pruebas de Carga (Locust)

```bash
# Instalar dependencias de desarrollo
pip install -r requirements-dev.txt

# Ejecutar prueba de carga (interfaz web)
locust -f load_test.py --host=http://localhost:8000
# Abre http://localhost:8089

# Ejecutar prueba de carga (modo headless)
locust -f load_test.py \
  --host=http://localhost:8000 \
  --users 400 \
  --spawn-rate 50 \
  --run-time 5m \
  --headless
```

---

## 🔧 Comandos Útiles

### Docker

```bash
# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes (reinicio completo)
docker-compose down -v

# Reconstruir imágenes
docker-compose build --no-cache

# Ver logs de un servicio específico
docker-compose logs postgres
docker-compose logs redis
```

### Base de Datos

```bash
# Conectar a PostgreSQL (dentro del contenedor)
docker exec -it ecommerce_postgres_dev psql -U postgres -d postgres

# Listar todas las tablas
\dt

# Ver estructura de una tabla
\d products

# Salir
\q
```

### Redis

```bash
# Conectar a Redis
docker exec -it ecommerce_redis_dev redis-cli

# Ver todas las claves
KEYS *

# Ver valor de una clave
GET products:id:1

# Limpiar caché
FLUSHALL

# Salir
exit
```

### Alembic (Migraciones)

```bash
# Ver estado actual
alembic current

# Ver historial de migraciones
alembic history

# Crear nueva migración
alembic revision --autogenerate -m "Descripción del cambio"

# Aplicar migraciones
alembic upgrade head

# Revertir última migración
alembic downgrade -1
```

---

## 🐛 Solución de Problemas Comunes

### Error: "relation does not exist"
**Problema**: Las tablas de la base de datos no se han creado.

**Solución**:
```bash
# Con Docker
docker-compose down -v
docker-compose up --build

# Local
python -c "from config.database import create_tables; create_tables()"
```

### Error: "Connection refused" (Redis)
**Problema**: Redis no está ejecutándose.

**Solución**:
```bash
# Iniciar Redis con Docker
docker run -d --name redis -p 6379:6379 redis:7-alpine

# O deshabilitar Redis temporalmente
export REDIS_ENABLED=false
python main.py
```

### Error: "Port 8000 already in use"
**Problema**: Otro proceso está usando el puerto 8000.

**Solución**:
```bash
# Windows
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# Linux/Mac
lsof -i :8000
kill -9 <PID>

# O usar otro puerto
export API_PORT=8001
uvicorn main:app --port 8001
```

### Error: "QueuePool limit exceeded"
**Problema**: Pool de conexiones de base de datos agotado.

**Solución**:
```bash
# Aumentar tamaño del pool
export DB_POOL_SIZE=100
export DB_MAX_OVERFLOW=200

# O reducir workers
export UVICORN_WORKERS=2
```

---

## 📚 Próximos Pasos

1. **Explorar la API**: Abre http://localhost:8000/docs y prueba todos los endpoints
2. **Leer las Historias de Usuario**: Ver `docs/HISTORIAS_USUARIO.md` para entender los requisitos
3. **Revisar la Arquitectura**: Ver `ARQUITECTURA.es.md` para entender el diseño del sistema
4. **Optimizar para Producción**: Ver `RENDIMIENTO.es.md` para guías de optimización
5. **Desplegar en Producción**: Ver `DESPLIEGUE.es.md` para instrucciones de deployment

---

## 🆘 ¿Necesitas Ayuda?

- **Documentación Completa**: Ver `README.es.md`
- **Guía de Tests**: Ver `tests/README_TESTS.md`
- **Guía de Alto Rendimiento**: Ver `HIGH_PERFORMANCE_GUIDE.md`
- **Issues**: Reporta problemas en el repositorio de GitHub

---

**¡Felicitaciones! 🎉** Ya tienes el sistema funcionando. Empieza a construir tu plataforma de e-commerce.