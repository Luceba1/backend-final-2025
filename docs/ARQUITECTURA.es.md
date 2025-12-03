# 🏗️ Arquitectura del Sistema - API E-commerce

Este documento describe la arquitectura completa del sistema de e-commerce, incluyendo patrones de diseño, flujo de datos y decisiones arquitectónicas.

---

## 📋 Tabla de Contenidos

- [Visión General](#-visión-general)
- [Arquitectura de Capas](#-arquitectura-de-capas)
- [Patrones de Diseño](#-patrones-de-diseño)
- [Modelo de Datos](#-modelo-de-datos)
- [Flujo de Datos](#-flujo-de-datos)
- [Componentes del Sistema](#-componentes-del-sistema)
- [Infraestructura](#-infraestructura)
- [Decisiones Arquitectónicas](#-decisiones-arquitectónicas)

---

## 🎯 Visión General

### Principios Arquitectónicos

1. **Separación de Responsabilidades**: Cada capa tiene una responsabilidad única y bien definida
2. **Inyección de Dependencias**: Las dependencias se inyectan en tiempo de ejecución
3. **DRY (Don't Repeat Yourself)**: Clases base reutilizables para funcionalidad común
4. **Fail Fast**: Validaciones tempranas para detectar errores rápidamente
5. **Stateless**: La API no mantiene estado entre peticiones
6. **Cache First**: Estrategia de caché para mejorar rendimiento

### Stack Tecnológico

```
┌─────────────────────────────────────────────────┐
│           FastAPI 0.104.1 (ASGI)                │
├─────────────────────────────────────────────────┤
│         Python 3.11.6 + Pydantic 2.5.1          │
├─────────────────────────────────────────────────┤
│       SQLAlchemy 2.0.23 (ORM) + Alembic         │
├─────────────────────────────────────────────────┤
│  PostgreSQL 13        │      Redis 7 (Cache)    │
├───────────────────────┴─────────────────────────┤
│              Docker + Docker Compose            │
└─────────────────────────────────────────────────┘
```

---

## 🏛️ Arquitectura de Capas

El sistema sigue una **arquitectura de 4 capas** estrictamente separadas:

```
┌──────────────────────────────────────────────┐
│  CAPA 1: Controllers (HTTP Layer)           │  ← Entrada de peticiones
│  • Routing FastAPI                          │
│  • Validación HTTP                          │
│  • Serialización JSON                       │
└──────────────────┬───────────────────────────┘
                   │ HTTP Request/Response
┌──────────────────▼───────────────────────────┐
│  CAPA 2: Services (Business Logic)          │  ← Lógica de negocio
│  • Validaciones de negocio                  │
│  • Orquestación de operaciones              │
│  • Gestión de transacciones                 │
└──────────────────┬───────────────────────────┘
                   │ DTOs (Schemas)
┌──────────────────▼───────────────────────────┐
│  CAPA 3: Repositories (Data Access)         │  ← Acceso a datos
│  • CRUD Operations                          │
│  • Queries SQL                              │
│  • Gestión de sesiones                      │
└──────────────────┬───────────────────────────┘
                   │ SQLAlchemy Models
┌──────────────────▼───────────────────────────┐
│  CAPA 4: Models (Domain Layer)              │  ← Modelo de dominio
│  • Entidades de base de datos               │
│  • Relaciones ORM                           │
│  • Constraints                              │
└──────────────────────────────────────────────┘
```

### Flujo de Datos

```
Cliente HTTP
    │
    ▼
┌─────────────────┐
│  Middleware     │ ← Rate Limiter, CORS, Request ID
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Controller     │ ← Validación de entrada (Pydantic)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Service       │ ← Lógica de negocio + Caché (Redis)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Repository     │ ← Operaciones de base de datos
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │ ← Persistencia
└─────────────────┘
```

---

## 🎨 Patrones de Diseño

### 1. Repository Pattern

**Objetivo**: Abstraer el acceso a datos de la lógica de negocio.

**Implementación**:

```python
# Base Repository
class BaseRepositoryImpl:
    def __init__(self, model, schema, db):
        self.model = model
        self.schema = schema
        self.db = db

    def find(self, id_key):
        """Get single record"""

    def find_all(self, skip, limit):
        """Get all records with pagination"""

    def save(self, model):
        """Create new record"""

    def update(self, id_key, changes):
        """Update existing record"""

    def remove(self, id_key):
        """Delete record"""
```

**Ventajas**:
- ✅ Separación de responsabilidades
- ✅ Facilita testing (mocking)
- ✅ Reutilización de código
- ✅ Cambio de fuente de datos sin afectar lógica de negocio

### 2. Service Layer Pattern

**Objetivo**: Encapsular la lógica de negocio compleja.

**Implementación**:

```python
class BaseServiceImpl:
    def __init__(self, repository_class, model, schema, db):
        self.repository = repository_class(model, schema, db)
        self.schema = schema

    def get_all(self, skip, limit):
        """Business logic + repository call"""

    def get_one(self, id_key):
        """Get with business validation"""

    def save(self, schema):
        """Validate + save"""

    def update(self, id_key, schema):
        """Validate + update"""

    def delete(self, id_key):
        """Validate + delete"""
```

**Ventajas**:
- ✅ Lógica de negocio centralizada
- ✅ Validaciones consistentes
- ✅ Transacciones gestionadas correctamente
- ✅ Testing independiente de la API

### 3. Dependency Injection Pattern

**Objetivo**: Desacoplar la creación de objetos de su uso.

**Implementación**:

```python
# Controller usa factory pattern
class ProductController(BaseControllerImpl):
    def __init__(self):
        super().__init__(
            schema=ProductSchema,
            service_factory=lambda db: ProductService(db),  # ← Inyección
            tags=["Products"]
        )

# FastAPI gestiona la sesión de DB automáticamente
@app.get("/products")
def get_products(db: Session = Depends(get_db)):  # ← Inyección automática
    service = ProductService(db)
    return service.get_all()
```

**Ventajas**:
- ✅ Bajo acoplamiento
- ✅ Facilita testing (mock dependencies)
- ✅ Gestión automática de recursos (sesiones DB)
- ✅ Configuración centralizada

### 4. Factory Pattern

**Objetivo**: Crear objetos sin exponer la lógica de creación.

**Implementación**:

```python
# Service Factory
service_factory = lambda db: ProductService(db)

# Controller usa la factory
controller = ProductController(
    schema=ProductSchema,
    service_factory=service_factory
)
```

### 5. Template Method Pattern

**Objetivo**: Definir el esqueleto de un algoritmo en la superclase.

**Implementación**:

```python
# Base Controller define el template
class BaseControllerImpl:
    def __init__(self, schema, service_factory, tags):
        self.router = APIRouter(tags=tags)

        # Template method - siempre genera estos endpoints
        @self.router.get("/")
        async def get_all(...):
            # Llama a service.get_all()

        @self.router.post("/", status_code=201)
        async def create(...):
            # Llama a service.save()
```

**Ventajas**:
- ✅ Código DRY (no repetir endpoints CRUD)
- ✅ Consistencia en toda la API
- ✅ Fácil de extender (agregar endpoints custom)

---

## 🗄️ Modelo de Datos

### Diagrama Entidad-Relación

```
┌──────────────┐
│   Category   │
│──────────────│
│ id_key (PK)  │
│ name         │
└──────┬───────┘
       │
       │ 1:N
       │
┌──────▼───────┐        ┌─────────────┐
│   Product    │────────│   Review    │
│──────────────│   1:N  │─────────────│
│ id_key (PK)  │        │ id_key (PK) │
│ name         │        │ rating      │
│ price        │        │ comment     │
│ stock        │        │ product_id  │
│ category_id  │        └─────────────┘
└──────┬───────┘
       │
       │ 1:N
       │
┌──────▼────────┐       ┌──────────────┐
│ OrderDetail   │───────│    Order     │
│───────────────│  N:1  │──────────────│
│ id_key (PK)   │       │ id_key (PK)  │
│ quantity      │       │ date         │
│ price         │       │ total        │
│ order_id (FK) │       │ delivery_meth│
│ product_id    │       │ status       │
└───────────────┘       │ client_id    │
                        │ bill_id      │
                        └──────┬───────┘
                               │
                     ┌─────────┴─────────┐
                     │                   │
              ┌──────▼───────┐    ┌─────▼──────┐
              │   Client     │    │    Bill    │
              │──────────────│    │────────────│
              │ id_key (PK)  │    │ id_key (PK)│
              │ name         │    │ bill_number│
              │ lastname     │    │ discount   │
              │ email (UQ)   │    │ total      │
              │ telephone    │    │ payment    │
              └──────┬───────┘    └────────────┘
                     │
                     │ 1:N
                     │
              ┌──────▼───────┐
              │   Address    │
              │──────────────│
              │ id_key (PK)  │
              │ street       │
              │ number       │
              │ city         │
              │ client_id    │
              └──────────────┘
```

### Relaciones Clave

1. **Product ↔ Category**: Muchos a Uno
   - Un producto pertenece a una categoría
   - Una categoría tiene muchos productos

2. **Product ↔ Review**: Uno a Muchos (cascade delete)
   - Un producto puede tener muchas reseñas
   - Al eliminar un producto, se eliminan sus reseñas

3. **Order ↔ OrderDetail**: Uno a Muchos (cascade delete)
   - Una orden tiene múltiples detalles
   - Al eliminar una orden, se eliminan sus detalles

4. **Client ↔ Address**: Uno a Muchos (cascade delete)
   - Un cliente tiene múltiples direcciones
   - Al eliminar un cliente, se eliminan sus direcciones

5. **Order ↔ Client**: Muchos a Uno
   - Una orden pertenece a un cliente
   - Un cliente tiene muchas órdenes

---

## 🔄 Flujo de Datos Completo

### Ejemplo: Crear un Pedido con Productos

```
1. Cliente HTTP POST /order_details
   │
   ▼
2. Middleware (Rate Limiter, Request ID)
   │
   ▼
3. OrderDetailController.create()
   ├─ Valida JSON con Pydantic
   ├─ Inyecta Session DB
   │
   ▼
4. OrderDetailService.save()
   ├─ Verifica que Order existe (FK validation)
   ├─ Verifica que Product existe (FK validation)
   ├─ Valida stock disponible
   │  └─ if product.stock < quantity: raise HTTP 400
   ├─ Valida precio coincide
   │  └─ if schema.price != product.price: raise HTTP 400
   │
   ▼
5. OrderDetailRepository.save()
   ├─ BEGIN TRANSACTION
   ├─ INSERT INTO order_details (...)
   ├─ UPDATE products SET stock = stock - quantity
   ├─ COMMIT
   │
   ▼
6. Retorna OrderDetailSchema (JSON)
   │
   ▼
7. Invalida caché de productos en Redis
   │
   ▼
8. HTTP 201 Created + JSON Response
```

### Manejo de Errores

```
En cualquier punto del flujo:

Error → Repository
   │
   ├─ db.rollback()           # Revertir transacción
   ├─ Log error con Request ID
   ├─ Raise InstanceNotFoundError
   │
   ▼
FastAPI Exception Handler
   │
   ├─ Sanitiza mensaje (no exponer internals)
   ├─ Retorna HTTP 404/400/500
   ▼
Cliente recibe error estructurado
```

---

## 🧩 Componentes del Sistema

### 1. Middleware Stack (LIFO Order)

```python
# 3. CORS (outermost)
fastapi_app.add_middleware(CORSMiddleware, ...)

# 2. Rate Limiter
fastapi_app.add_middleware(RateLimiterMiddleware)

# 1. Request ID (innermost - se ejecuta primero)
fastapi_app.add_middleware(RequestIDMiddleware)
```

**Flujo de Request**:
```
Request → CORS → Rate Limiter → Request ID → Controller
Response ← CORS ← Rate Limiter ← Request ID ← Controller
```

### 2. Sistema de Caché (Redis)

**Estrategia**:
- **Cache First**: Revisar caché antes de DB
- **Write Through**: Actualizar caché al escribir
- **Cache Invalidation**: Eliminar caché en mutations

**TTLs por Entidad**:
```python
Products: 5 minutos    # Cambian frecuentemente
Categories: 1 hora     # Casi estáticas
Clients: No cached     # Datos sensibles
Orders: No cached      # Datos transaccionales
```

**Claves de Caché**:
```
products:list:skip:0:limit:10
products:id:123
categories:list:skip:0:limit:100
categories:id:5
```

### 3. Pool de Conexiones DB

**Configuración de Producción**:
```python
DB_POOL_SIZE = 50         # Conexiones por worker
DB_MAX_OVERFLOW = 100     # Conexiones adicionales
UVICORN_WORKERS = 4       # Workers del servidor

Total Capacity = (50 + 100) × 4 = 600 conexiones
```

**Monitoreo**:
```python
# Health Check muestra utilización
{
  "db_pool": {
    "size": 50,
    "checked_in": 45,      # Disponibles
    "checked_out": 5,      # En uso
    "overflow": 0,
    "utilization_percent": 10.0
  }
}
```

---

## 🏢 Infraestructura

### Arquitectura de Despliegue

```
┌─────────────────────────────────────────┐
│         Load Balancer (Nginx)           │
│              Port 80/443                │
└──────────────────┬──────────────────────┘
                   │
      ┌────────────┴────────────┐
      │                         │
┌─────▼─────┐             ┌────▼──────┐
│  Worker 1 │             │  Worker N │
│  Port     │    ...      │  Port     │
│  8000     │             │  800N     │
└─────┬─────┘             └────┬──────┘
      │                        │
      └────────────┬───────────┘
                   │
      ┌────────────┴────────────┐
      │                         │
┌─────▼─────┐             ┌────▼──────┐
│ PostgreSQL│             │   Redis   │
│  Port     │             │   Port    │
│  5432     │             │   6379    │
└───────────┘             └───────────┘
```

### Componentes Docker

```yaml
services:
  api:
    - 4-8 Uvicorn workers
    - Connection pool: 50+100 per worker
    - Health checks cada 30s

  postgres:
    - PostgreSQL 13-alpine
    - Volume persistente
    - Configuración optimizada

  redis:
    - Redis 7-alpine
    - Cache + Rate Limiting
    - Configuración AOF
```

---

## 🎯 Decisiones Arquitectónicas

### 1. ¿Por qué FastAPI en lugar de Flask/Django?

**Decisión**: FastAPI

**Razones**:
- ✅ **Rendimiento**: Basado en Starlette (ASGI) - 3x más rápido que Flask
- ✅ **Validación Automática**: Pydantic integrado
- ✅ **Documentación Auto-generada**: Swagger UI incluido
- ✅ **Type Hints Nativos**: Python 3.6+ type hints
- ✅ **Async/Await**: Soporte nativo para operaciones asíncronas

### 2. ¿Por qué SQLAlchemy ORM en lugar de SQL raw?

**Decisión**: SQLAlchemy 2.0

**Razones**:
- ✅ **Seguridad**: Prevención automática de SQL injection
- ✅ **Portabilidad**: Cambiar de DB sin reescribir queries
- ✅ **Mantenibilidad**: Modelos Python vs SQL strings
- ✅ **Relationships**: Gestión automática de relaciones
- ✅ **Migraciones**: Alembic para versionado de schema

### 3. ¿Por qué Redis para caché?

**Decisión**: Redis 7

**Razones**:
- ✅ **Velocidad**: Operaciones en memoria (< 1ms)
- ✅ **Estructuras de Datos**: Soporte para listas, sets, hashes
- ✅ **TTL Automático**: Expiración de claves
- ✅ **Atomicidad**: Operaciones atómicas (INCR, PIPELINE)
- ✅ **Persistencia**: AOF/RDB para durabilidad

### 4. ¿Por qué PostgreSQL en lugar de MySQL?

**Decisión**: PostgreSQL 13

**Razones**:
- ✅ **ACID Compliant**: Transacciones robustas
- ✅ **JSON Support**: Columnas JSONB nativas
- ✅ **Extensibilidad**: Extensions (pg_trgm, etc.)
- ✅ **Concurrent Performance**: MVCC para alta concurrencia
- ✅ **Open Source**: Sin vendor lock-in

### 5. ¿Por qué Arquitectura de Capas?

**Decisión**: 4-layer architecture

**Razones**:
- ✅ **Separation of Concerns**: Cada capa una responsabilidad
- ✅ **Testability**: Testing independiente por capa
- ✅ **Maintainability**: Cambios localizados
- ✅ **Scalability**: Fácil extraer servicios a microservicios
- ✅ **Team Productivity**: Equipos pueden trabajar en paralelo

### 6. ¿Por qué lazy='select' en relaciones?

**Decisión**: `lazy='select'` para todas las relaciones ORM

**Razones**:
- ✅ **Evita N+1**: Con `lazy='joined'` se generan cartesian products
- ✅ **Performance**: Solo carga datos cuando se necesitan
- ✅ **Control**: El desarrollador decide cuándo cargar relaciones
- ❌ **Evita `lazy='joined'`**: Causó problemas críticos de rendimiento

### 7. ¿Por qué Service Factory Pattern?

**Decisión**: Lambda factories para servicios

**Razones**:
- ✅ **Fresh Sessions**: Cada request tiene su propia sesión DB
- ✅ **No Leaks**: Sesiones se cierran automáticamente
- ✅ **Thread Safe**: No compartir sesiones entre threads
- ✅ **Dependency Injection**: FastAPI gestiona el ciclo de vida

---

## 📊 Métricas de Arquitectura

### Cobertura de Código

```
Models:       ~95%
Repositories: ~90%
Services:     ~85%
Controllers:  ~80%
Overall:      >80%
```

### Métricas de Rendimiento

```
Response Time p95:    < 200ms
Throughput:           150-300 RPS
Concurrent Users:     400+
Error Rate:           < 1%
Cache Hit Rate:       > 70%
DB Pool Utilization:  < 70%
```

### Complejidad del Código

```
Cyclomatic Complexity: < 10 (por función)
Lines per Function:    < 50
Classes per Module:    < 5
```

---

## 🔮 Evolución Futura

### Posibles Mejoras

1. **Microservicios**
   - Extraer módulos a servicios independientes
   - API Gateway con Kong/Traefik
   - Service Mesh (Istio)

2. **Event Sourcing**
   - Kafka para eventos de dominio
   - CQRS pattern
   - Event store

3. **Caché Distribuido**
   - Redis Cluster
   - Cache sharding
   - Geo-replication

4. **Base de Datos**
   - Read replicas para queries
   - Partitioning por categoría
   - CITUS para scaling horizontal

---

**Documento actualizado**: 2025-11-18
**Versión**: 2.0
**Mantenedor**: Equipo de Arquitectura