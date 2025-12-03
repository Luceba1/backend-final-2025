# API REST E-commerce

<div align="center">

![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11.6-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Habilitado-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Licencia](https://img.shields.io/badge/Licencia-MIT-green?style=for-the-badge)

**API REST de grado empresarial para sistemas de comercio electrónico**

*Alto rendimiento • Escalable • Lista para producción*

[Características](#-características) •
[Inicio Rápido](#-inicio-rápido) •
[Documentación](#-documentación) •
[Arquitectura](#-arquitectura-del-sistema) •
[Rendimiento](#-rendimiento-y-optimización) •
[Despliegue](#-despliegue)

</div>

---

## 📋 Tabla de Contenidos

- [Resumen Ejecutivo](#-resumen-ejecutivo)
- [Características](#-características)
- [Stack Tecnológico](#-stack-tecnológico)
- [Arquitectura del Sistema](#-arquitectura-del-sistema)
- [Inicio Rápido](#-inicio-rápido)
- [Documentación](#-documentación)
- [Configuración](#-configuración)
- [Documentación de la API](#-documentación-de-la-api)
- [Esquema de Base de Datos](#-esquema-de-base-de-datos)
- [Rendimiento y Optimización](#-rendimiento-y-optimización)
- [Seguridad](#-seguridad)
- [Monitoreo y Observabilidad](#-monitoreo-y-observabilidad)
- [Desarrollo](#-desarrollo)
- [Pruebas](#-pruebas)
- [Despliegue](#-despliegue)
- [Contribuciones](#-contribuciones)
- [Licencia](#-licencia)

---

## 🎯 Resumen Ejecutivo

Una **API REST de FastAPI lista para producción** diseñada para sistemas de comercio electrónico de alto rendimiento. Construida con prácticas modernas de Python, esta API maneja **más de 400 solicitudes concurrentes** con tiempos de respuesta inferiores a 200ms.

### Propuesta de Valor

Esta solución de software proporciona una infraestructura robusta y escalable para gestionar todas las operaciones de un negocio de comercio electrónico, desde la administración de productos hasta el procesamiento completo de pedidos. El sistema está diseñado para crecer con su negocio, soportando desde pequeñas operaciones hasta plataformas de alto tráfico con miles de transacciones simultáneas.

### Puntos Destacados

- 🚀 **Alto Rendimiento**: Maneja 400+ solicitudes concurrentes, 150-300 RPS sostenidos
- 💾 **Caché Inteligente**: Sistema de caché basado en Redis con mejora de rendimiento del 60-70%
- 🔒 **Seguridad Empresarial**: Limitación de tasa, validación de entrada, prevención de inyección SQL
- 📊 **Monitoreo de Producción**: Chequeos de salud completos, métricas y logging
- 🐳 **Listo para Contenedores**: Docker Compose con configuración de producción optimizada
- 📈 **Escalable Horizontalmente**: Arquitectura multi-worker con pool de conexiones
- 📚 **Bien Documentado**: Swagger/OpenAPI, guías detalladas y ejemplos

### Casos de Uso

#### **1. Plataformas de Comercio Electrónico**
Sistema completo para gestionar un negocio en línea con catálogo de productos, gestión de inventario, procesamiento de pedidos y seguimiento de clientes.

#### **2. Sistemas de Retail**
Gestión de productos multi-categoría, administración de clientes, facturación y control de stock en tiempo real.

#### **3. Gestión de Inventarios**
Seguimiento de stock automático, alertas de bajo inventario, y actualización en tiempo real de disponibilidad de productos.

#### **4. Procesamiento de Pedidos**
Gestión completa del ciclo de vida de pedidos, seguimiento de entregas, procesamiento de pagos y gestión de devoluciones.

---

## ✨ Características

### Funcionalidad Principal

#### **Gestión de Productos**
- ✅ Operaciones CRUD completas con paginación
- ✅ Organización basada en categorías
- ✅ Gestión de stock con actualizaciones automáticas
- ✅ Validación de precios y restricciones
- ✅ Caché Redis (TTL de 5 minutos)
- ✅ Reseñas y calificaciones de productos

**Valor de Negocio**: Permite mantener un catálogo de productos actualizado en tiempo real, con información precisa de inventario que previene sobreventa y mejora la experiencia del cliente.

#### **Gestión de Clientes**
- ✅ Perfiles de cliente con validación única de email
- ✅ Gestión de múltiples direcciones
- ✅ Seguimiento de historial de pedidos
- ✅ Manejo de eliminación en cascada

**Valor de Negocio**: Centraliza toda la información del cliente para proporcionar un servicio personalizado y mantener un registro completo de interacciones y transacciones.

#### **Procesamiento de Pedidos**
- ✅ Soporte para pedidos multi-artículo
- ✅ Validación de claves foráneas (cliente, factura)
- ✅ Selección de método de entrega (Drive-thru, En mano, Entrega a domicilio)
- ✅ Seguimiento de estado de pedido (Pendiente, En Progreso, Entregado, Cancelado)
- ✅ Detalles de pedido con cantidad y precios

**Valor de Negocio**: Automatiza y simplifica el proceso de pedidos, reduciendo errores humanos y proporcionando transparencia total en el estado de cada transacción.

#### **Sistema de Facturación**
- ✅ Generación única de números de factura
- ✅ Gestión de descuentos
- ✅ Soporte de tipos de pago (Efectivo, Tarjeta)
- ✅ Cálculo de totales con validación

**Valor de Negocio**: Asegura cumplimiento fiscal con numeración única de facturas y mantiene registros precisos para contabilidad y auditorías.

### Características Avanzadas

#### **Optimización de Rendimiento**
- 🚀 **Pool de Conexiones**: 50 conexiones base + 100 overflow por worker
- 🚀 **Arquitectura Multi-Worker**: 4-8 workers Uvicorn para paralelismo
- 🚀 **Caché Redis**: Patrón cache-aside con invalidación automática
- 🚀 **Indexación de Base de Datos**: Índices optimizados en claves foráneas y columnas de búsqueda
- 🚀 **Carga Perezosa**: Relaciones SQLAlchemy optimizadas para prevenir consultas N+1

**Impacto Técnico**: El sistema puede manejar picos de tráfico de hasta 500 usuarios simultáneos con tiempos de respuesta consistentes, lo que garantiza una experiencia fluida incluso en períodos de alta demanda como ventas especiales o días festivos.

#### **Seguridad y Protección**
- 🔒 **Limitación de Tasa**: 100 solicitudes/60 segundos por IP (basado en Redis)
- 🔒 **Validación de Entrada**: Esquemas Pydantic con reglas comprehensivas
- 🔒 **Prevención de Inyección SQL**: Consultas parametrizadas vía ORM SQLAlchemy
- 🔒 **Configuración CORS**: Intercambio de recursos de origen cruzado configurable
- 🔒 **Manejo de Errores**: Degradación elegante y respuestas informativas

**Impacto en Seguridad**: Protege contra las vulnerabilidades más comunes (OWASP Top 10) y proporciona múltiples capas de defensa contra ataques maliciosos, asegurando la integridad de los datos del negocio y de los clientes.

#### **Observabilidad**
- 📊 **Chequeos de Salud**: Métricas de base de datos, Redis y pool de conexiones
- 📊 **Logging Centralizado**: Logs rotativos de archivo con múltiples niveles
- 📊 **Métricas de Rendimiento**: Tiempos de respuesta, tasas de acierto de caché, utilización de pool
- 📊 **OpenTelemetry**: Listo para integración de trazabilidad distribuida

**Valor Operativo**: Proporciona visibilidad total del sistema en tiempo real, permitiendo detectar y resolver problemas antes de que afecten a los usuarios finales.

#### **Experiencia del Desarrollador**
- 📚 **Documentación Auto-Generada**: Swagger UI y ReDoc
- 📚 **Seguridad de Tipos**: Type hints completos con Pydantic v2
- 📚 **Pruebas de Carga**: Scripts Locust incorporados
- 📚 **Soporte Docker**: Configuraciones de desarrollo y producción

---

## 🛠 Stack Tecnológico

### Framework Principal

| Tecnología | Versión | Propósito | Justificación |
|------------|---------|-----------|---------------|
| **FastAPI** | 0.104.1 | Framework web moderno con OpenAPI automático | Elegido por su alto rendimiento (comparable a Node.js y Go), validación automática de datos y documentación interactiva generada automáticamente |
| **Uvicorn** | 0.24.0 | Servidor ASGI para despliegue en producción | Servidor ASGI de alto rendimiento con soporte para workers múltiples |
| **Pydantic** | 2.5.1 | Validación de datos y gestión de configuración | Proporciona validación de tipos en tiempo de ejecución con excelente rendimiento |
| **Python** | 3.11.6 | Entorno de ejecución | Versión estable con mejoras significativas de rendimiento |

### Base de Datos y Caché

| Tecnología | Versión | Propósito | Justificación |
|------------|---------|-----------|---------------|
| **PostgreSQL** | 13-alpine | Base de datos relacional con cumplimiento ACID | Base de datos robusta, confiable y de código abierto con excelente soporte para integridad referencial |
| **SQLAlchemy** | 2.0.23 | ORM con soporte async | ORM maduro y potente que proporciona abstracción de base de datos con seguridad contra inyección SQL |
| **Redis** | 7-alpine | Caché en memoria y limitación de tasa | Almacén de datos en memoria ultra-rápido para mejorar rendimiento y gestionar límites de tasa |
| **psycopg2-binary** | 2.9.9 | Driver de PostgreSQL | Driver oficial y robusto para PostgreSQL |

### DevOps y Monitoreo

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Docker** | Latest | Contenerización |
| **Docker Compose** | 3.8 | Orquestación multi-contenedor |
| **Locust** | 2.18.0 | Pruebas de carga |
| **OpenTelemetry** | 1.12.0 | Observabilidad y trazabilidad |

### Herramientas de Desarrollo

| Herramienta | Versión | Propósito |
|-------------|---------|-----------|
| **pytest** | 7.4.3 | Framework de pruebas |
| **black** | 23.12.0 | Formateador de código |
| **flake8** | 6.1.0 | Linter |
| **mypy** | 1.7.1 | Verificador de tipos estáticos |

---

## 🏗 Arquitectura del Sistema

### Patrón de Arquitectura en Capas

El sistema implementa una **arquitectura en capas** estricta que separa las responsabilidades y facilita el mantenimiento, pruebas y escalabilidad del código.

```
┌─────────────────────────────────────────────────────────────┐
│                   Cliente (Petición HTTP)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Capa de Middleware                        │
│  • Rate Limiter (100 req/60s por IP)                        │
│  • CORS (Orígenes configurables)                            │
│  • Request ID (Trazabilidad distribuida)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Capa de Controladores                       │
│  • Enrutamiento HTTP (FastAPI)                              │
│  • Validación de Peticiones (Pydantic)                      │
│  • Inyección de Dependencias (get_db)                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Capa de Servicios                          │
│  • Lógica de Negocio                                        │
│  • Validación de Claves Foráneas                            │
│  • Gestión de Caché (Redis)                                 │
│  • Gestión de Stock                                         │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 Capa de Repositorios                         │
│  • Operaciones CRUD                                         │
│  • Gestión de Transacciones                                 │
│  • Consultas SQLAlchemy                                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                Base de Datos PostgreSQL                      │
│  • Pool de Conexiones (50 base + 100 overflow por worker)  │
│  • Índices Optimizados                                      │
│  • Transacciones ACID                                       │
└─────────────────────────────────────────────────────────────┘
```

### Explicación de Capas

#### **1. Capa de Middleware**
**Responsabilidad**: Procesar todas las peticiones HTTP antes de que lleguen a la lógica de negocio.

**Componentes**:
- **Rate Limiter**: Protege contra abuso limitando peticiones por IP
- **CORS**: Configura políticas de acceso entre dominios
- **Request ID**: Genera identificador único para trazabilidad de peticiones

**Beneficio**: Separa preocupaciones transversales del código de negocio, facilitando cambios en políticas de seguridad sin afectar la lógica principal.

#### **2. Capa de Controladores**
**Responsabilidad**: Manejar peticiones HTTP, validar entrada y devolver respuestas.

**Patrón Implementado**:
```python
# Ejemplo simplificado
@router.get("/products")
async def get_products(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)  # Inyección de dependencias
):
    service = ProductService(db)
    return service.get_all(skip, limit)
```

**Beneficio**: Los controladores son delgados y solo se encargan de HTTP, delegando toda la lógica a la capa de servicios.

#### **3. Capa de Servicios**
**Responsabilidad**: Implementar toda la lógica de negocio y reglas del dominio.

**Ejemplos de Lógica de Negocio**:
- Validar que un cliente existe antes de crear un pedido
- Verificar stock suficiente antes de confirmar venta
- Validar que el precio en el pedido coincide con el precio actual del producto
- Actualizar inventario automáticamente al crear/cancelar pedidos

**Beneficio**: Centraliza la lógica de negocio en un solo lugar, facilitando pruebas unitarias y mantenimiento.

#### **4. Capa de Repositorios**
**Responsabilidad**: Manejar toda la interacción con la base de datos.

**Operaciones Proporcionadas**:
```python
class BaseRepository:
    def find(id_key)          # Buscar por ID
    def find_all(skip, limit) # Listar con paginación
    def save(model)           # Crear
    def update(id_key, changes) # Actualizar
    def remove(id_key)        # Eliminar
    def save_all(models)      # Inserción masiva
```

**Beneficio**: Abstrae la persistencia de datos, permitiendo cambiar de base de datos con mínimos cambios en el código.

#### **5. Capa de Modelos y Esquemas**
**Responsabilidad**: Definir estructura de datos y reglas de validación.

- **Modelos (SQLAlchemy)**: Representan tablas de base de datos
- **Esquemas (Pydantic)**: Validan datos de entrada/salida de API

**Beneficio**: Separación clara entre representación de base de datos y contratos de API.

### Patrones de Diseño Implementados

#### **1. Inyección de Dependencias**
```python
# FastAPI maneja automáticamente el ciclo de vida
@router.get("/products")
async def get_products(db: Session = Depends(get_db)):
    # db es inyectada automáticamente
    # y cerrada al finalizar la petición
    service = ProductService(db)
    return service.get_all()
```

**Beneficio**: Facilita pruebas (podemos inyectar mocks) y gestión automática de recursos (cierre de sesiones de BD).

#### **2. Patrón Repositorio**
Abstrae la lógica de acceso a datos detrás de una interfaz uniforme.

**Beneficio**: Cambiar de PostgreSQL a MySQL requeriría solo cambiar la implementación del repositorio, no el código de servicios.

#### **3. Patrón Factory**
```python
# Creación de servicios mediante factories
service_factory = lambda db: ProductService(db)
```

**Beneficio**: Facilita la creación de objetos complejos y permite configuración centralizada.

#### **4. Patrón Singleton**
```python
# Redis configuration - una sola instancia compartida
redis_config = RedisConfig()
```

**Beneficio**: Asegura que solo existe una conexión Redis compartida entre todos los componentes.

#### **5. Patrón Cache-Aside**
```python
# 1. Buscar en caché
cached = cache.get(key)
if cached:
    return cached

# 2. Si no está en caché, buscar en BD
result = database.query()

# 3. Guardar en caché para siguientes peticiones
cache.set(key, result, ttl=300)
return result
```

**Beneficio**: Mejora dramáticamente el rendimiento (70% menos consultas a BD) manteniendo datos actualizados.

### Estructura del Proyecto

```
apipython-main/
├── config/                      # Configuración de aplicación
│   ├── database.py             # Pool de conexiones PostgreSQL
│   ├── redis_config.py         # Singleton de Redis
│   └── logging_config.py       # Logging centralizado
│
├── controllers/                 # Manejadores de peticiones HTTP
│   ├── base_controller_impl.py # Controlador CRUD genérico
│   ├── client_controller.py    # Endpoints de clientes
│   ├── product_controller.py   # Endpoints de productos (con caché)
│   ├── order_controller.py     # Endpoints de pedidos
│   ├── category_controller.py  # Endpoints de categorías (con caché)
│   ├── bill_controller.py      # Endpoints de facturación
│   ├── address_controller.py   # Endpoints de direcciones
│   ├── review_controller.py    # Endpoints de reseñas
│   ├── order_detail_controller.py # Endpoints de detalle de pedido
│   └── health_check.py         # Salud y métricas
│
├── services/                    # Capa de lógica de negocio
│   ├── base_service_impl.py    # Operaciones de servicio genéricas
│   ├── cache_service.py        # Abstracción de caché Redis
│   ├── product_service.py      # Lógica de productos (con caché)
│   ├── category_service.py     # Lógica de categorías (con caché)
│   ├── order_service.py        # Lógica de pedidos (validación FK)
│   ├── order_detail_service.py # Lógica de detalle de pedido
│   └── [otros servicios...]
│
├── repositories/                # Capa de acceso a datos
│   ├── base_repository_impl.py # CRUD genérico con SQLAlchemy 2.0
│   ├── product_repository.py   # Acceso a datos de productos
│   ├── order_repository.py     # Acceso a datos de pedidos
│   └── [otros repositorios...]
│
├── models/                      # Modelos ORM de SQLAlchemy
│   ├── base_model.py           # Base con id_key y timestamps
│   ├── client.py               # Entidad Cliente
│   ├── product.py              # Entidad Producto
│   ├── order.py                # Entidad Pedido
│   ├── order_detail.py         # Entidad DetallePedido
│   ├── bill.py                 # Entidad Factura
│   ├── category.py             # Entidad Categoría
│   ├── address.py              # Entidad Dirección
│   ├── review.py               # Entidad Reseña
│   └── enums.py                # Enumeraciones compartidas
│
├── schemas/                     # Esquemas de validación Pydantic
│   ├── base_schema.py          # Base con campos comunes
│   ├── client_schema.py        # Validación de Cliente
│   ├── product_schema.py       # Validación de Producto
│   ├── order_schema.py         # Validación de Pedido
│   └── [otros esquemas...]
│
├── middleware/                  # Middleware personalizado
│   ├── rate_limiter.py         # Limitación de tasa basada en Redis
│   └── request_id_middleware.py # Generación de Request ID
│
├── utils/                       # Utilidades
│   └── logging_utils.py        # Logging sanitizado
│
├── logs/                        # Logs de aplicación
│   ├── app.log                 # Logs generales (rotativo)
│   └── error.log               # Solo errores
│
├── tests/                       # Suite de pruebas
│   ├── conftest.py             # Fixtures de pytest
│   ├── test_models.py          # Pruebas de modelos
│   ├── test_repositories.py    # Pruebas de repositorios
│   ├── test_services.py        # Pruebas de servicios
│   ├── test_controllers.py     # Pruebas de controladores
│   └── test_integration.py     # Pruebas de integración
│
├── main.py                      # Punto de entrada de aplicación
├── run_production.py            # Servidor de producción (multi-worker)
├── load_test.py                 # Pruebas de carga con Locust
│
├── docker-compose.yaml          # Entorno de desarrollo
├── docker-compose.production.yaml  # Entorno de producción
├── Dockerfile                   # Build Docker básico
├── Dockerfile.production        # Build multi-etapa optimizado
│
├── requirements.txt             # Dependencias de producción
├── requirements-dev.txt         # Dependencias de desarrollo
│
└── docs/                        # Documentación
    ├── CLAUDE.md               # Guía de arquitectura para Claude Code
    ├── HISTORIAS_USUARIO.md   # Historias de usuario en español
    ├── API_DOCUMENTATION.es.md # Documentación completa de APIs
    ├── ARCHITECTURE.puml       # Diagrama PlantUML de arquitectura
    ├── HIGH_PERFORMANCE_GUIDE.md
    ├── REDIS_IMPLEMENTATION_GUIDE.md
    └── [otras guías...]
```

### Flujo de Datos - Ejemplo de Creación de Pedido

```
1. Cliente HTTP
   POST /orders
   {
     "client_id": 1,
     "bill_id": 1,
     "delivery_method": 3,
     "status": 1
   }
   ↓

2. Middleware
   - Rate Limiter: Verifica límite de peticiones
   - Request ID: Genera UUID único
   - CORS: Valida origen de petición
   ↓

3. OrderController
   - Valida schema con Pydantic
   - Inyecta sesión de BD
   - Llama a OrderService
   ↓

4. OrderService (Lógica de Negocio)
   - Valida que client_id existe (consulta ClientRepository)
   - Valida que bill_id existe (consulta BillRepository)
   - Verifica integridad de datos
   - Llama a OrderRepository.save()
   ↓

5. OrderRepository
   - Convierte schema a modelo SQLAlchemy
   - Ejecuta INSERT en transacción
   - Commit a base de datos
   - Maneja rollback en caso de error
   ↓

6. PostgreSQL
   - Valida restricciones de integridad referencial
   - Verifica foreign keys
   - Persiste datos
   - Retorna ID generado
   ↓

7. Respuesta
   OrderController devuelve:
   HTTP 201 Created
   {
     "id_key": 123,
     "client_id": 1,
     "bill_id": 1,
     "delivery_method": 3,
     "status": 1,
     "date": "2025-11-17T10:00:00"
   }
```

---

## 🚀 Inicio Rápido

### Requisitos Previos

| Componente | Versión Mínima | Recomendado | Notas |
|------------|----------------|-------------|-------|
| **Python** | 3.11+ | 3.11.6 | Versión con mejoras de rendimiento |
| **Docker** | 20.10+ | Latest | Opcional pero recomendado |
| **Docker Compose** | 2.0+ | Latest | Opcional pero recomendado |
| **PostgreSQL** | 13+ | 13-alpine | Si ejecuta localmente sin Docker |
| **Redis** | 7+ | 7-alpine | Si ejecuta localmente sin Docker |

### Opción 1: Docker Compose (Recomendado)

Esta es la forma más rápida de comenzar. Docker Compose levantará automáticamente todos los servicios necesarios.

#### Entorno de Desarrollo

```bash
# 1. Clonar el repositorio
git clone <repository-url>
cd apipython-main

# 2. Iniciar todos los servicios (API + PostgreSQL + Redis)
docker-compose up --build

# 3. Verificar que los servicios están corriendo
# API disponible en: http://localhost:8000
# Documentación Swagger en: http://localhost:8000/docs
# PostgreSQL en: localhost:5432
# Redis en: localhost:6379
```

**¿Qué está ocurriendo detrás de escenas?**
1. Docker Compose lee `docker-compose.yaml`
2. Construye la imagen Docker de la aplicación
3. Inicia PostgreSQL en un contenedor
4. Inicia Redis en un contenedor
5. Inicia la API FastAPI conectada a PostgreSQL y Redis
6. Crea automáticamente las tablas de base de datos
7. La API queda lista para recibir peticiones

#### Entorno de Producción

```bash
# Usar configuración de producción optimizada
docker-compose -f docker-compose.production.yaml up -d

# Verificar logs
docker-compose -f docker-compose.production.yaml logs -f api

# Escalar la API horizontalmente (3 instancias)
docker-compose -f docker-compose.production.yaml up -d --scale api=3

# Detener servicios
docker-compose -f docker-compose.production.yaml down
```

**Diferencias con desarrollo:**
- Múltiples workers Uvicorn (4-8) para mayor rendimiento
- PostgreSQL optimizado con parámetros de producción
- Redis con política de evicción LRU
- Health checks configurados
- Recursos limitados para prevenir uso excesivo
- Logs centralizados

### Opción 2: Desarrollo Local

Para desarrolladores que prefieren ejecutar la aplicación directamente en su máquina.

#### 1. Configurar Entorno Python

```bash
# Crear entorno virtual
python3.11 -m venv venv

# Activar entorno virtual
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Verificar activación
which python  # Debe mostrar ruta a venv/bin/python
```

**¿Por qué un entorno virtual?**
- Aísla dependencias del proyecto
- Previene conflictos con otras versiones de librerías
- Facilita reproducibilidad del entorno

#### 2. Instalar Dependencias

```bash
# Dependencias de producción
pip install -r requirements.txt

# Dependencias de desarrollo (opcional pero recomendado)
pip install -r requirements-dev.txt

# Verificar instalación
pip list
```

**Dependencias principales instaladas:**
- FastAPI y Uvicorn (framework web)
- SQLAlchemy y psycopg2 (base de datos)
- Pydantic (validación)
- Redis-py (caché)
- Pytest (pruebas, si instaló requirements-dev.txt)

#### 3. Configurar Variables de Entorno

```bash
# Copiar archivo de ejemplo
cp .env.example .env

# Editar configuración
nano .env  # o usar su editor preferido
```

**Configuración mínima para desarrollo:**
```bash
# Base de Datos
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_dev
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_ENABLED=true

# Aplicación
LOG_LEVEL=DEBUG
UVICORN_WORKERS=1

# Seguridad (desarrollo)
CORS_ORIGINS=*
RATE_LIMIT_ENABLED=false
```

#### 4. Configurar Base de Datos

**Opción A: Usar Docker solo para PostgreSQL y Redis**
```bash
# Iniciar solo servicios de base de datos
docker-compose up -d postgres redis

# Verificar que están corriendo
docker-compose ps
```

**Opción B: Instalar PostgreSQL y Redis localmente**

**PostgreSQL** (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql

# Crear base de datos
sudo -u postgres psql
postgres=# CREATE DATABASE ecommerce_dev;
postgres=# \q
```

**Redis** (Ubuntu/Debian):
```bash
sudo apt install redis-server
sudo systemctl start redis-server

# Verificar
redis-cli ping
# Debe responder: PONG
```

**Importante**: Las tablas de base de datos se crean automáticamente al iniciar la aplicación por primera vez.

#### 5. Ejecutar la Aplicación

```bash
# Modo desarrollo (hot reload - recarga automática)
python main.py

# Modo producción (multi-worker)
python run_production.py
```

**Diferencia entre modos:**
- **Desarrollo**: Un solo worker, recarga automática al cambiar código
- **Producción**: Múltiples workers, sin recarga automática, optimizado para rendimiento

#### 6. Verificar Instalación

```bash
# Chequeo de salud
curl http://localhost:8000/health_check

# Respuesta esperada:
# {
#   "status": "healthy",
#   "timestamp": "2025-11-17T10:00:00",
#   "checks": {
#     "database": {"status": "up", "latency_ms": 15.2},
#     "redis": {"status": "up"},
#     "db_pool": {"utilization_percent": 3.3, ...}
#   }
# }

# Abrir documentación interactiva
# En navegador: http://localhost:8000/docs
```

### Primeras Peticiones a la API

Una vez que la API está corriendo, puede probar las funcionalidades básicas:

```bash
# 1. Crear una categoría
curl -X POST "http://localhost:8000/categories" \
  -H "Content-Type: application/json" \
  -d '{"name": "Electrónica"}'

# Respuesta:
# {"id_key": 1, "name": "Electrónica"}

# 2. Crear un producto
curl -X POST "http://localhost:8000/products" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Laptop Dell XPS 13",
    "price": 1299.99,
    "stock": 15,
    "category_id": 1
  }'

# Respuesta:
# {
#   "id_key": 1,
#   "name": "Laptop Dell XPS 13",
#   "price": 1299.99,
#   "stock": 15,
#   "category_id": 1
# }

# 3. Listar productos (con caché automático)
curl "http://localhost:8000/products?skip=0&limit=10"

# Respuesta: Array de productos

# 4. Crear un cliente
curl -X POST "http://localhost:8000/clients" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Juan",
    "lastname": "Pérez",
    "email": "juan.perez@example.com",
    "telephone": "+525512345678"
  }'

# Respuesta:
# {
#   "id_key": 1,
#   "name": "Juan",
#   "lastname": "Pérez",
#   "email": "juan.perez@example.com",
#   "telephone": "+525512345678"
# }

# 5. Obtener cliente específico
curl "http://localhost:8000/clients/1"
```

### Explorar Documentación Interactiva

La API proporciona dos interfaces de documentación auto-generadas:

**1. Swagger UI** (Recomendado para pruebas)
```
URL: http://localhost:8000/docs
```
Características:
- Pruebe cada endpoint directamente desde el navegador
- Vea ejemplos de request/response
- Valide esquemas automáticamente
- Útil para desarrollo y QA

**2. ReDoc** (Recomendado para lectura)
```
URL: http://localhost:8000/redoc
```
Características:
- Documentación profesional y limpia
- Formato imprimible
- Ejemplos de código en múltiples lenguajes
- Ideal para compartir con equipo

---

## ⚙️ Configuración

### Variables de Entorno

El sistema se configura completamente mediante variables de entorno, permitiendo desplegar en múltiples ambientes sin cambiar código.

#### Configuración de Base de Datos

```bash
# Conexión
POSTGRES_HOST=postgres              # Hostname del servidor PostgreSQL
POSTGRES_PORT=5432                  # Puerto (default: 5432)
POSTGRES_DB=ecommerce_prod         # Nombre de la base de datos
POSTGRES_USER=postgres             # Usuario de base de datos
POSTGRES_PASSWORD=secure_password  # Contraseña (¡usar contraseña segura!)

# Pool de Conexiones (optimizado para 400+ peticiones concurrentes)
DB_POOL_SIZE=50                    # Conexiones base por worker
DB_MAX_OVERFLOW=100                # Conexiones adicionales en picos
DB_POOL_TIMEOUT=10                 # Timeout de conexión (segundos)
DB_POOL_RECYCLE=3600              # Reciclar conexiones después de 1 hora

# Capacidad Total = UVICORN_WORKERS × (POOL_SIZE + MAX_OVERFLOW)
# Ejemplo: 4 workers × (50 + 100) = 600 conexiones concurrentes
```

**Explicación del Pool de Conexiones:**

El pool de conexiones es crucial para el rendimiento. Imagínelo como un estacionamiento:
- `DB_POOL_SIZE` (50): Espacios permanentes siempre disponibles
- `DB_MAX_OVERFLOW` (100): Espacios temporales para picos de tráfico
- `DB_POOL_TIMEOUT` (10s): Tiempo máximo de espera por un espacio
- `DB_POOL_RECYCLE` (3600s): Tiempo antes de "renovar" una conexión

**¿Por qué es importante?**
- Sin pool: Cada petición abre/cierra conexión (muy lento, ~100ms overhead)
- Con pool: Conexiones reutilizadas (~1ms overhead)
- Mejora de rendimiento: **100x más rápido**

#### Configuración de Redis

```bash
# Conexión
REDIS_HOST=redis                   # Hostname de Redis
REDIS_PORT=6379                    # Puerto (default: 6379)
REDIS_DB=0                         # Número de base de datos Redis
REDIS_PASSWORD=                    # Contraseña (opcional, dejar vacío si no hay)

# Configuración de Caché
REDIS_ENABLED=true                 # Habilitar/deshabilitar caché
REDIS_CACHE_TTL=300               # TTL por defecto (5 minutos)
REDIS_MAX_CONNECTIONS=50          # Tamaño de pool de conexiones

# Comportamiento de Caché
# - Productos: TTL 5 minutos
# - Categorías: TTL 1 hora (cambian raramente)
# - Auto-invalidación en POST/PUT/DELETE
```

**Estrategia de Caché:**

```
Productos (cambian frecuentemente):
  TTL: 5 minutos
  Razón: Balance entre rendimiento y actualidad de datos

Categorías (cambian raramente):
  TTL: 1 hora
  Razón: Mayor rendimiento, datos estables

Invalidación Automática:
  - Al crear producto → Invalida lista de productos
  - Al actualizar producto → Invalida ese producto + listas
  - Al eliminar producto → Invalida ese producto + listas
```

**Impacto en Rendimiento:**
- Sin caché: 100% peticiones a BD, ~150ms respuesta
- Con caché (70% aciertos): 30% peticiones a BD, ~50ms respuesta promedio
- **Mejora: 3x más rápido**

#### Configuración de Aplicación

```bash
# Servidor
API_HOST=0.0.0.0                  # Bind a todas las interfaces
API_PORT=8000                      # Puerto de la API
UVICORN_WORKERS=4                  # Número de workers (4-8 recomendado)

# Ajuste de Rendimiento
BACKLOG=2048                       # Tamaño de cola de conexiones
TIMEOUT_KEEP_ALIVE=5              # Timeout de keep-alive
LIMIT_CONCURRENCY=1000            # Máximo de conexiones concurrentes
LIMIT_MAX_REQUESTS=10000          # Peticiones antes de reiniciar worker

# Desarrollo
RELOAD=false                       # Hot reload (solo desarrollo)
```

**¿Cuántos workers usar?**
```python
# Regla general
workers = (CPU_cores × 2) + 1

# Ejemplos:
2 cores → 5 workers
4 cores → 9 workers (usar 8 para números redondos)
8 cores → 17 workers (usar 16)

# ¿Por qué 2x + 1?
# - 2x permite paralelismo durante I/O (espera de BD)
# - +1 maneja picos mientras otros están ocupados
```

#### Configuración de Seguridad

```bash
# Limitación de Tasa
RATE_LIMIT_ENABLED=true           # Habilitar limitación de tasa
RATE_LIMIT_CALLS=100              # Máximo de peticiones
RATE_LIMIT_PERIOD=60              # Período (segundos)

# Ejemplo: 100 peticiones cada 60 segundos por IP
# ¿Por qué limitar?
# - Prevenir abuso/DDoS
# - Proteger recursos del servidor
# - Garantizar servicio equitativo

# CORS (Cross-Origin Resource Sharing)
CORS_ORIGINS=*                     # Orígenes permitidos (separados por coma)

# Ejemplos de configuración:
# Producción (restrictivo):
CORS_ORIGINS=https://mitienda.com,https://app.mitienda.com

# Desarrollo (permisivo):
CORS_ORIGINS=*  # Permite cualquier origen
```

#### Configuración de Logging

```bash
# Niveles de Logging
LOG_LEVEL=INFO                     # DEBUG, INFO, WARNING, ERROR

# Niveles explicados:
# - DEBUG: Todo (muy verboso, solo desarrollo)
# - INFO: Eventos importantes (producción)
# - WARNING: Situaciones anormales pero manejables
# - ERROR: Errores que afectan funcionalidad

ACCESS_LOG=true                    # Loguear peticiones HTTP

# Archivos de logs (auto-configurados):
# - logs/app.log (rotativo 10MB × 5 backups)
# - logs/error.log (solo errores)
```

#### Optimizaciones de Python

```bash
# Variables de entorno de Python
PYTHONUNBUFFERED=1                # Deshabilitar buffering de salida
PYTHONDONTWRITEBYTECODE=1         # No crear archivos .pyc

# Beneficios:
# PYTHONUNBUFFERED=1
#   - Logs aparecen inmediatamente (importante para Docker)
#   - Facilita depuración en tiempo real
#
# PYTHONDONTWRITEBYTECODE=1
#   - Reduce tamaño de imagen Docker
#   - Evita problemas con archivos .pyc obsoletos
```

### Configuraciones de Docker Compose

El proyecto incluye dos archivos de Docker Compose para diferentes ambientes.

#### Desarrollo (`docker-compose.yaml`)

**Características:**
- Instancia única de API (fácil depuración)
- PostgreSQL 13
- Redis 7
- Hot reload habilitado (cambios de código se reflejan inmediatamente)
- Exposición de puertos para debugging
- Montaje de volúmenes para actualizaciones de código en vivo

**Uso:**
```bash
docker-compose up --build
```

**Servicios incluidos:**
```yaml
services:
  api:
    - Puerto: 8000
    - Workers: 1
    - Reload: Habilitado

  postgres:
    - Puerto: 5432
    - Usuario: postgres
    - Contraseña: postgres

  redis:
    - Puerto: 6379
    - Sin autenticación
```

#### Producción (`docker-compose.production.yaml`)

**Características:**
- API multi-worker (4 workers)
- PostgreSQL 13-alpine optimizado
  - max_connections: 700
  - shared_buffers: 256MB
  - effective_cache_size: 768MB
- Redis 7-alpine
  - maxmemory: 256MB
  - eviction policy: allkeys-lru
- Health checks para todos los servicios
- Límites de recursos
- Políticas de auto-reinicio

**Uso:**
```bash
# Iniciar stack de producción
docker-compose -f docker-compose.production.yaml up -d

# Ver logs
docker-compose -f docker-compose.production.yaml logs -f api

# Escalar API horizontalmente
docker-compose -f docker-compose.production.yaml up -d --scale api=3

# Detener stack
docker-compose -f docker-compose.production.yaml down
```

**Optimizaciones de PostgreSQL aplicadas:**

```ini
# Gestión de Conexiones
max_connections = 700                    # Total de conexiones permitidas

# Configuración de Memoria
shared_buffers = 256MB                   # 25% de RAM (caché de PostgreSQL)
effective_cache_size = 768MB             # 75% de RAM (para planner)
work_mem = 16MB                          # Memoria por operación
maintenance_work_mem = 128MB            # Operaciones de mantenimiento

# Write-Ahead Logging (WAL)
wal_buffers = 16MB
min_wal_size = 1GB
max_wal_size = 4GB
checkpoint_completion_target = 0.9       # Checkpoints suaves

# Query Planner
default_statistics_target = 100
random_page_cost = 1.1                   # Optimizado para SSD
effective_io_concurrency = 200           # Concurrencia de SSD

# Paralelismo
max_worker_processes = 4
max_parallel_workers = 4
max_parallel_workers_per_gather = 2
```

**¿Qué significa cada parámetro?**

- **max_connections**: Máximo de clientes conectados simultáneamente
- **shared_buffers**: RAM dedicada a caché de datos de PostgreSQL
- **effective_cache_size**: Estimado de memoria disponible para cache (guía al planner)
- **work_mem**: Memoria para ordenamientos y hash tables por operación
- **random_page_cost**: Costo estimado de lectura aleatoria (1.1 = SSD rápido)

### Configuración de Redis

Configuración de producción:

```conf
maxmemory 256mb                          # Máximo uso de memoria
maxmemory-policy allkeys-lru             # Política de evicción
appendonly yes                           # Persistencia habilitada
tcp-backlog 511                          # Cola de conexiones
```

**Políticas de evicción explicadas:**

- **allkeys-lru**: Elimina claves menos recientemente usadas cuando se alcanza maxmemory
- **Alternativas**:
  - `volatile-lru`: Solo elimina claves con TTL
  - `allkeys-random`: Elimina claves aleatorias
  - `noeviction`: Error cuando se llena (no recomendado)

**Persistencia:**
- `appendonly yes`: Guarda cada operación en disco
- Trade-off: Mayor durabilidad vs menor rendimiento
- Recomendado para caché de producción

---

## 📚 Documentación de la API

### Documentación Interactiva

La API genera automáticamente documentación completa e interactiva en dos formatos.

#### Swagger UI (Recomendado para Pruebas)

**URL:** `http://localhost:8000/docs`

**Características:**
- **Interactividad**: Ejecute peticiones directamente desde el navegador
- **Ejemplos en vivo**: Request/response examples para cada endpoint
- **Validación automática**: Schema validation al escribir JSON
- **Autenticación**: Pruebe endpoints protegidos con tokens
- **Try-it-out**: Botón para probar cada operación

**Ejemplo de uso:**
1. Abrir http://localhost:8000/docs
2. Expandir endpoint POST /products
3. Click en "Try it out"
4. Editar JSON de ejemplo
5. Click en "Execute"
6. Ver response en tiempo real

#### ReDoc

**URL:** `http://localhost:8000/redoc`

**Características:**
- **Documentación profesional**: Diseño limpio y organizado
- **Formato imprimible**: Ideal para documentación PDF
- **Ejemplos de código**: Múltiples lenguajes (curl, Python, JavaScript)
- **Descripciones detalladas**: Schemas con toda su información
- **Búsqueda rápida**: Encontrar endpoints fácilmente

**Ideal para:**
- Compartir con equipo técnico
- Onboarding de desarrolladores
- Documentación formal del proyecto

### Endpoints de la API

Consulte el archivo dedicado [API_DOCUMENTATION.es.md](docs/API_DOCUMENTATION.es.md) para documentación completa de todos los endpoints.

Resumen rápido de módulos disponibles:

| Módulo | Base URL | Descripción | Endpoints |
|--------|----------|-------------|-----------|
| **Clientes** | `/clients` | Gestión de clientes | 5 endpoints |
| **Productos** | `/products` | Catálogo de productos (con caché) | 5 endpoints |
| **Categorías** | `/categories` | Categorías de productos (con caché) | 5 endpoints |
| **Pedidos** | `/orders` | Gestión de pedidos | 5 endpoints |
| **Detalles de Pedido** | `/order_details` | Líneas de pedido con gestión de stock | 5 endpoints |
| **Facturas** | `/bills` | Sistema de facturación | 5 endpoints |
| **Direcciones** | `/addresses` | Direcciones de clientes | 5 endpoints |
| **Reseñas** | `/reviews` | Reseñas de productos | 5 endpoints |
| **Salud** | `/health_check` | Estado del sistema | 1 endpoint |

**Total: 41 endpoints**

Para documentación detallada de cada endpoint con ejemplos, schemas y códigos de respuesta, consulte [API_DOCUMENTATION.es.md](docs/API_DOCUMENTATION.es.md).

---

## 📚 Documentación

El proyecto cuenta con documentación completa y detallada en español:

### Guías de Usuario

| Documento | Descripción | Enlace |
|-----------|-------------|--------|
| **Guía de Inicio Rápido** | Levantar el proyecto en menos de 5 minutos | [GUIA_INICIO_RAPIDO.es.md](docs/GUIA_INICIO_RAPIDO.es.md) |
| **Arquitectura del Sistema** | Patrones de diseño, capas, y flujo de datos | [ARQUITECTURA.es.md](docs/ARQUITECTURA.es.md) |
| **Guía de Rendimiento** | Optimización y pruebas de carga para 400+ usuarios | [RENDIMIENTO.es.md](docs/RENDIMIENTO.es.md) |
| **Guía de Despliegue** | Despliegue en producción con Docker y Nginx | [DESPLIEGUE.es.md](docs/DESPLIEGUE.es.md) |
| **Guía de Pruebas** | Suite de 189 pruebas automatizadas | [PRUEBAS.es.md](docs/PRUEBAS.es.md) |

### Documentación Técnica

| Documento | Descripción | Enlace |
|-----------|-------------|--------|
| **Historias de Usuario** | 16 historias con criterios de aceptación | [HISTORIAS_USUARIO.md](docs/HISTORIAS_USUARIO.md) |
| **Documentación de API** | Guía completa de todos los endpoints | [API_DOCUMENTATION.es.md](docs/API_DOCUMENTATION.es.md) |
| **Guía de Arquitectura (Claude Code)** | Documentación para IA y desarrollo | [CLAUDE.md](CLAUDE.md) |
| **Diagrama de Arquitectura** | Diagrama visual del sistema | [ARCHITECTURE.puml](docs/ARCHITECTURE.puml) |

### Documentación Interactiva

Cuando la API está ejecutándose, accede a:

- **Swagger UI**: http://localhost:8000/docs (Probar endpoints interactivamente)
- **ReDoc**: http://localhost:8000/redoc (Documentación profesional)
- **OpenAPI JSON**: http://localhost:8000/openapi.json (Especificación completa)

### Guías de Alto Rendimiento

| Documento | Descripción |
|-----------|-------------|
| **HIGH_PERFORMANCE_GUIDE.md** | Configuración para 400+ requests concurrentes |
| **REDIS_IMPLEMENTATION_GUIDE.md** | Sistema de caché y rate limiting |
| **LOAD_TEST_SUMMARY.md** | Resultados de pruebas de carga |
| **DEPLOYMENT_SUMMARY.md** | Solución de problemas críticos de producción |
| **PRODUCTION_READY.md** | Checklist de preparación para producción |

---

## 📞 Soporte y Contacto

### Obtener Ayuda

- **Documentación Completa**: Ver directorio `/docs`
- **API Docs**: http://localhost:8000/docs (cuando está corriendo)
- **Guía de Arquitectura**: [CLAUDE.md](CLAUDE.md)
- **Historias de Usuario**: [HISTORIAS_USUARIO.md](docs/HISTORIAS_USUARIO.md)
- **Documentación de API**: [API_DOCUMENTATION.es.md](docs/API_DOCUMENTATION.es.md)

### Obtener Ayuda

- **Issues**: Abrir un issue en GitHub
- **Discussions**: Usar GitHub Discussions
- **Email**: soporte@example.com

---

<div align="center">

**Construido con ❤️ usando FastAPI**

[Reportar Bug](https://github.com/your-repo/issues) •
[Solicitar Característica](https://github.com/your-repo/issues) •
[Documentación](https://github.com/your-repo/docs)

</div>