# Historias de Usuario - Sistema E-commerce API

## Índice
- [Actores del Sistema](#actores-del-sistema)
- [Módulo de Clientes](#módulo-de-clientes)
- [Módulo de Productos y Catálogo](#módulo-de-productos-y-catálogo)
- [Módulo de Pedidos](#módulo-de-pedidos)
- [Módulo de Facturación](#módulo-de-facturación)
- [Módulo de Reseñas](#módulo-de-reseñas)
- [Administración del Sistema](#administración-del-sistema)

---

## Actores del Sistema

### 1. Cliente Final
**Descripción**: Usuario que compra productos a través de la plataforma de e-commerce.

**Objetivos**:
- Buscar y explorar productos
- Realizar compras de manera segura y rápida
- Gestionar sus datos personales y direcciones
- Seguir el estado de sus pedidos
- Dejar reseñas sobre productos comprados

---

### 2. Administrador de Tienda
**Descripción**: Usuario con permisos para gestionar el catálogo de productos, inventario y configuración de la tienda.

**Objetivos**:
- Administrar catálogo de productos
- Controlar inventario y stock
- Gestionar categorías de productos
- Monitorear ventas y pedidos
- Analizar reseñas de productos

---

### 3. Operador de Pedidos
**Descripción**: Usuario encargado de procesar y gestionar pedidos de clientes.

**Objetivos**:
- Procesar pedidos entrantes
- Actualizar estados de pedidos
- Gestionar devoluciones y cancelaciones
- Coordinar entregas
- Resolver problemas de pedidos

---

### 4. Sistema Integrado (API Consumer)
**Descripción**: Aplicación externa que consume la API para integrarse con otros sistemas (frontend web, app móvil, ERP, etc.).

**Objetivos**:
- Consultar datos de productos en tiempo real
- Crear y actualizar pedidos
- Sincronizar inventarios
- Obtener información de clientes
- Generar reportes

---

## Módulo de Clientes

### HU-C01: Registro de Nuevo Cliente
**Como** cliente final
**Quiero** registrarme en el sistema proporcionando mi información personal
**Para** poder realizar compras y gestionar mis pedidos

**Criterios de Aceptación**:
- ✅ El sistema valida que el email no esté registrado previamente
- ✅ Se valida formato correcto de email (RFC 5322)
- ✅ Se valida formato de teléfono internacional (+52 5512345678)
- ✅ Nombre y apellido son campos obligatorios (1-100 caracteres)
- ✅ El sistema retorna HTTP 201 Created con los datos del cliente
- ✅ El sistema retorna HTTP 422 si los datos son inválidos

**Endpoint**: `POST /clients`

**Ejemplo de Petición**:
```json
{
  "name": "Juan",
  "lastname": "Pérez García",
  "email": "juan.perez@example.com",
  "telephone": "+525512345678"
}
```

**Reglas de Negocio**:
- Email debe ser único en el sistema
- Teléfono debe seguir formato internacional E.164
- Los datos personales se almacenan de forma segura

**Prioridad**: ALTA
**Estimación**: 3 puntos

---

### HU-C02: Consulta de Información de Cliente
**Como** cliente final
**Quiero** consultar mi información personal registrada
**Para** verificar que mis datos estén correctos

**Criterios de Aceptación**:
- ✅ El sistema retorna toda la información del cliente solicitado
- ✅ Retorna HTTP 200 OK si el cliente existe
- ✅ Retorna HTTP 404 Not Found si el cliente no existe
- ✅ No se exponen datos sensibles innecesarios

**Endpoint**: `GET /clients/{id_key}`

**Prioridad**: MEDIA
**Estimación**: 2 puntos

---

### HU-C03: Actualización de Datos Personales
**Como** cliente final
**Quiero** actualizar mi información personal (nombre, teléfono)
**Para** mantener mis datos actualizados

**Criterios de Aceptación**:
- ✅ El cliente puede actualizar cualquier campo excepto id_key
- ✅ Se validan todos los campos con las mismas reglas que el registro
- ✅ El email no puede duplicarse con otro cliente
- ✅ Retorna HTTP 200 OK con datos actualizados
- ✅ Retorna HTTP 404 si el cliente no existe

**Endpoint**: `PUT /clients/{id_key}`

**Ejemplo de Petición Parcial**:
```json
{
  "telephone": "+525587654321"
}
```

**Prioridad**: MEDIA
**Estimación**: 2 puntos

---

### HU-C04: Gestión de Direcciones de Entrega
**Como** cliente final
**Quiero** registrar múltiples direcciones de entrega
**Para** poder elegir dónde recibir mis pedidos

**Criterios de Aceptación**:
- ✅ Un cliente puede tener múltiples direcciones
- ✅ Cada dirección incluye: calle, número, ciudad
- ✅ La dirección se vincula automáticamente al cliente
- ✅ Al eliminar un cliente, sus direcciones se eliminan automáticamente (cascade)

**Endpoints**:
- `POST /addresses` - Crear dirección
- `GET /addresses?client_id={id}` - Listar direcciones del cliente
- `DELETE /addresses/{id_key}` - Eliminar dirección

**Ejemplo de Petición**:
```json
{
  "street": "Av. Reforma",
  "number": "123",
  "city": "Ciudad de México",
  "client_id": 1
}
```

**Prioridad**: ALTA
**Estimación**: 3 puntos

---

## Módulo de Productos y Catálogo

### HU-P01: Búsqueda y Navegación de Productos
**Como** cliente final
**Quiero** buscar y navegar por el catálogo de productos
**Para** encontrar artículos que deseo comprar

**Criterios de Aceptación**:
- ✅ El listado de productos está paginado (skip/limit)
- ✅ Los productos incluyen: nombre, precio, stock, categoría
- ✅ Los resultados se cachean en Redis durante 5 minutos
- ✅ Se muestra stock disponible en tiempo real
- ✅ Header `X-Cache-Hit: true` indica si vino de caché

**Endpoint**: `GET /products?skip=0&limit=20`

**Respuesta Ejemplo**:
```json
[
  {
    "id_key": 1,
    "name": "Laptop Dell XPS 13",
    "price": 1299.99,
    "stock": 15,
    "category_id": 1
  }
]
```

**Reglas de Negocio**:
- Solo se muestran productos con stock > 0 (configurable)
- Caché se invalida al crear/actualizar/eliminar productos
- Máximo 1000 resultados por página

**Prioridad**: CRÍTICA
**Estimación**: 5 puntos

---

### HU-P02: Consulta de Detalle de Producto
**Como** cliente final
**Quiero** ver los detalles completos de un producto
**Para** tomar una decisión de compra informada

**Criterios de Aceptación**:
- ✅ Muestra toda la información del producto
- ✅ Incluye stock disponible en tiempo real
- ✅ La respuesta se cachea durante 5 minutos
- ✅ Retorna HTTP 404 si el producto no existe

**Endpoint**: `GET /products/{id_key}`

**Prioridad**: ALTA
**Estimación**: 2 puntos

---

### HU-P03: Gestión de Catálogo de Productos (Admin)
**Como** administrador de tienda
**Quiero** agregar, editar y eliminar productos del catálogo
**Para** mantener la oferta actualizada

**Criterios de Aceptación**:
- ✅ Precio debe ser mayor a 0
- ✅ Stock debe ser mayor o igual a 0
- ✅ Categoría debe existir en el sistema
- ✅ Al crear/actualizar producto, se invalida caché
- ✅ No se puede eliminar producto con historial de ventas (HTTP 409)

**Endpoints**:
- `POST /products` - Crear producto
- `PUT /products/{id_key}` - Actualizar producto
- `DELETE /products/{id_key}` - Eliminar producto

**Ejemplo de Creación**:
```json
{
  "name": "iPhone 15 Pro",
  "price": 999.99,
  "stock": 50,
  "category_id": 2
}
```

**Reglas de Negocio**:
- Productos con pedidos asociados NO pueden eliminarse
- Sugerir marcar como "inactivo" en lugar de eliminar
- Actualización de stock es atómica (previene race conditions)

**Prioridad**: CRÍTICA
**Estimación**: 5 puntos

---

### HU-P04: Organización por Categorías
**Como** administrador de tienda
**Quiero** organizar productos en categorías
**Para** facilitar la navegación de clientes

**Criterios de Aceptación**:
- ✅ Las categorías tienen nombres únicos
- ✅ Un producto pertenece a una sola categoría
- ✅ Listado de categorías se cachea durante 1 hora (cambian poco)
- ✅ No se puede eliminar categoría con productos asociados

**Endpoints**:
- `POST /categories` - Crear categoría
- `GET /categories` - Listar categorías (cached 1h)
- `PUT /categories/{id_key}` - Actualizar categoría
- `DELETE /categories/{id_key}` - Eliminar categoría

**Prioridad**: ALTA
**Estimación**: 3 puntos

---

## Módulo de Pedidos

### HU-O01: Creación de Pedido
**Como** cliente final
**Quiero** crear un pedido con los productos que deseo comprar
**Para** completar mi compra

**Criterios de Aceptación**:
- ✅ El cliente debe existir en el sistema (validación FK)
- ✅ La factura debe existir en el sistema (validación FK)
- ✅ Se selecciona método de entrega (Drive-thru, En mano, A domicilio)
- ✅ Estado inicial es "PENDIENTE"
- ✅ La fecha se asigna automáticamente al momento de creación
- ✅ Retorna HTTP 404 si cliente o factura no existen

**Endpoint**: `POST /orders`

**Ejemplo de Petición**:
```json
{
  "total": 1299.99,
  "delivery_method": 3,
  "status": 1,
  "client_id": 1,
  "bill_id": 1
}
```

**Valores de Enums**:
```python
DeliveryMethod:
  DRIVE_THRU = 1
  ON_HAND = 2
  HOME_DELIVERY = 3

Status:
  PENDING = 1
  IN_PROGRESS = 2
  DELIVERED = 3
  CANCELED = 4
```

**Reglas de Negocio**:
- Validación de integridad referencial antes de persistir
- Fecha de pedido se establece en servidor (UTC)
- Total debe coincidir con suma de detalles de pedido

**Prioridad**: CRÍTICA
**Estimación**: 5 puntos

---

### HU-O02: Agregar Productos al Pedido
**Como** cliente final
**Quiero** agregar productos a mi pedido con cantidades específicas
**Para** comprar múltiples artículos

**Criterios de Aceptación**:
- ✅ Se verifica stock disponible antes de agregar
- ✅ El precio debe coincidir con el precio actual del producto
- ✅ Stock se decrementa automáticamente al agregar
- ✅ Retorna HTTP 400 si stock insuficiente
- ✅ Retorna HTTP 400 si hay discrepancia de precio

**Endpoint**: `POST /order_details`

**Ejemplo de Petición**:
```json
{
  "quantity": 2,
  "price": 1299.99,
  "order_id": 1,
  "product_id": 1
}
```

**Validaciones Críticas**:
```python
# Validación de Stock
if product.stock < quantity:
    raise HTTP 400 "Insufficient stock for product {id}.
                    Requested: {quantity}, Available: {stock}"

# Validación de Precio (previene fraude)
if schema.price != product.price:
    raise HTTP 400 "Price mismatch.
                    Expected {product.price}, got {schema.price}"
```

**Reglas de Negocio**:
- Operación es atómica (lock de fila en producto)
- Si falla, no se decrementa stock ni se crea detalle
- Stock actualizado es visible inmediatamente para otras peticiones

**Prioridad**: CRÍTICA
**Estimación**: 8 puntos

---

### HU-O03: Seguimiento de Estado de Pedido
**Como** cliente final
**Quiero** consultar el estado actual de mi pedido
**Para** saber cuándo llegará

**Criterios de Aceptación**:
- ✅ Muestra estado actual (Pendiente, En Progreso, Entregado, Cancelado)
- ✅ Muestra método de entrega seleccionado
- ✅ Muestra fecha de creación del pedido
- ✅ Retorna HTTP 200 con información completa

**Endpoint**: `GET /orders/{id_key}`

**Respuesta Ejemplo**:
```json
{
  "id_key": 1,
  "date": "2025-11-17T10:30:00Z",
  "total": 1299.99,
  "delivery_method": 3,
  "status": 2,
  "client_id": 1,
  "bill_id": 1
}
```

**Prioridad**: ALTA
**Estimación**: 2 puntos

---

### HU-O04: Cancelación de Pedido
**Como** cliente final
**Quiero** cancelar un pedido que aún no ha sido entregado
**Para** evitar cargos por compras no deseadas

**Criterios de Aceptación**:
- ✅ Solo se pueden cancelar pedidos en estado PENDING o IN_PROGRESS
- ✅ Al cancelar, el stock de productos se restaura automáticamente
- ✅ Estado cambia a CANCELED
- ✅ Se mantiene historial del pedido cancelado

**Endpoint**: `PUT /orders/{id_key}`

**Ejemplo de Cancelación**:
```json
{
  "status": 4
}
```

**Reglas de Negocio**:
- Pedidos DELIVERED no pueden cancelarse
- Restauración de stock es atómica
- Se notifica al sistema de facturación

**Prioridad**: ALTA
**Estimación**: 5 puntos

---

## Módulo de Facturación

### HU-F01: Generación de Factura
**Como** operador de pedidos
**Quiero** generar una factura para un pedido
**Para** cumplir con requisitos fiscales

**Criterios de Aceptación**:
- ✅ Número de factura es único y auto-generado
- ✅ Incluye descuentos aplicables
- ✅ Calcula total con descuentos
- ✅ Registra tipo de pago (Efectivo, Tarjeta)
- ✅ Fecha de factura se genera automáticamente

**Endpoint**: `POST /bills`

**Ejemplo de Petición**:
```json
{
  "bill_number": "BILL-2025-001234",
  "discount": 50.00,
  "total": 1249.99,
  "payment_type": "card"
}
```

**Valores de PaymentType**:
```python
PaymentType:
  CASH = "cash"
  CARD = "card"
```

**Reglas de Negocio**:
- Número de factura debe ser único
- Total debe ser >= 0
- Descuento debe ser >= 0 y <= total
- Fecha es timestamp del servidor

**Prioridad**: ALTA
**Estimación**: 3 puntos

---

## Módulo de Reseñas

### HU-R01: Dejar Reseña de Producto
**Como** cliente final
**Quiero** dejar una reseña y calificación de un producto comprado
**Para** compartir mi experiencia con otros clientes

**Criterios de Aceptación**:
- ✅ Calificación es de 0.0 a 5.0
- ✅ Comentario es opcional
- ✅ Reseña se vincula al producto específico
- ✅ Producto debe existir (validación FK)

**Endpoint**: `POST /reviews`

**Ejemplo de Petición**:
```json
{
  "rating": 4.5,
  "comment": "Excelente producto, muy buena calidad. Llegó a tiempo.",
  "product_id": 1
}
```

**Reglas de Negocio**:
- Rating debe estar en rango [0.0, 5.0]
- Un cliente puede dejar múltiples reseñas (una por compra)
- Reseñas no son editables (solo pueden eliminarse)

**Prioridad**: MEDIA
**Estimación**: 3 puntos

---

## Administración del Sistema

### HU-A01: Monitoreo de Salud del Sistema
**Como** administrador de sistema
**Quiero** consultar el estado de salud de todos los componentes
**Para** detectar problemas antes de que afecten a usuarios

**Criterios de Aceptación**:
- ✅ Verifica conectividad con PostgreSQL
- ✅ Verifica conectividad con Redis
- ✅ Muestra métricas de pool de conexiones
- ✅ Muestra latencia de base de datos
- ✅ Retorna HTTP 200 si todo está OK
- ✅ Retorna HTTP 500 si base de datos está caída

**Endpoint**: `GET /health_check`

**Respuesta Ejemplo**:
```json
{
  "status": "healthy",
  "timestamp": "2025-11-17T10:00:00.000Z",
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

**Umbrales de Alertas**:
- ✅ **Healthy**: Latencia < 100ms, Pool < 70%
- ⚠️ **Warning**: Latencia 100-500ms, Pool 70-90%
- 🔴 **Critical**: Latencia > 500ms, Pool > 90%, Redis caído

**Prioridad**: CRÍTICA
**Estimación**: 5 puntos

---

### HU-A02: Pruebas de Carga
**Como** administrador de sistema
**Quiero** ejecutar pruebas de carga en el sistema
**Para** validar que soporta 400+ usuarios concurrentes

**Criterios de Aceptación**:
- ✅ Sistema maneja 400 usuarios concurrentes
- ✅ Tasa de error < 1%
- ✅ Tiempo de respuesta p95 < 200ms
- ✅ Pool de conexiones no se agota (< 90%)

**Herramienta**: Locust

**Comando**:
```bash
locust -f load_test.py \
  --host=http://localhost:8000 \
  --users 400 \
  --spawn-rate 50 \
  --run-time 5m \
  --headless
```

**Prioridad**: ALTA
**Estimación**: 3 puntos

---

## Resumen de Prioridades

| Prioridad | Historias de Usuario | Puntos Totales |
|-----------|---------------------|----------------|
| **CRÍTICA** | HU-P01, HU-P03, HU-O01, HU-O02, HU-A01 | 28 puntos |
| **ALTA** | HU-C01, HU-C04, HU-P02, HU-P04, HU-O03, HU-O04, HU-F01, HU-A02 | 26 puntos |
| **MEDIA** | HU-C02, HU-C03, HU-R01 | 7 puntos |

**Total**: 61 puntos de historia

---

## Matriz de Trazabilidad

| Historia de Usuario | Endpoint(s) | Modelo(s) | Servicio(s) | Pruebas |
|---------------------|-------------|-----------|-------------|---------|
| HU-C01 | POST /clients | Client | ClientService | test_services.py::TestClientService |
| HU-C02 | GET /clients/{id} | Client | ClientService | test_controllers.py::TestClientController |
| HU-C03 | PUT /clients/{id} | Client | ClientService | test_services.py::TestClientService |
| HU-C04 | POST /addresses | Address | AddressService | test_integration.py |
| HU-P01 | GET /products | Product | ProductService | test_services.py::TestProductService |
| HU-P02 | GET /products/{id} | Product | ProductService | test_controllers.py::TestProductController |
| HU-P03 | POST/PUT/DELETE /products | Product | ProductService | test_medium_priority_fixes.py::test_prevent_product_deletion |
| HU-P04 | POST/GET/PUT/DELETE /categories | Category | CategoryService | test_services.py::TestCategoryService |
| HU-O01 | POST /orders | Order | OrderService | test_services.py::TestOrderService::test_save_order_invalid_client |
| HU-O02 | POST /order_details | OrderDetail | OrderDetailService | test_services.py::TestOrderDetailService::test_save_order_detail_insufficient_stock |
| HU-O03 | GET /orders/{id} | Order | OrderService | test_controllers.py::TestOrderController |
| HU-O04 | PUT /orders/{id} | Order | OrderService | test_integration.py::test_order_cancellation_restores_stock |
| HU-F01 | POST /bills | Bill | BillService | test_services.py::TestBillService |
| HU-R01 | POST /reviews | Review | ReviewService | test_services.py::TestReviewService |
| HU-A01 | GET /health_check | - | HealthCheck | test_controllers.py::test_health_check_healthy |
| HU-A02 | - | - | - | load_test.py |

---

**Documento creado**: 2025-11-17
**Versión**: 1.0
**Autor**: Analista de Sistemas - Product Owner