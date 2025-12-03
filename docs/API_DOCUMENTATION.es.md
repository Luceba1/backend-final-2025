# Documentación Completa de APIs - Sistema E-commerce

## Índice
- [Introducción](#introducción)
- [Autenticación](#autenticación)
- [Códigos de Respuesta HTTP](#códigos-de-respuesta-http)
- [Paginación](#paginación)
- [Manejo de Errores](#manejo-de-errores)
- [Rate Limiting](#rate-limiting)
- [API de Clientes](#api-de-clientes)
- [API de Productos](#api-de-productos)
- [API de Categorías](#api-de-categorías)
- [API de Pedidos](#api-de-pedidos)
- [API de Detalles de Pedido](#api-de-detalles-de-pedido)
- [API de Facturas](#api-de-facturas)
- [API de Direcciones](#api-de-direcciones)
- [API de Reseñas](#api-de-reseñas)
- [API de Health Check](#api-de-health-check)

---

## Introducción

Esta documentación describe todos los endpoints disponibles en la API REST de E-commerce. La API está construida con FastAPI y sigue los principios RESTful.

### URL Base

```
Desarrollo:  http://localhost:8000
Producción:  https://api.example.com
```

### Formato de Datos

- **Request Content-Type**: `application/json`
- **Response Content-Type**: `application/json`
- **Charset**: UTF-8

### Versionado

Actualmente la API no usa versionado en la URL. Futuras versiones usarán el patrón `/v1/`, `/v2/`, etc.

---

## Autenticación

**Estado actual**: La API **no requiere autenticación**.

**Recomendación para producción**: Implementar JWT (JSON Web Tokens) o API Keys.

Ejemplo de implementación futura:
```http
Authorization: Bearer <token>
```

---

## Códigos de Respuesta HTTP

La API utiliza códigos de estado HTTP estándar:

| Código | Significado | Descripción |
|--------|-------------|-------------|
| **200** | OK | Petición exitosa (GET, PUT) |
| **201** | Created | Recurso creado exitosamente (POST) |
| **204** | No Content | Eliminación exitosa sin contenido (DELETE) |
| **400** | Bad Request | Datos de entrada inválidos (validación fallida) |
| **404** | Not Found | Recurso no encontrado |
| **409** | Conflict | Conflicto (ej: email duplicado, producto con ventas) |
| **422** | Unprocessable Entity | Error de validación de Pydantic |
| **429** | Too Many Requests | Rate limit excedido |
| **500** | Internal Server Error | Error interno del servidor |

---

## Paginación

Todos los endpoints de listado soportan paginación mediante query parameters.

### Parámetros

```http
GET /endpoint?skip=0&limit=100
```

| Parámetro | Tipo | Default | Máximo | Descripción |
|-----------|------|---------|--------|-------------|
| `skip` | integer | 0 | - | Número de registros a saltar |
| `limit` | integer | 100 | 1000 | Número máximo de registros a retornar |

### Ejemplo

```bash
# Obtener los primeros 20 productos
curl "http://localhost:8000/products?skip=0&limit=20"

# Obtener los siguientes 20 productos (página 2)
curl "http://localhost:8000/products?skip=20&limit=20"

# Obtener los siguientes 20 productos (página 3)
curl "http://localhost:8000/products?skip=40&limit=20"
```

---

## Manejo de Errores

### Formato de Error Estándar

```json
{
  "detail": "Descripción del error"
}
```

### Error de Validación (422)

```json
{
  "detail": [
    {
      "loc": ["body", "price"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt"
    }
  ]
}
```

### Ejemplos de Errores Comunes

**Recurso no encontrado (404)**:
```json
{
  "message": "Product with id 999 not found"
}
```

**Stock insuficiente (400)**:
```json
{
  "detail": "Insufficient stock for product 1. Requested: 10, Available: 5"
}
```

**Discrepancia de precio (400)**:
```json
{
  "detail": "Price mismatch. Expected 999.99, got 899.99"
}
```

**Clave foránea inválida (404)**:
```json
{
  "message": "Client with id 999 not found"
}
```

---

## Rate Limiting

### Límites Actuales

- **100 peticiones** por **60 segundos** por dirección IP
- Basado en Redis con operaciones atómicas
- El endpoint `/health_check` está excluido del rate limiting

### Headers de Respuesta

Todas las respuestas incluyen headers informativos:

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 60
```

### Respuesta al Exceder Límite (429)

```json
{
  "detail": "Rate limit exceeded. Maximum 100 requests per 60 seconds.",
  "retry_after": 45
}
```

**Headers adicionales**:
```http
Retry-After: 45
```

---

## API de Clientes

Gestión de clientes del sistema.

### Listar Todos los Clientes

```http
GET /clients
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "name": "Juan",
    "lastname": "Pérez García",
    "email": "juan.perez@example.com",
    "telephone": "+525512345678"
  },
  {
    "id_key": 2,
    "name": "María",
    "lastname": "López",
    "email": "maria.lopez@example.com",
    "telephone": "+525587654321"
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/clients?skip=0&limit=10"
```

---

### Obtener Cliente por ID

```http
GET /clients/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del cliente

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Juan",
  "lastname": "Pérez García",
  "email": "juan.perez@example.com",
  "telephone": "+525512345678"
}
```

**Respuesta 404 Not Found**:
```json
{
  "message": "Client with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/clients/1"
```

---

### Crear Nuevo Cliente

```http
POST /clients
```

**Request Body**:
```json
{
  "name": "Juan",
  "lastname": "Pérez García",
  "email": "juan.perez@example.com",
  "telephone": "+525512345678"
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `name` | string | Sí | 1-100 caracteres |
| `lastname` | string | Sí | 1-100 caracteres |
| `email` | EmailStr | Sí | Formato email válido, único |
| `telephone` | string | Sí | Formato internacional E.164 |

**Validación de Teléfono**:
- Pattern: `^\+?[1-9]\d{6,19}$`
- Ejemplos válidos: `+525512345678`, `+14155552671`
- Ejemplos inválidos: `5512345678`, `12-3456-7890`

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "name": "Juan",
  "lastname": "Pérez García",
  "email": "juan.perez@example.com",
  "telephone": "+525512345678"
}
```

**Respuesta 409 Conflict** (email duplicado):
```json
{
  "detail": "Email already registered"
}
```

**Respuesta 422 Unprocessable Entity** (validación fallida):
```json
{
  "detail": [
    {
      "loc": ["body", "email"],
      "msg": "value is not a valid email address",
      "type": "value_error.email"
    }
  ]
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/clients" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Juan",
    "lastname": "Pérez García",
    "email": "juan.perez@example.com",
    "telephone": "+525512345678"
  }'
```

---

### Actualizar Cliente

```http
PUT /clients/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del cliente

**Request Body** (todos los campos opcionales):
```json
{
  "name": "Juan Carlos",
  "telephone": "+525599887766"
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Juan Carlos",
  "lastname": "Pérez García",
  "email": "juan.perez@example.com",
  "telephone": "+525599887766"
}
```

**Respuesta 404 Not Found**:
```json
{
  "message": "Client with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/clients/1" \
  -H "Content-Type: application/json" \
  -d '{
    "telephone": "+525599887766"
  }'
```

---

### Eliminar Cliente

```http
DELETE /clients/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del cliente

**Comportamiento**:
- Elimina el cliente del sistema
- **Cascada**: También elimina direcciones asociadas
- **Advertencia**: NO elimina pedidos asociados (integridad referencial)

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Respuesta 404 Not Found**:
```json
{
  "message": "Client with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/clients/1"
```

---

## API de Productos

Gestión del catálogo de productos con **soporte de caché Redis**.

### Características de Caché

- **TTL**: 5 minutos
- **Cache Keys**:
  - Lista: `products:list:skip:0:limit:10`
  - Individual: `products:id:123`
- **Invalidación**: Automática en POST/PUT/DELETE
- **Header**: `X-Cache-Hit: true` indica caché

---

### Listar Todos los Productos

```http
GET /products
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "name": "Laptop Dell XPS 13",
    "price": 1299.99,
    "stock": 15,
    "category_id": 1
  },
  {
    "id_key": 2,
    "name": "iPhone 15 Pro",
    "price": 999.99,
    "stock": 25,
    "category_id": 2
  }
]
```

**Headers de Respuesta**:
```http
X-Cache-Hit: true
Content-Type: application/json
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/products?skip=0&limit=20"
```

---

### Obtener Producto por ID

```http
GET /products/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del producto

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Laptop Dell XPS 13",
  "price": 1299.99,
  "stock": 15,
  "category_id": 1
}
```

**Respuesta 404 Not Found**:
```json
{
  "message": "Product with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/products/1"
```

---

### Crear Nuevo Producto

```http
POST /products
```

**Request Body**:
```json
{
  "name": "Laptop Dell XPS 13",
  "price": 1299.99,
  "stock": 15,
  "category_id": 1
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `name` | string | Sí | 1-200 caracteres |
| `price` | float | Sí | > 0 |
| `stock` | integer | No | >= 0, default: 0 |
| `category_id` | integer | Sí | Categoría debe existir |

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "name": "Laptop Dell XPS 13",
  "price": 1299.99,
  "stock": 15,
  "category_id": 1
}
```

**Efecto Secundario**:
- ✅ Invalida **todas** las claves de caché de productos (`products:*`)

**Respuesta 422 Unprocessable Entity** (precio inválido):
```json
{
  "detail": [
    {
      "loc": ["body", "price"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt"
    }
  ]
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/products" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Laptop Dell XPS 13",
    "price": 1299.99,
    "stock": 15,
    "category_id": 1
  }'
```

---

### Actualizar Producto

```http
PUT /products/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del producto

**Request Body** (todos los campos opcionales):
```json
{
  "price": 1199.99,
  "stock": 20
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Laptop Dell XPS 13",
  "price": 1199.99,
  "stock": 20,
  "category_id": 1
}
```

**Efecto Secundario**:
- ✅ Invalida caché del producto específico
- ✅ Invalida todas las listas de productos

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/products/1" \
  -H "Content-Type: application/json" \
  -d '{
    "price": 1199.99,
    "stock": 20
  }'
```

---

### Eliminar Producto

```http
DELETE /products/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del producto

**Validación Crítica**:
- ❌ **NO se puede eliminar** producto con historial de ventas (order_details asociados)
- ✅ Retorna HTTP 409 Conflict con mensaje descriptivo

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta, producto eliminado exitosamente)

**Respuesta 409 Conflict** (tiene ventas):
```json
{
  "detail": "Cannot delete product 1: product has associated sales history. Consider marking as inactive instead of deleting."
}
```

**Respuesta 404 Not Found**:
```json
{
  "message": "Product with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/products/1"
```

---

## API de Categorías

Gestión de categorías de productos con **caché de larga duración**.

### Características de Caché

- **TTL**: 1 hora (3600 segundos)
- **Razón**: Las categorías cambian raramente
- **Invalidación**: Automática en POST/PUT/DELETE

---

### Listar Todas las Categorías

```http
GET /categories
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "name": "Electrónica"
  },
  {
    "id_key": 2,
    "name": "Ropa"
  },
  {
    "id_key": 3,
    "name": "Hogar"
  }
]
```

**Headers**:
```http
X-Cache-Hit: true
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/categories"
```

---

### Obtener Categoría por ID

```http
GET /categories/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la categoría

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Electrónica"
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/categories/1"
```

---

### Crear Nueva Categoría

```http
POST /categories
```

**Request Body**:
```json
{
  "name": "Deportes"
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `name` | string | Sí | 1-100 caracteres, único |

**Respuesta 201 Created**:
```json
{
  "id_key": 4,
  "name": "Deportes"
}
```

**Respuesta 409 Conflict** (nombre duplicado):
```json
{
  "detail": "Category name already exists"
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/categories" \
  -H "Content-Type: application/json" \
  -d '{"name": "Deportes"}'
```

---

### Actualizar Categoría

```http
PUT /categories/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la categoría

**Request Body**:
```json
{
  "name": "Electrónica y Tecnología"
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "name": "Electrónica y Tecnología"
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/categories/1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Electrónica y Tecnología"}'
```

---

### Eliminar Categoría

```http
DELETE /categories/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la categoría

**Validación**:
- ❌ No se puede eliminar categoría con productos asociados

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Respuesta 409 Conflict**:
```json
{
  "detail": "Cannot delete category with associated products"
}
```

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/categories/1"
```

---

## API de Pedidos

Gestión del ciclo completo de pedidos con **validación de integridad referencial**.

### Listar Todos los Pedidos

```http
GET /orders
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "date": "2025-11-17T10:30:00Z",
    "total": 1299.99,
    "delivery_method": 3,
    "status": 1,
    "client_id": 1,
    "bill_id": 1
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/orders?skip=0&limit=20"
```

---

### Obtener Pedido por ID

```http
GET /orders/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del pedido

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "date": "2025-11-17T10:30:00Z",
  "total": 1299.99,
  "delivery_method": 3,
  "status": 1,
  "client_id": 1,
  "bill_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/orders/1"
```

---

### Crear Nuevo Pedido

```http
POST /orders
```

**Request Body**:
```json
{
  "total": 1299.99,
  "delivery_method": 3,
  "status": 1,
  "client_id": 1,
  "bill_id": 1
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `date` | datetime | No | Auto-asignado si no se provee |
| `total` | float | Sí | >= 0 |
| `delivery_method` | integer | Sí | 1, 2 o 3 (enum) |
| `status` | integer | Sí | 1, 2, 3 o 4 (enum) |
| `client_id` | integer | Sí | Cliente debe existir |
| `bill_id` | integer | Sí | Factura debe existir |

**Enumeraciones**:

```python
DeliveryMethod:
  DRIVE_THRU = 1       # Recoger en auto
  ON_HAND = 2          # Recoger en tienda
  HOME_DELIVERY = 3    # Entrega a domicilio

Status:
  PENDING = 1          # Pendiente
  IN_PROGRESS = 2      # En progreso
  DELIVERED = 3        # Entregado
  CANCELED = 4         # Cancelado
```

**Validaciones Críticas**:
1. ✅ Verifica que `client_id` existe en tabla `clients`
2. ✅ Verifica que `bill_id` existe en tabla `bills`
3. ✅ Asigna fecha actual si no se provee

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "date": "2025-11-17T10:30:00.123456Z",
  "total": 1299.99,
  "delivery_method": 3,
  "status": 1,
  "client_id": 1,
  "bill_id": 1
}
```

**Respuesta 404 Not Found** (cliente no existe):
```json
{
  "message": "Client with id 999 not found"
}
```

**Respuesta 404 Not Found** (factura no existe):
```json
{
  "message": "Bill with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "total": 1299.99,
    "delivery_method": 3,
    "status": 1,
    "client_id": 1,
    "bill_id": 1
  }'
```

---

### Actualizar Pedido

```http
PUT /orders/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del pedido

**Request Body** (campos opcionales):
```json
{
  "status": 2
}
```

**Casos de Uso Comunes**:

**Marcar como En Progreso**:
```json
{"status": 2}
```

**Marcar como Entregado**:
```json
{"status": 3}
```

**Cancelar Pedido**:
```json
{"status": 4}
```

**Respuesta 200 OK**:
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

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/orders/1" \
  -H "Content-Type: application/json" \
  -d '{"status": 2}'
```

---

### Eliminar Pedido

```http
DELETE /orders/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del pedido

**Comportamiento**:
- Elimina el pedido del sistema
- **Cascada**: También elimina detalles de pedido asociados
- **Stock**: Se restaura automáticamente al eliminar detalles

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/orders/1"
```

---

## API de Detalles de Pedido

Gestión de líneas de pedido con **validación de stock y prevención de fraude**.

### Características Críticas

1. **Validación de Stock**: Verifica disponibilidad antes de confirmar
2. **Validación de Precio**: Previene manipulación de precios (anti-fraude)
3. **Actualización Atómica**: Stock se actualiza en transacción
4. **Restauración Automática**: Stock se restaura al cancelar/eliminar

---

### Listar Todos los Detalles de Pedido

```http
GET /order_details
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "quantity": 2,
    "price": 1299.99,
    "order_id": 1,
    "product_id": 1
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/order_details"
```

---

### Obtener Detalle de Pedido por ID

```http
GET /order_details/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del detalle

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "quantity": 2,
  "price": 1299.99,
  "order_id": 1,
  "product_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/order_details/1"
```

---

### Crear Nuevo Detalle de Pedido

```http
POST /order_details
```

**Request Body**:
```json
{
  "quantity": 2,
  "price": 1299.99,
  "order_id": 1,
  "product_id": 1
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `quantity` | integer | Sí | > 0 |
| `price` | float | Sí | > 0 |
| `order_id` | integer | Sí | Pedido debe existir |
| `product_id` | integer | Sí | Producto debe existir |

**Validaciones de Negocio Críticas**:

1. **Validación de Stock**:
```python
if product.stock < quantity:
    raise HTTP 400 "Insufficient stock for product {product_id}.
                    Requested: {quantity}, Available: {product.stock}"
```

2. **Validación de Precio** (Prevención de Fraude):
```python
if schema.price != product.price:
    raise HTTP 400 "Price mismatch.
                    Expected {product.price}, got {schema.price}"
```

3. **Actualización de Stock**:
```python
# Atómico en transacción
product.stock -= quantity
db.commit()
```

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "quantity": 2,
  "price": 1299.99,
  "order_id": 1,
  "product_id": 1
}
```

**Efecto Secundario**:
- ✅ Stock del producto se decrementa automáticamente
- ✅ Producto: stock 15 → 13 (si quantity = 2)

**Respuesta 400 Bad Request** (stock insuficiente):
```json
{
  "detail": "Insufficient stock for product 1. Requested: 10, Available: 5"
}
```

**Respuesta 400 Bad Request** (precio no coincide):
```json
{
  "detail": "Price mismatch. Expected 1299.99, got 999.99"
}
```

**Respuesta 404 Not Found** (producto no existe):
```json
{
  "message": "Product with id 999 not found"
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/order_details" \
  -H "Content-Type: application/json" \
  -d '{
    "quantity": 2,
    "price": 1299.99,
    "order_id": 1,
    "product_id": 1
  }'
```

---

### Actualizar Detalle de Pedido

```http
PUT /order_details/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del detalle

**Request Body** (campos opcionales):
```json
{
  "quantity": 3
}
```

**Comportamiento al Cambiar Cantidad**:

```python
# Aumentar cantidad (2 → 3)
if new_quantity > old_quantity:
    additional = new_quantity - old_quantity  # 1
    # Verifica stock disponible
    if product.stock < additional:
        raise HTTP 400 "Insufficient stock"
    # Decrementa stock adicional
    product.stock -= additional

# Disminuir cantidad (3 → 2)
if new_quantity < old_quantity:
    returned = old_quantity - new_quantity  # 1
    # Restaura stock
    product.stock += returned
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "quantity": 3,
  "price": 1299.99,
  "order_id": 1,
  "product_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/order_details/1" \
  -H "Content-Type: application/json" \
  -d '{"quantity": 3}'
```

---

### Eliminar Detalle de Pedido

```http
DELETE /order_details/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID del detalle

**Comportamiento**:
- Elimina el detalle del pedido
- **Restaura stock automáticamente**:
  ```python
  product.stock += order_detail.quantity
  ```

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Efecto Secundario**:
- ✅ Stock del producto se incrementa
- ✅ Si quantity era 2, stock aumenta en 2

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/order_details/1"
```

---

## API de Facturas

Gestión del sistema de facturación.

### Listar Todas las Facturas

```http
GET /bills
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "bill_number": "BILL-2025-001234",
    "discount": 50.00,
    "date": "2025-11-17",
    "total": 1249.99,
    "payment_type": "card"
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/bills"
```

---

### Obtener Factura por ID

```http
GET /bills/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la factura

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "bill_number": "BILL-2025-001234",
  "discount": 50.00,
  "date": "2025-11-17",
  "total": 1249.99,
  "payment_type": "card"
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/bills/1"
```

---

### Crear Nueva Factura

```http
POST /bills
```

**Request Body**:
```json
{
  "bill_number": "BILL-2025-001234",
  "discount": 50.00,
  "date": "2025-11-17",
  "total": 1249.99,
  "payment_type": "card"
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `bill_number` | string | Sí | Único, formato libre |
| `discount` | float | No | >= 0, default: 0 |
| `date` | date | Sí | Formato ISO 8601 |
| `total` | float | Sí | >= 0 |
| `payment_type` | string | Sí | "cash" o "card" |

**Valores de PaymentType**:
```python
PaymentType:
  CASH = "cash"
  CARD = "card"
```

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "bill_number": "BILL-2025-001234",
  "discount": 50.00,
  "date": "2025-11-17",
  "total": 1249.99,
  "payment_type": "card"
}
```

**Respuesta 409 Conflict** (número de factura duplicado):
```json
{
  "detail": "Bill number already exists"
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/bills" \
  -H "Content-Type: application/json" \
  -d '{
    "bill_number": "BILL-2025-001234",
    "discount": 50.00,
    "date": "2025-11-17",
    "total": 1249.99,
    "payment_type": "card"
  }'
```

---

### Actualizar Factura

```http
PUT /bills/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la factura

**Request Body** (campos opcionales):
```json
{
  "payment_type": "cash"
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "bill_number": "BILL-2025-001234",
  "discount": 50.00,
  "date": "2025-11-17",
  "total": 1249.99,
  "payment_type": "cash"
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/bills/1" \
  -H "Content-Type: application/json" \
  -d '{"payment_type": "cash"}'
```

---

### Eliminar Factura

```http
DELETE /bills/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la factura

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/bills/1"
```

---

## API de Direcciones

Gestión de direcciones de entrega de clientes.

### Listar Todas las Direcciones

```http
GET /addresses
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "street": "Av. Reforma",
    "number": "123",
    "city": "Ciudad de México",
    "client_id": 1
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/addresses"
```

---

### Obtener Dirección por ID

```http
GET /addresses/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la dirección

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "street": "Av. Reforma",
  "number": "123",
  "city": "Ciudad de México",
  "client_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/addresses/1"
```

---

### Crear Nueva Dirección

```http
POST /addresses
```

**Request Body**:
```json
{
  "street": "Av. Reforma",
  "number": "123",
  "city": "Ciudad de México",
  "client_id": 1
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `street` | string | Sí | 1-200 caracteres |
| `number` | string | No | Puede ser alfanumérico |
| `city` | string | Sí | 1-100 caracteres |
| `client_id` | integer | Sí | Cliente debe existir |

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "street": "Av. Reforma",
  "number": "123",
  "city": "Ciudad de México",
  "client_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/addresses" \
  -H "Content-Type: application/json" \
  -d '{
    "street": "Av. Reforma",
    "number": "123",
    "city": "Ciudad de México",
    "client_id": 1
  }'
```

---

### Actualizar Dirección

```http
PUT /addresses/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la dirección

**Request Body** (campos opcionales):
```json
{
  "number": "456"
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "street": "Av. Reforma",
  "number": "456",
  "city": "Ciudad de México",
  "client_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/addresses/1" \
  -H "Content-Type: application/json" \
  -d '{"number": "456"}'
```

---

### Eliminar Dirección

```http
DELETE /addresses/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la dirección

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/addresses/1"
```

---

## API de Reseñas

Gestión de reseñas y calificaciones de productos.

### Listar Todas las Reseñas

```http
GET /reviews
```

**Query Parameters**:
- `skip` (opcional): Registros a saltar (default: 0)
- `limit` (opcional): Registros a retornar (default: 100)

**Respuesta 200 OK**:
```json
[
  {
    "id_key": 1,
    "rating": 4.5,
    "comment": "Excelente producto, muy buena calidad.",
    "product_id": 1
  }
]
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/reviews"
```

---

### Obtener Reseña por ID

```http
GET /reviews/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la reseña

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "rating": 4.5,
  "comment": "Excelente producto, muy buena calidad.",
  "product_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/reviews/1"
```

---

### Crear Nueva Reseña

```http
POST /reviews
```

**Request Body**:
```json
{
  "rating": 4.5,
  "comment": "Excelente producto, muy buena calidad. Llegó a tiempo.",
  "product_id": 1
}
```

**Schema de Validación**:

| Campo | Tipo | Requerido | Validaciones |
|-------|------|-----------|--------------|
| `rating` | float | Sí | 0.0 <= rating <= 5.0 |
| `comment` | string | No | Opcional, texto libre |
| `product_id` | integer | Sí | Producto debe existir |

**Respuesta 201 Created**:
```json
{
  "id_key": 1,
  "rating": 4.5,
  "comment": "Excelente producto, muy buena calidad. Llegó a tiempo.",
  "product_id": 1
}
```

**Respuesta 422 Unprocessable Entity** (rating fuera de rango):
```json
{
  "detail": [
    {
      "loc": ["body", "rating"],
      "msg": "ensure this value is less than or equal to 5.0",
      "type": "value_error.number.not_le"
    }
  ]
}
```

**Ejemplo curl**:
```bash
curl -X POST "http://localhost:8000/reviews" \
  -H "Content-Type: application/json" \
  -d '{
    "rating": 4.5,
    "comment": "Excelente producto, muy buena calidad.",
    "product_id": 1
  }'
```

---

### Actualizar Reseña

```http
PUT /reviews/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la reseña

**Request Body** (campos opcionales):
```json
{
  "rating": 5.0,
  "comment": "Perfecto! Mejor de lo esperado."
}
```

**Respuesta 200 OK**:
```json
{
  "id_key": 1,
  "rating": 5.0,
  "comment": "Perfecto! Mejor de lo esperado.",
  "product_id": 1
}
```

**Ejemplo curl**:
```bash
curl -X PUT "http://localhost:8000/reviews/1" \
  -H "Content-Type: application/json" \
  -d '{
    "rating": 5.0,
    "comment": "Perfecto! Mejor de lo esperado."
  }'
```

---

### Eliminar Reseña

```http
DELETE /reviews/{id_key}
```

**Path Parameters**:
- `id_key` (requerido): ID de la reseña

**Respuesta 204 No Content**:
(Sin cuerpo de respuesta)

**Ejemplo curl**:
```bash
curl -X DELETE "http://localhost:8000/reviews/1"
```

---

## API de Health Check

Endpoint de monitoreo para verificar el estado del sistema.

### Verificar Estado del Sistema

```http
GET /health_check
```

**Sin parámetros requeridos**

**Respuesta 200 OK** (sistema saludable):
```json
{
  "status": "healthy",
  "timestamp": "2025-11-17T10:00:00.123456Z",
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

**Respuesta 200 OK** (Redis caído, pero sistema funcional):
```json
{
  "status": "degraded",
  "timestamp": "2025-11-17T10:00:00.123456Z",
  "checks": {
    "database": {
      "status": "up",
      "latency_ms": 18.45
    },
    "redis": {
      "status": "down",
      "error": "Connection refused"
    },
    "db_pool": {
      "size": 50,
      "checked_in": 42,
      "checked_out": 8,
      "overflow": 0,
      "total_capacity": 150,
      "utilization_percent": 5.3
    }
  }
}
```

**Respuesta 500 Internal Server Error** (base de datos caída):
```json
{
  "status": "critical",
  "timestamp": "2025-11-17T10:00:00.123456Z",
  "checks": {
    "database": {
      "status": "down",
      "error": "Connection refused"
    },
    "redis": {
      "status": "up"
    }
  }
}
```

**Campos de Respuesta**:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `status` | string | Estado general: "healthy", "degraded", "critical" |
| `timestamp` | datetime | Momento de la verificación |
| `checks.database.status` | string | Estado de PostgreSQL: "up" o "down" |
| `checks.database.latency_ms` | float | Latencia de ping a BD en milisegundos |
| `checks.redis.status` | string | Estado de Redis: "up" o "down" |
| `checks.db_pool.size` | integer | Tamaño base del pool de conexiones |
| `checks.db_pool.checked_in` | integer | Conexiones disponibles |
| `checks.db_pool.checked_out` | integer | Conexiones en uso |
| `checks.db_pool.overflow` | integer | Conexiones overflow en uso |
| `checks.db_pool.total_capacity` | integer | Capacidad total (size + max_overflow) |
| `checks.db_pool.utilization_percent` | float | Porcentaje de utilización del pool |

**Umbrales de Estado**:

```python
Estado "healthy":
  • Database: up
  • Database latency: < 100ms
  • Redis: up
  • Pool utilization: < 70%

Estado "degraded":
  • Database: up
  • Database latency: 100-500ms
  • Redis: down (tolerable)
  • Pool utilization: 70-90%

Estado "critical":
  • Database: down
  • Database latency: > 500ms
  • Pool utilization: > 90%
```

**Notas**:
- Este endpoint **NO** está sujeto a rate limiting
- Útil para health checks de Kubernetes, Docker, load balancers
- Retorna HTTP 500 si base de datos está caída

**Ejemplo curl**:
```bash
curl -X GET "http://localhost:8000/health_check"
```

**Ejemplo con jq (formateado)**:
```bash
curl -s "http://localhost:8000/health_check" | jq
```

---

## Apéndices

### Resumen de Endpoints

| Recurso | GET (lista) | GET (id) | POST | PUT | DELETE | Total |
|---------|-------------|----------|------|-----|--------|-------|
| /clients | ✅ | ✅ | ✅ | ✅ | ✅ | 5 |
| /products | ✅ cached | ✅ cached | ✅ | ✅ | ✅ | 5 |
| /categories | ✅ cached | ✅ cached | ✅ | ✅ | ✅ | 5 |
| /orders | ✅ | ✅ | ✅ FK | ✅ | ✅ | 5 |
| /order_details | ✅ | ✅ | ✅ stock | ✅ | ✅ stock | 5 |
| /bills | ✅ | ✅ | ✅ | ✅ | ✅ | 5 |
| /addresses | ✅ | ✅ | ✅ | ✅ | ✅ | 5 |
| /reviews | ✅ | ✅ | ✅ | ✅ | ✅ | 5 |
| /health_check | ✅ | - | - | - | - | 1 |
| **TOTAL** | | | | | | **41** |

### Convenciones de Nomenclatura

| Convención | Ejemplo | Uso |
|------------|---------|-----|
| **snake_case** | `id_key`, `client_id`, `payment_type` | Nombres de campos JSON |
| **PascalCase** | `ClientSchema`, `ProductModel` | Nombres de clases |
| **kebab-case** | `/order-details` | URLs (opcional, aquí se usa snake_case) |
| **SCREAMING_SNAKE_CASE** | `PENDING`, `IN_PROGRESS` | Valores de enums |

### Tipos de Datos

| Tipo JSON | Tipo Python | Tipo PostgreSQL | Ejemplo |
|-----------|-------------|-----------------|---------|
| string | str | VARCHAR | "Juan Pérez" |
| integer | int | INTEGER | 123 |
| number | float | FLOAT | 1299.99 |
| boolean | bool | BOOLEAN | true |
| null | None | NULL | null |
| array | list | - | [1, 2, 3] |
| object | dict | JSONB | {"key": "value"} |

### Guía de Testing

**Probar con curl**:
```bash
# Guardar respuesta en archivo
curl http://localhost:8000/products > products.json

# Mostrar solo headers
curl -I http://localhost:8000/products

# Medir tiempo de respuesta
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/products

# Con autenticación (futura)
curl -H "Authorization: Bearer <token>" http://localhost:8000/products
```

**Probar con Postman**:
1. Importar colección desde Swagger: http://localhost:8000/docs
2. Configurar variable de entorno `{{baseUrl}}` = `http://localhost:8000`
3. Ejecutar colección completa con Collection Runner

**Probar con Python**:
```python
import requests

# GET
response = requests.get("http://localhost:8000/products")
print(response.json())

# POST
new_client = {
    "name": "Juan",
    "lastname": "Pérez",
    "email": "juan@example.com",
    "telephone": "+525512345678"
}
response = requests.post(
    "http://localhost:8000/clients",
    json=new_client
)
print(response.status_code)  # 201
print(response.json())
```

---

**Documento creado**: 2025-11-17
**Versión**: 1.0
**Autor**: Analista de Sistemas - Product Owner