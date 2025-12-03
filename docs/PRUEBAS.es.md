# 🧪 Guía de Pruebas - API E-commerce

Documentación completa del sistema de pruebas del proyecto.

---

## 📋 Tabla de Contenidos

- [Resumen del Sistema de Pruebas](#-resumen-del-sistema-de-pruebas)
- [Estructura de Pruebas](#-estructura-de-pruebas)
- [Ejecutar Pruebas](#-ejecutar-pruebas)
- [Pruebas por Capa](#-pruebas-por-capa)
- [Fixtures y Datos de Prueba](#-fixtures-y-datos-de-prueba)
- [Pruebas de Integración](#-pruebas-de-integración)
- [Pruebas de Carga](#-pruebas-de-carga)
- [Cobertura de Código](#-cobertura-de-código)
- [Escribir Nuevas Pruebas](#-escribir-nuevas-pruebas)

---

## 🎯 Resumen del Sistema de Pruebas

El proyecto cuenta con **189 pruebas automatizadas** que cubren:

- ✅ **Modelos** (SQLAlchemy ORM)
- ✅ **Repositories** (Acceso a datos)
- ✅ **Services** (Lógica de negocio)
- ✅ **Controllers** (Endpoints HTTP)
- ✅ **Integration** (Flujos end-to-end)
- ✅ **Middleware** (Rate limiting, logging)

### Métricas de Cobertura

```
Models:          ~95%
Repositories:    ~90%
Services:        ~85%
Controllers:     ~80%
Overall:         >80%
```

---

## 📁 Estructura de Pruebas

```
tests/
├── conftest.py                      # Fixtures compartidas
├── test_models.py                   # 30+ pruebas de modelos
├── test_repositories.py             # 20+ pruebas de repositorios
├── test_services.py                 # 50+ pruebas de servicios
├── test_controllers.py              # 40+ pruebas de controladores
├── test_integration.py              # 15+ pruebas de integración
├── test_middleware.py               # 10+ pruebas de middleware
├── test_concurrency.py              # Pruebas de concurrencia
├── test_logging_utils.py            # 28 pruebas de logging
├── test_medium_priority_fixes.py    # 15 pruebas de validación
└── README_TESTS.md                  # Documentación detallada
```

---

## 🚀 Ejecutar Pruebas

### Comandos Básicos

```bash
# Todas las pruebas
pytest tests/ -v

# Con cobertura
pytest tests/ --cov=. --cov-report=html

# Abrir reporte de cobertura
# Windows: start htmlcov/index.html
# Linux/Mac: open htmlcov/index.html
```

### Pruebas por Módulo

```bash
# Pruebas de modelos
pytest tests/test_models.py -v

# Pruebas de repositorios
pytest tests/test_repositories.py -v

# Pruebas de servicios
pytest tests/test_services.py -v

# Pruebas de controladores
pytest tests/test_controllers.py -v

# Pruebas de integración
pytest tests/test_integration.py -v
```

### Prueba Individual

```bash
# Ejecutar prueba específica
pytest tests/test_services.py::TestOrderDetailService::test_save_order_detail_insufficient_stock -v

# Con output detallado
pytest tests/test_services.py::TestOrderDetailService::test_save_order_detail_insufficient_stock -vvs
```

### Ejecutar en Paralelo

```bash
# Instalar plugin
pip install pytest-xdist

# Ejecutar pruebas en paralelo
pytest tests/ -n auto

# Especificar número de workers
pytest tests/ -n 4
```

---

## 🧩 Pruebas por Capa

### 1. Pruebas de Modelos (test_models.py)

**Objetivo**: Verificar modelos SQLAlchemy y relaciones ORM.

**Ejemplos**:

```python
def test_product_model_creation(db_session):
    """Verifica creación de producto"""
    category = CategoryModel(name="Electronics")
    db_session.add(category)
    db_session.commit()

    product = ProductModel(
        name="Laptop",
        price=999.99,
        stock=10,
        category_id=category.id_key
    )
    db_session.add(product)
    db_session.commit()

    assert product.id_key is not None
    assert product.name == "Laptop"
    assert product.category.name == "Electronics"

def test_product_cascade_delete_reviews(db_session):
    """Verifica que al eliminar producto se eliminan reseñas"""
    product = ProductModel(name="Test", price=10, stock=5)
    review = ReviewModel(rating=5.0, comment="Great!", product=product)

    db_session.add_all([product, review])
    db_session.commit()

    db_session.delete(product)
    db_session.commit()

    # Reseña debe estar eliminada (cascade)
    assert db_session.query(ReviewModel).count() == 0
```

**Cobertura**: ~95%

### 2. Pruebas de Repositorios (test_repositories.py)

**Objetivo**: Verificar operaciones CRUD y manejo de excepciones.

**Ejemplos**:

```python
def test_repository_find_all_pagination(product_repository):
    """Verifica paginación"""
    # Crear 25 productos
    for i in range(25):
        product_repository.save(ProductModel(name=f"Product {i}", price=10, stock=5))

    # Primera página (10 items)
    page1 = product_repository.find_all(skip=0, limit=10)
    assert len(page1) == 10

    # Segunda página (10 items)
    page2 = product_repository.find_all(skip=10, limit=10)
    assert len(page2) == 10

    # Tercera página (5 items restantes)
    page3 = product_repository.find_all(skip=20, limit=10)
    assert len(page3) == 5

def test_repository_find_not_found(product_repository):
    """Verifica error cuando no existe el registro"""
    with pytest.raises(InstanceNotFoundError):
        product_repository.find(999)
```

**Cobertura**: ~90%

### 3. Pruebas de Servicios (test_services.py)

**Objetivo**: Verificar lógica de negocio y validaciones críticas.

**Pruebas Críticas de Negocio**:

```python
def test_save_order_detail_insufficient_stock(seeded_db):
    """CRÍTICO: Prevenir sobreventa"""
    service = OrderDetailService(seeded_db)

    # Producto tiene stock = 10
    # Intentar vender 15 (más de lo disponible)
    schema = OrderDetailSchema(
        quantity=15,
        price=999.99,
        order_id=1,
        product_id=1
    )

    with pytest.raises(HTTPException) as exc:
        service.save(schema)

    assert exc.value.status_code == 400
    assert "Insufficient stock" in str(exc.value.detail)

def test_save_order_detail_price_mismatch(seeded_db):
    """CRÍTICO: Prevenir fraude de precios"""
    service = OrderDetailService(seeded_db)

    # Precio real del producto: 999.99
    # Intentar con precio falso: 1.00
    schema = OrderDetailSchema(
        quantity=1,
        price=1.00,  # ❌ Precio incorrecto
        order_id=1,
        product_id=1
    )

    with pytest.raises(HTTPException) as exc:
        service.save(schema)

    assert exc.value.status_code == 400
    assert "Price mismatch" in str(exc.value.detail)

def test_delete_order_detail_restores_stock(seeded_db):
    """CRÍTICO: Reintegrar stock al cancelar"""
    service = OrderDetailService(seeded_db)

    # Crear detalle (stock inicial = 10)
    schema = OrderDetailSchema(
        quantity=5,
        price=999.99,
        order_id=1,
        product_id=1
    )
    detail = service.save(schema)

    # Stock ahora debería ser 5
    product = seeded_db.query(ProductModel).get(1)
    assert product.stock == 5

    # Eliminar detalle (cancelar venta)
    service.delete(detail.id_key)

    # Stock debe regresar a 10
    seeded_db.refresh(product)
    assert product.stock == 10
```

**Cobertura**: ~85% (100% en rutas críticas)

### 4. Pruebas de Controladores (test_controllers.py)

**Objetivo**: Verificar endpoints HTTP y códigos de estado.

**Ejemplos**:

```python
def test_create_product_success(api_client, seeded_db):
    """Verifica creación de producto"""
    response = api_client.post("/products", json={
        "name": "iPhone 15 Pro",
        "price": 999.99,
        "stock": 50,
        "category_id": 1
    })

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "iPhone 15 Pro"
    assert data["price"] == 999.99
    assert "id_key" in data

def test_create_product_validation_error(api_client):
    """Verifica validación de campos"""
    response = api_client.post("/products", json={
        "name": "Test",
        "price": -10,  # ❌ Precio negativo
        "stock": 5
    })

    assert response.status_code == 422
    assert "validation error" in response.json()["detail"].lower()

def test_get_product_not_found(api_client):
    """Verifica 404 cuando no existe"""
    response = api_client.get("/products/999")
    assert response.status_code == 404
```

**Cobertura**: ~80%

### 5. Pruebas de Integración (test_integration.py)

**Objetivo**: Verificar flujos completos end-to-end.

**Ejemplo de Flujo Completo**:

```python
def test_complete_order_creation_flow(api_client):
    """
    Flujo completo de creación de pedido:
    1. Crear categoría
    2. Crear producto
    3. Crear cliente
    4. Crear dirección
    5. Crear factura
    6. Crear orden
    7. Agregar detalle de orden (verifica stock)
    8. Crear reseña
    9. Actualizar estado de orden
    """

    # 1. Categoría
    cat_resp = api_client.post("/categories", json={"name": "Electronics"})
    assert cat_resp.status_code == 201
    category_id = cat_resp.json()["id_key"]

    # 2. Producto
    prod_resp = api_client.post("/products", json={
        "name": "Laptop",
        "price": 1299.99,
        "stock": 10,
        "category_id": category_id
    })
    assert prod_resp.status_code == 201
    product_id = prod_resp.json()["id_key"]

    # 3. Cliente
    client_resp = api_client.post("/clients", json={
        "name": "John",
        "lastname": "Doe",
        "email": "john@example.com",
        "telephone": "+525512345678"
    })
    assert client_resp.status_code == 201
    client_id = client_resp.json()["id_key"]

    # 4-9: ... (continúa el flujo)
```

**Cobertura**: Flujos completos del sistema

---

## 🎭 Fixtures y Datos de Prueba

### Fixtures Principales (conftest.py)

```python
@pytest.fixture
def db_session():
    """Base de datos SQLite en memoria (aislada por prueba)"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

@pytest.fixture
def seeded_db(db_session):
    """
    Base de datos pre-poblada con:
    - 1 Categoría (Electronics)
    - 1 Producto (Laptop, $999.99, stock: 10)
    - 1 Cliente (John Doe)
    - 1 Dirección (123 Main St)
    - 1 Factura (BILL-001)
    - 1 Orden (PENDING)
    - 1 OrderDetail (quantity: 1)
    - 1 Reseña (5 estrellas)
    """
    # ... datos pre-cargados
    return db_session

@pytest.fixture
def api_client(test_app):
    """Cliente HTTP para pruebas de endpoints"""
    return TestClient(test_app)

@pytest.fixture
def mock_redis():
    """Mock de Redis para pruebas de middleware"""
    return MagicMock()
```

---

## 📊 Cobertura de Código

### Generar Reporte de Cobertura

```bash
# Generar reporte HTML
pytest tests/ --cov=. --cov-report=html --cov-report=term

# Ver en navegador
start htmlcov/index.html  # Windows
open htmlcov/index.html   # Mac/Linux
```

### Configuración de Cobertura (.coveragerc)

```ini
[run]
source = .
omit =
    tests/*
    venv/*
    main.py
    run_production.py

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise AssertionError
    raise NotImplementedError
    if __name__ == .__main__.:
```

### Objetivos de Cobertura

- **Modelos**: >= 95%
- **Repositories**: >= 90%
- **Services**: >= 85%
- **Controllers**: >= 80%
- **General**: >= 80%

---

## ✍️ Escribir Nuevas Pruebas

### Template para Prueba de Servicio

```python
def test_my_new_feature(seeded_db):
    """Descripción clara de qué se prueba"""
    # Arrange (Preparar)
    service = MyService(seeded_db)
    schema = MySchema(field1="value1", field2="value2")

    # Act (Ejecutar)
    result = service.my_method(schema)

    # Assert (Verificar)
    assert result.field1 == "value1"
    assert result.id_key is not None
```

### Template para Prueba de Controller

```python
def test_my_endpoint(api_client, seeded_db):
    """Descripción del endpoint a probar"""
    # Act
    response = api_client.post("/my_endpoint", json={
        "field1": "value1",
        "field2": "value2"
    })

    # Assert
    assert response.status_code == 201
    data = response.json()
    assert data["field1"] == "value1"
```

### Mejores Prácticas

1. **Nombres Descriptivos**: `test_create_product_with_invalid_price` en lugar de `test1`
2. **AAA Pattern**: Arrange, Act, Assert
3. **Un Concepto por Prueba**: No mezclar múltiples validaciones
4. **Independencia**: Cada prueba debe funcionar sola
5. **Cleanup Automático**: Usar fixtures con yield
6. **Datos Realistas**: Usar datos que representen casos reales

---

## 📈 Matriz de Trazabilidad

| Historia Usuario | Prueba | Archivo | Línea |
|-------------------|--------|---------|-------|
| HU-C01 | test_save_client | test_services.py | 45 |
| HU-P01 | test_get_all_products | test_controllers.py | 120 |
| HU-O02 | test_save_order_detail_insufficient_stock | test_services.py | 312 |
| HU-O04 | test_order_cancellation_restores_stock | test_integration.py | 85 |
| HU-A01 | test_health_check_healthy | test_controllers.py | 450 |

Ver `docs/HISTORIAS_USUARIO.md` para matriz completa.

---

## 🎯 Checklist de Testing

### Antes de Commit

- [ ] Todas las pruebas pasan (`pytest tests/ -v`)
- [ ] Cobertura >= 80% (`pytest tests/ --cov=.`)
- [ ] No hay warnings de pytest
- [ ] Nuevas features tienen pruebas
- [ ] Pruebas críticas de negocio pasan

### Antes de PR/Merge

- [ ] CI/CD pasa todas las pruebas
- [ ] Pruebas de integración pasan
- [ ] Documentación actualizada
- [ ] Código revisado (code review)

---

**¡Sistema de pruebas robusto y completo!** ✅

**Documento actualizado**: 2025-11-18
**Versión**: 2.0
**Mantenedor**: Equipo de QA