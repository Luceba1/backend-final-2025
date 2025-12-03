"""Unit tests for API controllers/endpoints."""
import pytest
from datetime import datetime, date
from fastapi.testclient import TestClient

from main import create_fastapi_app
from config.database import get_db
from models.enums import DeliveryMethod, Status, PaymentType


@pytest.fixture
def test_app(db_session):
    """Create test FastAPI app with database override."""
    app = create_fastapi_app()

    # Override get_db dependency
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    return app


@pytest.fixture
def api_client(test_app):
    """Create test client for API testing."""
    return TestClient(test_app)


class TestCategoryEndpoints:
    """Tests for Category API endpoints."""

    def test_get_all_categories(self, api_client, seeded_db):
        """Test GET /categories/."""
        response = api_client.get("/categories/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert any(c["name"] == "Electronics" for c in data)

    def test_get_category_by_id(self, api_client, seeded_db):
        """Test GET /categories/{id}."""
        category = seeded_db["category"]
        response = api_client.get(f"/categories/{category.id_key}")

        assert response.status_code == 200
        data = response.json()
        assert data["id_key"] == category.id_key
        assert data["name"] == "Electronics"

    def test_get_category_not_found(self, api_client):
        """Test GET /categories/{id} with non-existent ID."""
        response = api_client.get("/categories/9999")

        assert response.status_code == 404

    def test_create_category(self, api_client):
        """Test POST /categories/."""
        payload = {"name": "Books"}
        response = api_client.post("/categories/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Books"
        assert "id_key" in data

    def test_update_category(self, api_client, seeded_db):
        """Test PUT /categories/{id}."""
        category = seeded_db["category"]
        payload = {
            "id_key": category.id_key,
            "name": "Updated Electronics"
        }
        response = api_client.put(f"/categories/{category.id_key}", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Electronics"

    def test_delete_category(self, api_client):
        """Test DELETE /categories/{id}."""
        # Create a category to delete
        create_response = api_client.post("/categories/", json={"name": "Temporary"})
        category_id = create_response.json()["id_key"]

        response = api_client.delete(f"/categories/{category_id}")

        assert response.status_code == 204

        # Verify it's deleted
        get_response = api_client.get(f"/categories/{category_id}")
        assert get_response.status_code == 404


class TestProductEndpoints:
    """Tests for Product API endpoints."""

    def test_get_all_products(self, api_client, seeded_db):
        """Test GET /products/."""
        response = api_client.get("/products/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_get_all_products_with_pagination(self, api_client, seeded_db):
        """Test GET /products/ with pagination."""
        response = api_client.get("/products/?skip=0&limit=5")

        assert response.status_code == 200
        data = response.json()
        assert len(data) <= 5

    def test_create_product(self, api_client, seeded_db):
        """Test POST /products/."""
        category = seeded_db["category"]
        payload = {
            "name": "Mouse",
            "price": 29.99,
            "stock": 50,
            "category_id": category.id_key
        }
        response = api_client.post("/products/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Mouse"
        assert data["price"] == 29.99
        assert data["stock"] == 50

    def test_update_product(self, api_client, seeded_db):
        """Test PUT /products/{id}."""
        product = seeded_db["product"]
        payload = {
            "id_key": product.id_key,
            "name": "Updated Laptop",
            "price": 1099.99,
            "stock": 15,
            "category_id": product.category_id
        }
        response = api_client.put(f"/products/{product.id_key}", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Laptop"
        assert data["price"] == 1099.99


class TestClientEndpoints:
    """Tests for Client API endpoints."""

    def test_get_all_clients(self, api_client, seeded_db):
        """Test GET /clients/."""
        response = api_client.get("/clients/")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_create_client(self, api_client):
        """Test POST /clients/."""
        payload = {
            "name": "Jane Smith",
            "email": "jane@example.com",
            "telephone": "+9876543210",
            "age": 25
        }
        response = api_client.post("/clients/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Jane Smith"
        assert data["email"] == "jane@example.com"

    def test_update_client(self, api_client, seeded_db):
        """Test PUT /clients/{id}."""
        client = seeded_db["client"]
        payload = {
            "id_key": client.id_key,
            "name": "John Updated",
            "email": client.email,
            "telephone": client.phone,
            "age": 35
        }
        response = api_client.put(f"/clients/{client.id_key}", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["age"] == 35


class TestOrderEndpoints:
    """Tests for Order API endpoints with FK validation."""

    def test_create_order_success(self, api_client, seeded_db):
        """Test POST /orders/ with valid references."""
        client = seeded_db["client"]
        bill = seeded_db["bill"]
        payload = {
            "date": datetime.utcnow().isoformat(),
            "total": 500.0,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": client.id_key,
            "bill_id": bill.id_key
        }
        response = api_client.post("/orders/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["total"] == 500.0
        assert data["client_id"] == client.id_key

    def test_create_order_invalid_client(self, api_client, seeded_db):
        """Test POST /orders/ with invalid client ID."""
        bill = seeded_db["bill"]
        payload = {
            "date": datetime.utcnow().isoformat(),
            "total": 500.0,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": 9999,  # Non-existent
            "bill_id": bill.id_key
        }
        response = api_client.post("/orders/", json=payload)

        assert response.status_code == 404

    def test_create_order_invalid_bill(self, api_client, seeded_db):
        """Test POST /orders/ with invalid bill ID."""
        client = seeded_db["client"]
        payload = {
            "date": datetime.utcnow().isoformat(),
            "total": 500.0,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": client.id_key,
            "bill_id": 9999  # Non-existent
        }
        response = api_client.post("/orders/", json=payload)

        assert response.status_code == 404

    def test_update_order_status(self, api_client, seeded_db):
        """Test PUT /orders/{id} to update status."""
        order = seeded_db["order"]
        payload = {
            "id_key": order.id_key,
            "date": order.date.isoformat(),
            "total": order.total,
            "delivery_method": order.delivery_method.value,
            "status": Status.DELIVERED.value,
            "client_id": order.client_id,
            "bill_id": order.bill_id
        }
        response = api_client.put(f"/orders/{order.id_key}", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == Status.DELIVERED.value


class TestOrderDetailEndpoints:
    """Tests for OrderDetail API endpoints with stock management."""

    def test_create_order_detail_success(self, api_client, seeded_db):
        """Test POST /order-details/ with sufficient stock."""
        order = seeded_db["order"]
        product = seeded_db["product"]

        # Get current stock
        product_response = api_client.get(f"/products/{product.id_key}")
        initial_stock = product_response.json()["stock"]

        payload = {
            "quantity": 2,
            "price": product.price,
            "order_id": order.id_key,
            "product_id": product.id_key
        }
        response = api_client.post("/order-details/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["quantity"] == 2

        # Verify stock was deducted
        product_response = api_client.get(f"/products/{product.id_key}")
        new_stock = product_response.json()["stock"]
        assert new_stock == initial_stock - 2

    def test_create_order_detail_insufficient_stock(self, api_client, seeded_db):
        """Test POST /order-details/ with insufficient stock."""
        order = seeded_db["order"]
        product = seeded_db["product"]

        payload = {
            "quantity": 999,  # More than available
            "price": product.price,
            "order_id": order.id_key,
            "product_id": product.id_key
        }
        response = api_client.post("/order-details/", json=payload)

        assert response.status_code in [400, 422, 500]  # Should be validation error

    def test_create_order_detail_price_mismatch(self, api_client, seeded_db):
        """Test POST /order-details/ with incorrect price."""
        order = seeded_db["order"]
        product = seeded_db["product"]

        payload = {
            "quantity": 1,
            "price": 100.0,  # Wrong price
            "order_id": order.id_key,
            "product_id": product.id_key
        }
        response = api_client.post("/order-details/", json=payload)

        assert response.status_code in [400, 422, 500]  # Should be validation error

    def test_delete_order_detail_restores_stock(self, api_client, seeded_db):
        """Test DELETE /order-details/{id} restores stock."""
        order = seeded_db["order"]
        product = seeded_db["product"]

        # Create order detail
        payload = {
            "quantity": 2,
            "price": product.price,
            "order_id": order.id_key,
            "product_id": product.id_key
        }
        create_response = api_client.post("/order-details/", json=payload)
        order_detail_id = create_response.json()["id_key"]

        # Get stock after creation
        product_response = api_client.get(f"/products/{product.id_key}")
        stock_after_create = product_response.json()["stock"]

        # Delete order detail
        delete_response = api_client.delete(f"/order-details/{order_detail_id}")
        assert delete_response.status_code == 204

        # Verify stock was restored
        product_response = api_client.get(f"/products/{product.id_key}")
        stock_after_delete = product_response.json()["stock"]
        assert stock_after_delete == stock_after_create + 2


class TestBillEndpoints:
    """Tests for Bill API endpoints."""

    def test_create_bill(self, api_client):
        """Test POST /bills/."""
        payload = {
            "bill_number": "BILL-TEST-001",
            "discount": 15.0,
            "date": date.today().isoformat(),
            "total": 850.0,
            "payment_type": PaymentType.CARD.value
        }
        response = api_client.post("/bills/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["bill_number"] == "BILL-TEST-001"
        assert data["total"] == 850.0

    def test_get_bill_by_id(self, api_client, seeded_db):
        """Test GET /bills/{id}."""
        bill = seeded_db["bill"]
        response = api_client.get(f"/bills/{bill.id_key}")

        assert response.status_code == 200
        data = response.json()
        assert data["id_key"] == bill.id_key


class TestAddressEndpoints:
    """Tests for Address API endpoints."""

    def test_create_address(self, api_client, seeded_db):
        """Test POST /addresses/."""
        client = seeded_db["client"]
        payload = {
            "street": "789 Oak Ave",
            "city": "Chicago",
            "postal_code": "60601",
            "country": "USA",
            "client_id": client.id_key
        }
        response = api_client.post("/addresses/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["street"] == "789 Oak Ave"

    def test_get_address_by_id(self, api_client, seeded_db):
        """Test GET /addresses/{id}."""
        address = seeded_db["address"]
        response = api_client.get(f"/addresses/{address.id_key}")

        assert response.status_code == 200
        data = response.json()
        assert data["street"] == "123 Main St"


class TestReviewEndpoints:
    """Tests for Review API endpoints."""

    def test_create_review(self, api_client, seeded_db):
        """Test POST /reviews/."""
        product = seeded_db["product"]
        client = seeded_db["client"]
        payload = {
            "rating": 4,
            "comment": "Good value",
            "product_id": product.id_key,
            "client_id": client.id_key
        }
        response = api_client.post("/reviews/", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["rating"] == 4

    def test_update_review(self, api_client, seeded_db):
        """Test PUT /reviews/{id}."""
        review = seeded_db["review"]
        payload = {
            "id_key": review.id_key,
            "rating": 3,
            "comment": "Updated comment",
            "product_id": review.product_id,
            "client_id": review.client_id
        }
        response = api_client.put(f"/reviews/{review.id_key}", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["rating"] == 3


class TestHealthCheckEndpoint:
    """Tests for health check endpoint."""

    def test_health_check(self, api_client):
        """Test GET /health_check/."""
        response = api_client.get("/health_check/")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "checks" in data
