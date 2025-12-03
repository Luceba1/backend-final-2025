"""Integration tests for end-to-end workflows."""
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


class TestCompleteOrderWorkflow:
    """Integration tests for complete order creation workflow."""

    def test_complete_order_creation_flow(self, api_client):
        """Test complete flow: Category -> Product -> Client -> Bill -> Order -> OrderDetail."""

        # Step 1: Create Category
        category_payload = {"name": "Electronics"}
        category_response = api_client.post("/categories/", json=category_payload)
        assert category_response.status_code == 201
        category_id = category_response.json()["id_key"]

        # Step 2: Create Product
        product_payload = {
            "name": "Gaming Laptop",
            "price": 1499.99,
            "stock": 10,
            "category_id": category_id
        }
        product_response = api_client.post("/products/", json=product_payload)
        assert product_response.status_code == 201
        product_id = product_response.json()["id_key"]

        # Step 3: Create Client
        client_payload = {
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "telephone": "+1234567890",
            "age": 28
        }
        client_response = api_client.post("/clients/", json=client_payload)
        assert client_response.status_code == 201
        client_id = client_response.json()["id_key"]

        # Step 4: Create Address for Client
        address_payload = {
            "street": "123 Tech St",
            "city": "San Francisco",
            "postal_code": "94102",
            "country": "USA",
            "client_id": client_id
        }
        address_response = api_client.post("/addresses/", json=address_payload)
        assert address_response.status_code == 201

        # Step 5: Create Bill
        bill_payload = {
            "bill_number": "BILL-INT-001",
            "discount": 100.0,
            "date": date.today().isoformat(),
            "total": 1399.99,
            "payment_type": PaymentType.CARD.value
        }
        bill_response = api_client.post("/bills/", json=bill_payload)
        assert bill_response.status_code == 201
        bill_id = bill_response.json()["id_key"]

        # Step 6: Create Order
        order_payload = {
            "date": datetime.utcnow().isoformat(),
            "total": 1399.99,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": client_id,
            "bill_id": bill_id
        }
        order_response = api_client.post("/orders/", json=order_payload)
        assert order_response.status_code == 201
        order_id = order_response.json()["id_key"]

        # Step 7: Add Product to Order (OrderDetail)
        order_detail_payload = {
            "quantity": 1,
            "price": 1499.99,
            "order_id": order_id,
            "product_id": product_id
        }
        order_detail_response = api_client.post("/order-details/", json=order_detail_payload)
        assert order_detail_response.status_code == 201

        # Step 8: Verify stock was deducted
        product_check = api_client.get(f"/products/{product_id}")
        assert product_check.json()["stock"] == 9

        # Step 9: Add Review
        review_payload = {
            "rating": 5,
            "comment": "Excellent gaming laptop!",
            "product_id": product_id,
            "client_id": client_id
        }
        review_response = api_client.post("/reviews/", json=review_payload)
        assert review_response.status_code == 201

        # Step 10: Update Order Status
        order_update_payload = {
            "id_key": order_id,
            "date": order_response.json()["date"],
            "total": 1399.99,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.DELIVERED.value,
            "client_id": client_id,
            "bill_id": bill_id
        }
        update_response = api_client.put(f"/orders/{order_id}", json=order_update_payload)
        assert update_response.status_code == 200
        assert update_response.json()["status"] == Status.DELIVERED.value


class TestMultipleProductsOrder:
    """Integration tests for orders with multiple products."""

    def test_order_with_multiple_products(self, api_client):
        """Test creating an order with multiple different products."""

        # Create Category
        category = api_client.post("/categories/", json={"name": "Office Supplies"}).json()

        # Create Multiple Products
        products = []
        for i in range(3):
            product_payload = {
                "name": f"Product {i}",
                "price": 100.0 * (i + 1),
                "stock": 20,
                "category_id": category["id_key"]
            }
            product = api_client.post("/products/", json=product_payload).json()
            products.append(product)

        # Create Client
        client = api_client.post("/clients/", json={
            "name": "Bob Smith",
            "email": "bob@example.com",
            "telephone": "+1111111111",
            "age": 35
        }).json()

        # Create Bill
        bill = api_client.post("/bills/", json={
            "bill_number": "BILL-MULTI-001",
            "date": date.today().isoformat(),
            "total": 600.0,
            "payment_type": PaymentType.CASH.value
        }).json()

        # Create Order
        order = api_client.post("/orders/", json={
            "date": datetime.utcnow().isoformat(),
            "total": 600.0,
            "delivery_method": DeliveryMethod.ON_HAND.value,
            "status": Status.PENDING.value,
            "client_id": client["id_key"],
            "bill_id": bill["id_key"]
        }).json()

        # Add all products to order
        for product in products:
            order_detail_response = api_client.post("/order-details/", json={
                "quantity": 2,
                "price": product["price"],
                "order_id": order["id_key"],
                "product_id": product["id_key"]
            })
            assert order_detail_response.status_code == 201

        # Verify all products have updated stock
        for product in products:
            updated_product = api_client.get(f"/products/{product['id_key']}").json()
            assert updated_product["stock"] == 18  # 20 - 2


class TestStockManagementScenarios:
    """Integration tests for stock management."""

    def test_stock_depletion_prevents_overselling(self, api_client):
        """Test that orders cannot exceed available stock."""

        # Create Category and Product with limited stock
        category = api_client.post("/categories/", json={"name": "Limited Stock"}).json()
        product = api_client.post("/products/", json={
            "name": "Limited Item",
            "price": 50.0,
            "stock": 5,
            "category_id": category["id_key"]
        }).json()

        # Create Client and Bill
        client = api_client.post("/clients/", json={
            "name": "Customer",
            "email": "customer@example.com",
            "telephone": "+2222222222",
            "age": 30
        }).json()

        bill1 = api_client.post("/bills/", json={
            "bill_number": "BILL-STOCK-001",
            "date": date.today().isoformat(),
            "total": 150.0,
            "payment_type": PaymentType.CASH.value
        }).json()

        # Create Order
        order1 = api_client.post("/orders/", json={
            "date": datetime.utcnow().isoformat(),
            "total": 150.0,
            "delivery_method": DeliveryMethod.DRIVE_THRU.value,
            "status": Status.PENDING.value,
            "client_id": client["id_key"],
            "bill_id": bill1["id_key"]
        }).json()

        # Order 3 items (should succeed)
        response1 = api_client.post("/order-details/", json={
            "quantity": 3,
            "price": product["price"],
            "order_id": order1["id_key"],
            "product_id": product["id_key"]
        })
        assert response1.status_code == 201

        # Verify stock is now 2
        updated_product = api_client.get(f"/products/{product['id_key']}").json()
        assert updated_product["stock"] == 2

        # Try to order 3 more (should fail - only 2 left)
        bill2 = api_client.post("/bills/", json={
            "bill_number": "BILL-STOCK-002",
            "date": date.today().isoformat(),
            "total": 150.0,
            "payment_type": PaymentType.CASH.value
        }).json()

        order2 = api_client.post("/orders/", json={
            "date": datetime.utcnow().isoformat(),
            "total": 150.0,
            "delivery_method": DeliveryMethod.DRIVE_THRU.value,
            "status": Status.PENDING.value,
            "client_id": client["id_key"],
            "bill_id": bill2["id_key"]
        }).json()

        response2 = api_client.post("/order-details/", json={
            "quantity": 3,
            "price": product["price"],
            "order_id": order2["id_key"],
            "product_id": product["id_key"]
        })
        assert response2.status_code in [400, 422, 500]  # Should fail

    def test_order_cancellation_restores_stock(self, api_client):
        """Test that deleting order details restores stock."""

        # Setup: Category, Product, Client, Bill, Order
        category = api_client.post("/categories/", json={"name": "Cancellable"}).json()
        product = api_client.post("/products/", json={
            "name": "Refundable Item",
            "price": 75.0,
            "stock": 10,
            "category_id": category["id_key"]
        }).json()

        client = api_client.post("/clients/", json={
            "name": "Refund Customer",
            "email": "refund@example.com",
            "telephone": "+3333333333",
            "age": 40
        }).json()

        bill = api_client.post("/bills/", json={
            "bill_number": "BILL-CANCEL-001",
            "date": date.today().isoformat(),
            "total": 150.0,
            "payment_type": PaymentType.CARD.value
        }).json()

        order = api_client.post("/orders/", json={
            "date": datetime.utcnow().isoformat(),
            "total": 150.0,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": client["id_key"],
            "bill_id": bill["id_key"]
        }).json()

        # Create order detail (deducts 2 from stock)
        order_detail = api_client.post("/order-details/", json={
            "quantity": 2,
            "price": product["price"],
            "order_id": order["id_key"],
            "product_id": product["id_key"]
        }).json()

        # Verify stock is 8
        stock_after_order = api_client.get(f"/products/{product['id_key']}").json()["stock"]
        assert stock_after_order == 8

        # Cancel order (delete order detail)
        delete_response = api_client.delete(f"/order-details/{order_detail['id_key']}")
        assert delete_response.status_code == 204

        # Verify stock is restored to 10
        stock_after_cancel = api_client.get(f"/products/{product['id_key']}").json()["stock"]
        assert stock_after_cancel == 10


class TestClientOrderHistory:
    """Integration tests for client order history."""

    def test_client_multiple_orders(self, api_client):
        """Test client placing multiple orders."""

        # Create Category and Products
        category = api_client.post("/categories/", json={"name": "Gadgets"}).json()
        product1 = api_client.post("/products/", json={
            "name": "Phone",
            "price": 699.99,
            "stock": 50,
            "category_id": category["id_key"]
        }).json()

        product2 = api_client.post("/products/", json={
            "name": "Tablet",
            "price": 499.99,
            "stock": 30,
            "category_id": category["id_key"]
        }).json()

        # Create Client
        client = api_client.post("/clients/", json={
            "name": "Frequent Buyer",
            "email": "frequent@example.com",
            "telephone": "+4444444444",
            "age": 32
        }).json()

        # Create multiple bills and orders
        for i in range(2):
            bill = api_client.post("/bills/", json={
                "bill_number": f"BILL-FREQ-{i:03d}",
                "date": date.today().isoformat(),
                "total": 699.99,
                "payment_type": PaymentType.CARD.value
            }).json()

            order = api_client.post("/orders/", json={
                "date": datetime.utcnow().isoformat(),
                "total": 699.99,
                "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
                "status": Status.PENDING.value,
                "client_id": client["id_key"],
                "bill_id": bill["id_key"]
            }).json()

            # Add product to each order
            product = product1 if i == 0 else product2
            api_client.post("/order-details/", json={
                "quantity": 1,
                "price": product["price"],
                "order_id": order["id_key"],
                "product_id": product["id_key"]
            })

        # Verify client exists and orders were created
        client_check = api_client.get(f"/clients/{client['id_key']}")
        assert client_check.status_code == 200


class TestProductReviewWorkflow:
    """Integration tests for product review workflow."""

    def test_multiple_reviews_for_product(self, api_client):
        """Test multiple clients reviewing the same product."""

        # Create Category and Product
        category = api_client.post("/categories/", json={"name": "Reviewed Items"}).json()
        product = api_client.post("/products/", json={
            "name": "Popular Product",
            "price": 299.99,
            "stock": 100,
            "category_id": category["id_key"]
        }).json()

        # Create multiple clients and reviews
        ratings = [5, 4, 3, 5, 4]
        for i, rating in enumerate(ratings):
            client = api_client.post("/clients/", json={
                "name": f"Reviewer {i}",
                "email": f"reviewer{i}@example.com",
                "telephone": f"+{5555555550 + i}",
                "age": 25 + i
            }).json()

            review_response = api_client.post("/reviews/", json={
                "rating": rating,
                "comment": f"Review comment {i}",
                "product_id": product["id_key"],
                "client_id": client["id_key"]
            })
            assert review_response.status_code == 201

        # Verify product exists
        product_check = api_client.get(f"/products/{product['id_key']}")
        assert product_check.status_code == 200


class TestErrorHandling:
    """Integration tests for error handling."""

    def test_invalid_foreign_key_references(self, api_client):
        """Test proper error handling for invalid FK references."""

        # Try to create order with non-existent client and bill
        order_payload = {
            "date": datetime.utcnow().isoformat(),
            "total": 100.0,
            "delivery_method": DeliveryMethod.HOME_DELIVERY.value,
            "status": Status.PENDING.value,
            "client_id": 9999,
            "bill_id": 9999
        }
        response = api_client.post("/orders/", json=order_payload)
        assert response.status_code == 404

    def test_price_validation_in_order_detail(self, api_client, seeded_db):
        """Test price validation in order detail creation."""
        product = seeded_db["product"]
        order = seeded_db["order"]

        # Try to create order detail with wrong price
        order_detail_payload = {
            "quantity": 1,
            "price": 1.0,  # Wrong price
            "order_id": order.id_key,
            "product_id": product.id_key
        }
        response = api_client.post("/order-details/", json=order_detail_payload)

        # Should fail with validation error
        assert response.status_code in [400, 422, 500]
