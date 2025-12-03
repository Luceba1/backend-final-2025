"""Unit tests for repository layer."""
import pytest
from datetime import datetime, date

from repositories.base_repository_impl import InstanceNotFoundError
from repositories.category_repository import CategoryRepository
from repositories.product_repository import ProductRepository
from repositories.client_repository import ClientRepository
from repositories.address_repository import AddressRepository
from repositories.bill_repository import BillRepository
from repositories.order_repository import OrderRepository
from repositories.order_detail_repository import OrderDetailRepository
from repositories.review_repository import ReviewRepository

from models.category import CategoryModel
from models.product import ProductModel
from models.client import ClientModel
from models.address import AddressModel
from models.bill import BillModel
from models.order import OrderModel
from models.order_detail import OrderDetailModel
from models.review import ReviewModel
from models.enums import DeliveryMethod, Status, PaymentType

from schemas.category_schema import CategorySchema
from schemas.product_schema import ProductSchema
from schemas.client_schema import ClientSchema
from schemas.address_schema import AddressSchema
from schemas.bill_schema import BillSchema
from schemas.order_schema import OrderSchema
from schemas.order_detail_schema import OrderDetailSchema
from schemas.review_schema import ReviewSchema


class TestCategoryRepository:
    """Tests for CategoryRepository."""

    def test_save_category(self, db_session):
        """Test saving a category."""
        repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")

        result = repo.save(category)

        assert result.id_key is not None
        assert result.name == "Electronics"

    def test_find_category(self, db_session):
        """Test finding a category by ID."""
        repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")
        saved = repo.save(category)

        result = repo.find(saved.id_key)

        assert result.id_key == saved.id_key
        assert result.name == "Electronics"

    def test_find_category_not_found(self, db_session):
        """Test finding a non-existent category."""
        repo = CategoryRepository(db_session)

        with pytest.raises(InstanceNotFoundError):
            repo.find(999)

    def test_find_all_categories(self, db_session):
        """Test finding all categories."""
        repo = CategoryRepository(db_session)
        repo.save(CategoryModel(name="Electronics"))
        repo.save(CategoryModel(name="Books"))

        result = repo.find_all()

        assert len(result) == 2

    def test_update_category(self, db_session):
        """Test updating a category."""
        repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")
        saved = repo.save(category)

        result = repo.update(saved.id_key, {"name": "Updated Electronics"})

        assert result.name == "Updated Electronics"

    def test_update_category_not_found(self, db_session):
        """Test updating a non-existent category."""
        repo = CategoryRepository(db_session)

        with pytest.raises(InstanceNotFoundError):
            repo.update(999, {"name": "Test"})

    def test_remove_category(self, db_session):
        """Test removing a category."""
        repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")
        saved = repo.save(category)

        repo.remove(saved.id_key)

        with pytest.raises(InstanceNotFoundError):
            repo.find(saved.id_key)

    def test_remove_category_not_found(self, db_session):
        """Test removing a non-existent category."""
        repo = CategoryRepository(db_session)

        with pytest.raises(InstanceNotFoundError):
            repo.remove(999)


class TestProductRepository:
    """Tests for ProductRepository."""

    def test_save_product(self, db_session):
        """Test saving a product."""
        category_repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")
        saved_category = category_repo.save(category)

        product_repo = ProductRepository(db_session)
        product = ProductModel(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=saved_category.id_key
        )
        result = product_repo.save(product)

        assert result.id_key is not None
        assert result.name == "Laptop"
        assert result.price == 999.99
        assert result.stock == 10

    def test_find_product(self, db_session, seeded_db):
        """Test finding a product by ID."""
        product_repo = ProductRepository(db_session)
        product = seeded_db["product"]

        result = product_repo.find(product.id_key)

        assert result.id_key == product.id_key
        assert result.name == "Laptop"

    def test_find_all_products_with_pagination(self, db_session, seeded_db):
        """Test finding all products with pagination."""
        product_repo = ProductRepository(db_session)

        # Add more products
        category = seeded_db["category"]
        for i in range(5):
            product = ProductModel(
                name=f"Product {i}",
                price=100.0 + i,
                stock=i,
                category_id=category.id_key
            )
            product_repo.save(product)

        # Test pagination
        result_page1 = product_repo.find_all(skip=0, limit=3)
        result_page2 = product_repo.find_all(skip=3, limit=3)

        assert len(result_page1) == 3
        assert len(result_page2) == 3

    def test_update_product_stock(self, db_session, seeded_db):
        """Test updating product stock."""
        product_repo = ProductRepository(db_session)
        product = seeded_db["product"]

        result = product_repo.update(product.id_key, {"stock": 20})

        assert result.stock == 20

    def test_save_all_products(self, db_session):
        """Test saving multiple products."""
        category_repo = CategoryRepository(db_session)
        category = CategoryModel(name="Electronics")
        saved_category = category_repo.save(category)

        product_repo = ProductRepository(db_session)
        products = [
            ProductModel(name=f"Product {i}", price=100.0 + i, stock=i, category_id=saved_category.id_key)
            for i in range(3)
        ]

        result = product_repo.save_all(products)

        assert len(result) == 3
        for i, product in enumerate(result):
            assert product.name == f"Product {i}"


class TestClientRepository:
    """Tests for ClientRepository."""

    def test_save_client(self, db_session):
        """Test saving a client."""
        repo = ClientRepository(db_session)
        client = ClientModel(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )

        result = repo.save(client)

        assert result.id_key is not None
        assert result.name == "John Doe"
        assert result.email == "john@example.com"

    def test_find_client(self, db_session, seeded_db):
        """Test finding a client by ID."""
        repo = ClientRepository(db_session)
        client = seeded_db["client"]

        result = repo.find(client.id_key)

        assert result.id_key == client.id_key
        assert result.name == "John Doe"

    def test_update_client(self, db_session, seeded_db):
        """Test updating a client."""
        repo = ClientRepository(db_session)
        client = seeded_db["client"]

        result = repo.update(client.id_key, {"age": 35})

        assert result.age == 35


class TestAddressRepository:
    """Tests for AddressRepository."""

    def test_save_address(self, db_session, seeded_db):
        """Test saving an address."""
        repo = AddressRepository(db_session)
        client = seeded_db["client"]

        address = AddressModel(
            street="456 Elm St",
            city="Los Angeles",
            postal_code="90001",
            country="USA",
            client_id=client.id_key
        )

        result = repo.save(address)

        assert result.id_key is not None
        assert result.street == "456 Elm St"

    def test_find_address(self, db_session, seeded_db):
        """Test finding an address by ID."""
        repo = AddressRepository(db_session)
        address = seeded_db["address"]

        result = repo.find(address.id_key)

        assert result.id_key == address.id_key
        assert result.street == "123 Main St"


class TestBillRepository:
    """Tests for BillRepository."""

    def test_save_bill(self, db_session):
        """Test saving a bill."""
        repo = BillRepository(db_session)
        bill = BillModel(
            bill_number="BILL-002",
            discount=5.0,
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CARD
        )

        result = repo.save(bill)

        assert result.id_key is not None
        assert result.bill_number == "BILL-002"

    def test_find_bill(self, db_session, seeded_db):
        """Test finding a bill by ID."""
        repo = BillRepository(db_session)
        bill = seeded_db["bill"]

        result = repo.find(bill.id_key)

        assert result.id_key == bill.id_key
        assert result.bill_number == "BILL-001"

    def test_update_bill_total(self, db_session, seeded_db):
        """Test updating bill total."""
        repo = BillRepository(db_session)
        bill = seeded_db["bill"]

        result = repo.update(bill.id_key, {"total": 1000.0})

        assert result.total == 1000.0


class TestOrderRepository:
    """Tests for OrderRepository."""

    def test_save_order(self, db_session, seeded_db):
        """Test saving an order."""
        repo = OrderRepository(db_session)
        client = seeded_db["client"]
        bill = seeded_db["bill"]

        order = OrderModel(
            date=datetime.utcnow(),
            total=500.0,
            delivery_method=DeliveryMethod.HOME_DELIVERY,
            status=Status.IN_PROGRESS,
            client_id=client.id_key,
            bill_id=bill.id_key
        )

        result = repo.save(order)

        assert result.id_key is not None
        assert result.total == 500.0
        assert result.delivery_method == DeliveryMethod.HOME_DELIVERY

    def test_find_order(self, db_session, seeded_db):
        """Test finding an order by ID."""
        repo = OrderRepository(db_session)
        order = seeded_db["order"]

        result = repo.find(order.id_key)

        assert result.id_key == order.id_key
        assert result.total == 989.99

    def test_update_order_status(self, db_session, seeded_db):
        """Test updating order status."""
        repo = OrderRepository(db_session)
        order = seeded_db["order"]

        result = repo.update(order.id_key, {"status": Status.DELIVERED})

        assert result.status == Status.DELIVERED


class TestOrderDetailRepository:
    """Tests for OrderDetailRepository."""

    def test_save_order_detail(self, db_session, seeded_db):
        """Test saving an order detail."""
        repo = OrderDetailRepository(db_session)
        order = seeded_db["order"]
        product = seeded_db["product"]

        order_detail = OrderDetailModel(
            quantity=2,
            price=999.99,
            order_id=order.id_key,
            product_id=product.id_key
        )

        result = repo.save(order_detail)

        assert result.id_key is not None
        assert result.quantity == 2

    def test_find_order_detail(self, db_session, seeded_db):
        """Test finding an order detail by ID."""
        repo = OrderDetailRepository(db_session)
        order_detail = seeded_db["order_detail"]

        result = repo.find(order_detail.id_key)

        assert result.id_key == order_detail.id_key
        assert result.quantity == 1

    def test_update_order_detail_quantity(self, db_session, seeded_db):
        """Test updating order detail quantity."""
        repo = OrderDetailRepository(db_session)
        order_detail = seeded_db["order_detail"]

        result = repo.update(order_detail.id_key, {"quantity": 3})

        assert result.quantity == 3


class TestReviewRepository:
    """Tests for ReviewRepository."""

    def test_save_review(self, db_session, seeded_db):
        """Test saving a review."""
        repo = ReviewRepository(db_session)
        product = seeded_db["product"]
        client = seeded_db["client"]

        review = ReviewModel(
            rating=4,
            comment="Good product",
            product_id=product.id_key,
            client_id=client.id_key
        )

        result = repo.save(review)

        assert result.id_key is not None
        assert result.rating == 4

    def test_find_review(self, db_session, seeded_db):
        """Test finding a review by ID."""
        repo = ReviewRepository(db_session)
        review = seeded_db["review"]

        result = repo.find(review.id_key)

        assert result.id_key == review.id_key
        assert result.rating == 5

    def test_update_review_rating(self, db_session, seeded_db):
        """Test updating review rating."""
        repo = ReviewRepository(db_session)
        review = seeded_db["review"]

        result = repo.update(review.id_key, {"rating": 3, "comment": "Average"})

        assert result.rating == 3
        assert result.comment == "Average"

    def test_remove_review(self, db_session, seeded_db):
        """Test removing a review."""
        repo = ReviewRepository(db_session)
        review = seeded_db["review"]

        repo.remove(review.id_key)

        with pytest.raises(InstanceNotFoundError):
            repo.find(review.id_key)
