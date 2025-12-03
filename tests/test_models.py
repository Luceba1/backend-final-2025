"""Unit tests for SQLAlchemy models."""
import pytest
from datetime import datetime, date
from sqlalchemy.exc import IntegrityError

from models.category import CategoryModel as Category
from models.product import ProductModel as Product
from models.client import ClientModel as Client
from models.address import AddressModel as Address
from models.bill import BillModel as Bill
from models.order import OrderModel as Order
from models.order_detail import OrderDetailModel as OrderDetail
from models.review import ReviewModel as Review
from models.enums import DeliveryMethod, Status, PaymentType


class TestCategoryModel:
    """Tests for Category model."""

    def test_create_category(self, db_session):
        """Test creating a category."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.commit()

        assert category.id_key is not None
        assert category.name == "Electronics"

    def test_category_name_required(self, db_session):
        """Test that category name is required."""
        category = Category()
        db_session.add(category)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_category_products_relationship(self, db_session):
        """Test category to products relationship."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        assert len(category.products) == 1
        assert category.products[0].name == "Laptop"


class TestProductModel:
    """Tests for Product model."""

    def test_create_product(self, db_session):
        """Test creating a product."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        assert product.id_key is not None
        assert product.name == "Laptop"
        assert product.price == 999.99
        assert product.stock == 10
        assert product.category_id == category.id_key

    def test_product_required_fields(self, db_session):
        """Test that required fields are enforced."""
        product = Product()
        db_session.add(product)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_product_default_stock(self, db_session):
        """Test default stock value."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        assert product.stock == 0

    def test_product_category_relationship(self, db_session):
        """Test product to category relationship."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        assert product.category is not None
        assert product.category.name == "Electronics"

    def test_product_reviews_relationship(self, db_session):
        """Test product to reviews relationship."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.flush()

        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        review = Review(
            rating=5,
            comment="Great!",
            product_id=product.id_key,
            client_id=client.id_key
        )
        db_session.add(review)
        db_session.commit()

        assert len(product.reviews) == 1
        assert product.reviews[0].rating == 5


class TestClientModel:
    """Tests for Client model."""

    def test_create_client(self, db_session):
        """Test creating a client."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.commit()

        assert client.id_key is not None
        assert client.name == "John Doe"
        assert client.email == "john@example.com"
        assert client.telephone == "+1234567890"
        assert client.age == 30

    def test_client_required_fields(self, db_session):
        """Test that required fields are enforced."""
        client = Client()
        db_session.add(client)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_client_addresses_relationship(self, db_session):
        """Test client to addresses relationship."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        address = Address(
            street="123 Main St",
            city="New York",
            postal_code="10001",
            country="USA",
            client_id=client.id_key
        )
        db_session.add(address)
        db_session.commit()

        assert len(client.addresses) == 1
        assert client.addresses[0].street == "123 Main St"

    def test_client_orders_relationship(self, db_session):
        """Test client to orders relationship."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=100.0,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            status=Status.PENDING,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        assert len(client.orders) == 1
        assert client.orders[0].total == 100.0


class TestAddressModel:
    """Tests for Address model."""

    def test_create_address(self, db_session):
        """Test creating an address."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        address = Address(
            street="123 Main St",
            city="New York",
            postal_code="10001",
            country="USA",
            client_id=client.id_key
        )
        db_session.add(address)
        db_session.commit()

        assert address.id_key is not None
        assert address.street == "123 Main St"
        assert address.city == "New York"
        assert address.postal_code == "10001"
        assert address.country == "USA"
        assert address.client_id == client.id_key

    def test_address_client_relationship(self, db_session):
        """Test address to client relationship."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        address = Address(
            street="123 Main St",
            city="New York",
            postal_code="10001",
            country="USA",
            client_id=client.id_key
        )
        db_session.add(address)
        db_session.commit()

        assert address.client is not None
        assert address.client.name == "John Doe"


class TestBillModel:
    """Tests for Bill model."""

    def test_create_bill(self, db_session):
        """Test creating a bill."""
        bill = Bill(
            bill_number="BILL-001",
            discount=10.0,
            date=date.today(),
            total=990.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.commit()

        assert bill.id_key is not None
        assert bill.bill_number == "BILL-001"
        assert bill.discount == 10.0
        assert bill.total == 990.0
        assert bill.payment_type == PaymentType.CASH

    def test_bill_required_fields(self, db_session):
        """Test that required fields are enforced."""
        bill = Bill()
        db_session.add(bill)
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_bill_unique_bill_number(self, db_session):
        """Test that bill_number is unique."""
        bill1 = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill1)
        db_session.commit()

        bill2 = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=200.0,
            payment_type=PaymentType.CARD
        )
        db_session.add(bill2)
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestOrderModel:
    """Tests for Order model."""

    def test_create_order(self, db_session):
        """Test creating an order."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=100.0,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            status=Status.PENDING,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        assert order.id_key is not None
        assert order.total == 100.0
        assert order.delivery_method == DeliveryMethod.DRIVE_THRU
        assert order.status == Status.PENDING

    def test_order_client_relationship(self, db_session):
        """Test order to client relationship."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=100.0,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            status=Status.PENDING,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        assert order.client is not None
        assert order.client.name == "John Doe"

    def test_order_bill_relationship(self, db_session):
        """Test order to bill relationship."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=100.0,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            status=Status.PENDING,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        assert order.bill is not None
        assert order.bill.bill_number == "BILL-001"

    def test_order_default_status(self, db_session):
        """Test default order status."""
        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=100.0,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=100.0,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        assert order.status == Status.PENDING


class TestOrderDetailModel:
    """Tests for OrderDetail model."""

    def test_create_order_detail(self, db_session):
        """Test creating an order detail."""
        # Create dependencies
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.flush()

        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        bill = Bill(
            bill_number="BILL-001",
            date=date.today(),
            total=999.99,
            payment_type=PaymentType.CASH
        )
        db_session.add(bill)
        db_session.flush()

        order = Order(
            date=datetime.utcnow(),
            total=999.99,
            delivery_method=DeliveryMethod.DRIVE_THRU,
            status=Status.PENDING,
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.flush()

        order_detail = OrderDetail(
            quantity=1,
            price=999.99,
            order_id=order.id_key,
            product_id=product.id_key
        )
        db_session.add(order_detail)
        db_session.commit()

        assert order_detail.id_key is not None
        assert order_detail.quantity == 1
        assert order_detail.price == 999.99

    def test_order_detail_relationships(self, db_session, seeded_db):
        """Test order detail relationships."""
        order_detail = seeded_db["order_detail"]

        assert order_detail.order is not None
        assert order_detail.product is not None
        assert order_detail.order.total == 989.99
        assert order_detail.product.name == "Laptop"


class TestReviewModel:
    """Tests for Review model."""

    def test_create_review(self, db_session):
        """Test creating a review."""
        category = Category(name="Electronics")
        db_session.add(category)
        db_session.flush()

        product = Product(
            name="Laptop",
            price=999.99,
            stock=10,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.flush()

        client = Client(
            name="John Doe",
            email="john@example.com",
            telephone="+1234567890",
            age=30
        )
        db_session.add(client)
        db_session.flush()

        review = Review(
            rating=5,
            comment="Excellent product!",
            product_id=product.id_key,
            client_id=client.id_key
        )
        db_session.add(review)
        db_session.commit()

        assert review.id_key is not None
        assert review.rating == 5
        assert review.comment == "Excellent product!"

    def test_review_relationships(self, db_session, seeded_db):
        """Test review relationships."""
        review = seeded_db["review"]

        assert review.product is not None
        assert review.client is not None
        assert review.product.name == "Laptop"
        assert review.client.name == "John Doe"


class TestEnums:
    """Tests for enum values."""

    def test_delivery_method_enum(self):
        """Test DeliveryMethod enum."""
        assert DeliveryMethod.DRIVE_THRU.value == 1
        assert DeliveryMethod.ON_HAND.value == 2
        assert DeliveryMethod.HOME_DELIVERY.value == 3

    def test_status_enum(self):
        """Test Status enum."""
        assert Status.PENDING.value == 1
        assert Status.IN_PROGRESS.value == 2
        assert Status.DELIVERED.value == 3
        assert Status.CANCELED.value == 4

    def test_payment_type_enum(self):
        """Test PaymentType enum."""
        assert PaymentType.CASH.value == "cash"
        assert PaymentType.CARD.value == "card"
