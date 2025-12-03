"""Pytest configuration and fixtures for testing."""
import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient
from datetime import datetime, date
from typing import Generator

# Set test environment before importing app modules
os.environ['POSTGRES_HOST'] = 'localhost'
os.environ['POSTGRES_PORT'] = '5432'
os.environ['POSTGRES_DB'] = 'test_db'
os.environ['POSTGRES_USER'] = 'postgres'
os.environ['POSTGRES_PASSWORD'] = 'postgres'
os.environ['REDIS_HOST'] = 'localhost'
os.environ['REDIS_PORT'] = '6379'

from models.base_model import base as Base
from main import create_fastapi_app


# Test database URL
TEST_DATABASE_URL = "sqlite:///:memory:"  # In-memory SQLite for fast testing


@pytest.fixture(scope="session")
def engine():
    """Create test database engine."""
    test_engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},  # SQLite specific
        echo=False
    )
    Base.metadata.create_all(bind=test_engine)
    yield test_engine
    Base.metadata.drop_all(bind=test_engine)
    test_engine.dispose()


@pytest.fixture(scope="function")
def db_session(engine) -> Generator[Session, None, None]:
    """Create a new database session for each test."""
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = TestSessionLocal()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        # Clean all tables after each test
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(table.delete())
        session.commit()


@pytest.fixture(scope="function")
def client(db_session: Session) -> Generator[TestClient, None, None]:
    """Create a test client for API testing."""
    app = create_fastapi_app()

    # Override database session dependency
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    # Note: You may need to implement dependency override in your app
    # app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client


# Model fixtures
@pytest.fixture
def sample_category_data():
    """Sample category data."""
    return {
        "name": "Electronics"
    }


@pytest.fixture
def sample_product_data():
    """Sample product data."""
    return {
        "name": "Laptop",
        "price": 999.99,
        "stock": 10,
        "category_id": 1
    }


@pytest.fixture
def sample_client_data():
    """Sample client data."""
    return {
        "name": "John",
        "lastname": "Doe",
        "email": "john.doe@example.com",
        "telephone": "+1234567890"
    }


@pytest.fixture
def sample_address_data():
    """Sample address data."""
    return {
        "street": "123 Main St",
        "city": "New York",
        "postal_code": "10001",
        "country": "USA",
        "client_id": 1
    }


@pytest.fixture
def sample_bill_data():
    """Sample bill data."""
    return {
        "bill_number": "BILL-001",
        "discount": 10.0,
        "date": date.today(),
        "total": 989.99,
        "payment_type": 1,  # Changed from "cash" to 1 (PaymentType.CASH)
        "client_id": 1  # ✅ Added - required field
    }


@pytest.fixture
def sample_order_data():
    """Sample order data."""
    return {
        "date": datetime.utcnow(),
        "total": 989.99,
        "delivery_method": 1,  # DRIVE_THRU
        "status": 1,  # PENDING
        "client_id": 1,
        "bill_id": 1
    }


@pytest.fixture
def sample_order_detail_data():
    """Sample order detail data."""
    return {
        "quantity": 2,
        "price": 999.99,
        "order_id": 1,
        "product_id": 1
    }


@pytest.fixture
def sample_review_data():
    """Sample review data."""
    return {
        "rating": 4.5,  # Changed to float in range 1.0-5.0
        "comment": "Excellent product, highly recommended!",  # Min 10 chars
        "product_id": 1  # Required field (removed client_id - not in schema)
    }


# Database seeding fixtures
@pytest.fixture
def seeded_db(db_session: Session):
    """Seed database with test data."""
    from models.category import CategoryModel
    from models.product import ProductModel
    from models.client import ClientModel
    from models.address import AddressModel
    from models.bill import BillModel
    from models.order import OrderModel
    from models.order_detail import OrderDetailModel
    from models.review import ReviewModel
    from models.enums import DeliveryMethod, Status, PaymentType

    # Create category
    category = CategoryModel(name="Electronics")
    db_session.add(category)
    db_session.flush()

    # Create product
    product = ProductModel(
        name="Laptop",
        price=999.99,
        stock=10,
        category_id=category.id_key
    )
    db_session.add(product)
    db_session.flush()

    # Create client
    client = ClientModel(
        name="John",
        lastname="Doe",
        email="john.doe@example.com",
        telephone="+1234567890"
    )
    db_session.add(client)
    db_session.flush()

    # Create address
    address = AddressModel(
        street="123 Main St",
        city="New York",
        postal_code="10001",
        country="USA",
        client_id=client.id_key
    )
    db_session.add(address)
    db_session.flush()

    # Create bill
    bill = BillModel(
        bill_number="BILL-001",
        discount=10.0,
        date=date.today(),
        total=989.99,
        payment_type=PaymentType.CASH,
        client_id=client.id_key  # ✅ Added - required field
    )
    db_session.add(bill)
    db_session.flush()

    # Create order
    order = OrderModel(
        date=datetime.utcnow(),
        total=989.99,
        delivery_method=DeliveryMethod.DRIVE_THRU,
        status=Status.PENDING,
        client_id=client.id_key,
        bill_id=bill.id_key
    )
    db_session.add(order)
    db_session.flush()

    # Create order detail
    order_detail = OrderDetailModel(
        quantity=1,
        price=999.99,
        order_id=order.id_key,
        product_id=product.id_key
    )
    db_session.add(order_detail)
    db_session.flush()

    # Create review
    review = ReviewModel(
        rating=5.0,  # Float in range 1.0-5.0
        comment="Excellent product, highly recommended!",  # Min 10 chars
        product_id=product.id_key
        # Note: client_id removed - ReviewModel doesn't have this field
    )
    db_session.add(review)

    db_session.commit()

    return {
        "category": category,
        "product": product,
        "client": client,
        "address": address,
        "bill": bill,
        "order": order,
        "order_detail": order_detail,
        "review": review
    }


# Mock Redis client for testing
@pytest.fixture
def mock_redis(monkeypatch):
    """Mock Redis client for testing."""
    class MockRedis:
        def __init__(self):
            self.data = {}
            self.expirations = {}

        def get(self, key):
            return self.data.get(key)

        def set(self, key, value, ex=None):
            self.data[key] = value
            if ex:
                self.expirations[key] = ex
            return True

        def delete(self, key):
            if key in self.data:
                del self.data[key]
            if key in self.expirations:
                del self.expirations[key]
            return True

        def incr(self, key):
            current = int(self.data.get(key, 0))
            self.data[key] = str(current + 1)
            return current + 1

        def expire(self, key, seconds):
            self.expirations[key] = seconds
            return True

        def pipeline(self):
            return MockPipeline(self)

        def ping(self):
            return True

    class MockPipeline:
        def __init__(self, redis_client):
            self.redis = redis_client
            self.commands = []

        def incr(self, key):
            self.commands.append(('incr', key))
            return self

        def expire(self, key, seconds):
            self.commands.append(('expire', key, seconds))
            return self

        def execute(self):
            results = []
            for cmd in self.commands:
                if cmd[0] == 'incr':
                    results.append(self.redis.incr(cmd[1]))
                elif cmd[0] == 'expire':
                    results.append(self.redis.expire(cmd[1], cmd[2]))
            self.commands = []
            return results

    return MockRedis()
