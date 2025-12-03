import os
import logging
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from models.address import AddressModel  # noqa
from models.base_model import base
from models.bill import BillModel  # noqa
from models.category import CategoryModel  # noqa
from models.client import ClientModel  # noqa
from models.order import OrderModel  # noqa
from models.order_detail import OrderDetailModel  # noqa
from models.product import ProductModel  # noqa
from models.review import ReviewModel  # noqa

# Get logger (logging is configured in main.py)
logger = logging.getLogger(__name__)

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(env_path)

# Database configuration with defaults
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

# High-performance connection pool configuration
# For 400 concurrent requests with 4 workers: 400/4 = 100 connections per worker
# Pool size + max_overflow should handle peak load
POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '50'))  # Base pool size per worker
MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '100'))  # Additional connections during peak
POOL_TIMEOUT = int(os.getenv('DB_POOL_TIMEOUT', '10'))  # Wait time for connection (reduced for production)
POOL_RECYCLE = int(os.getenv('DB_POOL_RECYCLE', '3600'))  # Recycle connections after 1 hour

DATABASE_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# Create engine with optimized connection pooling for high concurrency
engine = create_engine(
    DATABASE_URI,
    pool_pre_ping=True,  # Verify connections before using (prevents stale connections)
    pool_size=POOL_SIZE,  # Minimum number of connections in pool
    max_overflow=MAX_OVERFLOW,  # Additional connections beyond pool_size
    pool_timeout=POOL_TIMEOUT,  # Seconds to wait before giving up on connection
    pool_recycle=POOL_RECYCLE,  # Recycle connections to prevent stale connections
    echo=False,  # Disable SQL logging in production for performance
    future=True,  # Use SQLAlchemy 2.0 style
)

# SessionLocal class for creating new sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency injection for database sessions.
    Creates a new session for each request and closes it when done.

    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """Create all tables in the database."""
    try:
        base.metadata.create_all(engine)
        logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise


def drop_database():
    """Drop all tables in the database."""
    try:
        base.metadata.drop_all(engine)
        logger.info("Tables dropped successfully.")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise


def check_connection() -> bool:
    """Check if database connection is working."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Database connection established.")
        return True
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return False
