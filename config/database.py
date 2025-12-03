import os
import logging
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# Import models to register them with SQLAlchemy
from models.address import AddressModel  # noqa
from models.base_model import base
from models.bill import BillModel  # noqa
from models.category import CategoryModel  # noqa
from models.client import ClientModel  # noqa
from models.order import OrderModel  # noqa
from models.order_detail import OrderDetailModel  # noqa
from models.product import ProductModel  # noqa
from models.review import ReviewModel  # noqa

logger = logging.getLogger(__name__)

# Load .env (local only — Render injects env vars automatically)
load_dotenv()

# =======================================================
# 🔥 UNIVERSAL DATABASE CONFIG (LOCAL + RENDER)
# =======================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Local fallback
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

    DATABASE_URL = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

logger.info(f"📌 Using database: {DATABASE_URL}")

# =======================================================
# 🔥 Engine optimizado para Render
# =======================================================
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=10,
    pool_recycle=3600,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =======================================================
# 🔥 Crear tablas
# =======================================================
def create_tables():
    try:
        base.metadata.create_all(engine)
        logger.info("🟢 Tables created successfully.")
    except Exception as e:
        logger.error(f"❌ Error creating tables: {e}")
        raise


# =======================================================
# 🔥 Comprobar conexión
# =======================================================
def check_connection() -> bool:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("🟢 Database connection OK.")
        return True
    except Exception as e:
        logger.error(f"❌ DB connection failed: {e}")
        return False


# =======================================================
# 🔥 AUTO-CREATE TABLES WHEN RUNNING ON RENDER
# =======================================================
def init_render_tables():
    """
    Automatically create tables on Render.
    Render injects env var: RENDER=true
    """
    if os.getenv("RENDER") == "true":
        logger.info("🟣 Render detected — creating tables in remote DB...")
        try:
            create_tables()
            logger.info("🟢 Tables created successfully in Render DB")
        except Exception as e:
            logger.error(f"❌ Error creating tables in Render: {e}")


# Ejecutar automáticamente al importar este archivo
init_render_tables()

