"""
Concurrency Tests for Race Condition Fixes

Tests concurrent operations to validate SELECT FOR UPDATE locks
and ensure stock consistency under high concurrency.
"""
import pytest
import concurrent.futures
import threading
from sqlalchemy.orm import Session

from models.product import ProductModel
from models.order import OrderModel
from models.order_detail import OrderDetailModel
from models.client import ClientModel
from models.bill import BillModel
from models.category import CategoryModel
from services.order_detail_service import OrderDetailService
from services.product_service import ProductService
from schemas.order_detail_schema import OrderDetailSchema
from schemas.product_schema import ProductSchema


class TestConcurrentStockOperations:
    """Test concurrent operations on stock management"""

    def test_concurrent_order_detail_creation_prevents_overselling(self, db_session: Session):
        """
        Test that 100 concurrent order detail creations don't cause overselling

        Scenario:
        - Product has stock of 10
        - 100 concurrent requests try to buy 1 unit each
        - Only 10 should succeed, 90 should fail with insufficient stock
        """
        # Setup: Create product with limited stock
        category = CategoryModel(name="Electronics")
        db_session.add(category)
        db_session.commit()

        product = ProductModel(
            name="Limited Edition Product",
            price=99.99,
            stock=10,  # Only 10 units available
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        # Create client and bill for orders
        client = ClientModel(
            name="Test",
            lastname="Client",
            email="test@example.com",
            telephone="+1234567890"
        )
        db_session.add(client)
        db_session.commit()

        bill = BillModel(
            bill_number="BILL-CONCURRENT-001",
            total=999.90,
            client_id=client.id_key
        )
        db_session.add(bill)
        db_session.commit()

        # Create order for order details
        order = OrderModel(
            date="2025-11-17",
            delivery_method="Drive-thru",
            status="PENDING",
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()
        db_session.refresh(order)

        # Thread-safe counters
        success_count = threading.Lock()
        failure_count = threading.Lock()
        successes = []
        failures = []

        def create_order_detail(request_num: int):
            """Try to create an order detail (buy 1 unit)"""
            # Create new session for each thread
            from config.database import SessionLocal
            thread_db = SessionLocal()

            try:
                service = OrderDetailService(thread_db)

                # Try to buy 1 unit
                schema = OrderDetailSchema(
                    quantity=1,
                    price=product.price,
                    order_id=order.id_key,
                    product_id=product.id_key
                )

                result = service.save(schema)

                with success_count:
                    successes.append(request_num)

                return {"status": "success", "request": request_num}

            except ValueError as e:
                # Expected: Insufficient stock
                with failure_count:
                    failures.append(request_num)

                return {"status": "insufficient_stock", "request": request_num, "error": str(e)}

            except Exception as e:
                return {"status": "error", "request": request_num, "error": str(e)}

            finally:
                thread_db.close()

        # Execute 100 concurrent requests
        print("\n🧪 Testing 100 concurrent order detail creations...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(create_order_detail, i)
                for i in range(1, 101)  # 100 concurrent requests
            ]

            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # Analyze results
        success_results = [r for r in results if r["status"] == "success"]
        insufficient_stock_results = [r for r in results if r["status"] == "insufficient_stock"]
        error_results = [r for r in results if r["status"] == "error"]

        print(f"\n📊 Results:")
        print(f"  ✅ Successful purchases: {len(success_results)}")
        print(f"  ❌ Insufficient stock: {len(insufficient_stock_results)}")
        print(f"  🔥 Errors: {len(error_results)}")

        # Refresh product to get final stock
        db_session.refresh(product)
        print(f"  📦 Final stock: {product.stock}")

        # Assertions
        assert len(success_results) == 10, \
            f"Expected exactly 10 successful purchases, got {len(success_results)}"

        assert len(insufficient_stock_results) == 90, \
            f"Expected 90 insufficient stock errors, got {len(insufficient_stock_results)}"

        assert len(error_results) == 0, \
            f"Expected no errors, got {len(error_results)}: {error_results}"

        assert product.stock == 0, \
            f"Expected final stock to be 0, got {product.stock}"

        # Verify order details count
        order_details_count = db_session.query(OrderDetailModel).filter(
            OrderDetailModel.order_id == order.id_key
        ).count()

        assert order_details_count == 10, \
            f"Expected 10 order details, got {order_details_count}"

        print("✅ Concurrency test PASSED - No overselling detected!")


    def test_concurrent_order_detail_updates_maintain_stock_consistency(self, db_session: Session):
        """
        Test that concurrent updates to order detail quantities maintain stock consistency

        Scenario:
        - Product has stock of 100
        - OrderDetail has quantity of 1
        - 50 concurrent requests try to increase quantity to different values
        - Only requests that don't exceed available stock should succeed
        """
        # Setup
        category = CategoryModel(name="Electronics")
        db_session.add(category)
        db_session.commit()

        product = ProductModel(
            name="Updatable Product",
            price=50.00,
            stock=100,
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        client = ClientModel(
            name="Test",
            lastname="Client",
            email="update@example.com",
            telephone="+1234567890"
        )
        db_session.add(client)
        db_session.commit()

        bill = BillModel(
            bill_number="BILL-UPDATE-001",
            total=50.00,
            client_id=client.id_key
        )
        db_session.add(bill)
        db_session.commit()

        order = OrderModel(
            date="2025-11-17",
            delivery_method="Drive-thru",
            status="PENDING",
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        # Create initial order detail with quantity 1
        order_detail = OrderDetailModel(
            quantity=1,
            price=product.price,
            order_id=order.id_key,
            product_id=product.id_key
        )
        db_session.add(order_detail)

        # Update product stock (100 - 1 = 99)
        product.stock = 99
        db_session.commit()
        db_session.refresh(order_detail)

        initial_stock = product.stock
        print(f"\n🧪 Initial stock: {initial_stock}")

        successes = []
        failures = []
        success_lock = threading.Lock()
        failure_lock = threading.Lock()

        def update_order_detail(new_quantity: int):
            """Try to update order detail quantity"""
            from config.database import SessionLocal
            thread_db = SessionLocal()

            try:
                service = OrderDetailService(thread_db)

                schema = OrderDetailSchema(
                    quantity=new_quantity,
                    price=product.price,
                    order_id=order.id_key,
                    product_id=product.id_key
                )

                result = service.update(order_detail.id_key, schema)

                with success_lock:
                    successes.append(new_quantity)

                return {"status": "success", "quantity": new_quantity}

            except ValueError as e:
                with failure_lock:
                    failures.append(new_quantity)

                return {"status": "insufficient_stock", "quantity": new_quantity}

            except Exception as e:
                return {"status": "error", "quantity": new_quantity, "error": str(e)}

            finally:
                thread_db.close()

        # Try to update to quantities 2-51 concurrently (50 requests)
        print("🧪 Testing 50 concurrent order detail updates...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            futures = [
                executor.submit(update_order_detail, qty)
                for qty in range(2, 52)
            ]

            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        success_results = [r for r in results if r["status"] == "success"]
        failure_results = [r for r in results if r["status"] == "insufficient_stock"]

        print(f"\n📊 Results:")
        print(f"  ✅ Successful updates: {len(success_results)}")
        print(f"  ❌ Insufficient stock: {len(failure_results)}")

        # Refresh to get final state
        db_session.refresh(product)
        db_session.refresh(order_detail)

        print(f"  📦 Final stock: {product.stock}")
        print(f"  📋 Final order detail quantity: {order_detail.quantity}")

        # Only ONE update should succeed
        assert len(success_results) == 1, \
            f"Expected exactly 1 successful update, got {len(success_results)}"

        assert len(failure_results) == 49, \
            f"Expected 49 failed updates, got {len(failure_results)}"

        # Verify stock consistency
        expected_stock = initial_stock - (order_detail.quantity - 1)
        assert product.stock == expected_stock, \
            f"Expected stock {expected_stock}, got {product.stock}"

        print("✅ Update concurrency test PASSED - Stock consistency maintained!")


    def test_concurrent_order_detail_deletes_restore_stock_correctly(self, db_session: Session):
        """
        Test that concurrent deletes restore stock correctly

        Scenario:
        - Create 10 order details each with quantity 5
        - Delete all 10 concurrently
        - Stock should be restored correctly (initial_stock + 50)
        """
        # Setup
        category = CategoryModel(name="Electronics")
        db_session.add(category)
        db_session.commit()

        initial_stock = 100
        product = ProductModel(
            name="Deletable Product",
            price=25.00,
            stock=initial_stock - 50,  # Already consumed 50 units
            category_id=category.id_key
        )
        db_session.add(product)
        db_session.commit()

        client = ClientModel(
            name="Test",
            lastname="Client",
            email="delete@example.com",
            telephone="+1234567890"
        )
        db_session.add(client)
        db_session.commit()

        bill = BillModel(
            bill_number="BILL-DELETE-001",
            total=1250.00,
            client_id=client.id_key
        )
        db_session.add(bill)
        db_session.commit()

        order = OrderModel(
            date="2025-11-17",
            delivery_method="Drive-thru",
            status="PENDING",
            client_id=client.id_key,
            bill_id=bill.id_key
        )
        db_session.add(order)
        db_session.commit()

        # Create 10 order details, each with quantity 5
        order_detail_ids = []
        for i in range(10):
            od = OrderDetailModel(
                quantity=5,
                price=product.price,
                order_id=order.id_key,
                product_id=product.id_key
            )
            db_session.add(od)
            db_session.commit()
            db_session.refresh(od)
            order_detail_ids.append(od.id_key)

        print(f"\n🧪 Initial stock before deletes: {product.stock}")
        print(f"🧪 Created {len(order_detail_ids)} order details to delete")

        def delete_order_detail(od_id: int):
            """Try to delete order detail"""
            from config.database import SessionLocal
            thread_db = SessionLocal()

            try:
                service = OrderDetailService(thread_db)
                service.delete(od_id)
                return {"status": "success", "id": od_id}

            except Exception as e:
                return {"status": "error", "id": od_id, "error": str(e)}

            finally:
                thread_db.close()

        # Delete all 10 concurrently
        print("🧪 Deleting 10 order details concurrently...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(delete_order_detail, od_id)
                for od_id in order_detail_ids
            ]

            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        success_results = [r for r in results if r["status"] == "success"]
        error_results = [r for r in results if r["status"] == "error"]

        print(f"\n📊 Results:")
        print(f"  ✅ Successful deletes: {len(success_results)}")
        print(f"  🔥 Errors: {len(error_results)}")

        # Refresh product
        db_session.refresh(product)

        print(f"  📦 Final stock: {product.stock}")
        print(f"  📦 Expected stock: {initial_stock}")

        # All deletes should succeed
        assert len(success_results) == 10, \
            f"Expected 10 successful deletes, got {len(success_results)}"

        assert len(error_results) == 0, \
            f"Expected no errors, got {len(error_results)}"

        # Stock should be restored correctly
        assert product.stock == initial_stock, \
            f"Expected stock {initial_stock}, got {product.stock}"

        print("✅ Delete concurrency test PASSED - Stock restored correctly!")


@pytest.mark.integration
class TestConcurrentCacheOperations:
    """Test concurrent cache operations with distributed locks"""

    def test_concurrent_cache_stampede_prevention(self, db_session: Session):
        """
        Test that cache stampede protection works with concurrent requests

        Scenario:
        - Cache is empty
        - 100 concurrent requests for same data
        - Only 1 DB query should be executed (others wait for cache)
        """
        from services.cache_service import cache_service
        import time

        # Clear cache
        cache_service.clear_all()

        call_count = []
        call_count_lock = threading.Lock()

        def expensive_computation():
            """Simulates expensive DB query"""
            with call_count_lock:
                call_count.append(1)

            # Simulate slow query
            time.sleep(0.1)
            return {"data": "expensive_result", "timestamp": time.time()}

        def get_cached_data():
            """Get data with cache stampede protection"""
            return cache_service.get_or_set(
                key="expensive_data",
                callback=expensive_computation,
                ttl=300
            )

        print("\n🧪 Testing cache stampede with 100 concurrent requests...")

        # Execute 100 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(get_cached_data)
                for _ in range(100)
            ]

            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        print(f"\n📊 Results:")
        print(f"  📞 Total requests: 100")
        print(f"  🗄️  DB queries executed: {len(call_count)}")
        print(f"  ✅ Results returned: {len(results)}")

        # With stampede protection, only 1-3 queries should execute
        # (1 if perfect lock, 2-3 if some retries before cache fills)
        assert len(call_count) <= 5, \
            f"Expected ≤5 DB queries with stampede protection, got {len(call_count)}"

        assert len(results) == 100, \
            f"Expected 100 results, got {len(results)}"

        # All results should be identical (from cache)
        first_result = results[0]
        for result in results:
            assert result["data"] == first_result["data"], \
                "All results should be identical"

        print(f"✅ Cache stampede test PASSED - Only {len(call_count)} DB queries for 100 requests!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])