"""
Load Testing Script for FastAPI E-commerce API

Tests the API's ability to handle 400 concurrent requests.
Requires: pip install locust
"""
import random
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner


class EcommerceUser(HttpUser):
    """
    Simulates a user interacting with the e-commerce API.
    """
    # Wait between 1-3 seconds between requests (simulate real user behavior)
    wait_time = between(1, 3)

    def on_start(self):
        """Called when a user starts."""
        self.client.verify = False  # Disable SSL verification for testing

    @task(10)
    def list_products(self):
        """List products with pagination (most common operation)."""
        skip = random.randint(0, 100)
        limit = random.randint(10, 50)
        self.client.get(f"/products?skip={skip}&limit={limit}")

    @task(8)
    def get_product(self):
        """Get a specific product by ID."""
        product_id = random.randint(1, 100)
        self.client.get(f"/products/{product_id}")

    @task(10)
    def list_clients(self):
        """List clients with pagination."""
        skip = random.randint(0, 50)
        limit = random.randint(10, 30)
        self.client.get(f"/clients?skip={skip}&limit={limit}")

    @task(6)
    def get_client(self):
        """Get a specific client by ID."""
        client_id = random.randint(1, 50)
        self.client.get(f"/clients/{client_id}")

    @task(8)
    def list_categories(self):
        """List all categories."""
        self.client.get("/categories")

    @task(5)
    def list_orders(self):
        """List orders with pagination."""
        skip = random.randint(0, 50)
        limit = random.randint(10, 20)
        self.client.get(f"/orders?skip={skip}&limit={limit}")

    @task(3)
    def create_client(self):
        """Create a new client."""
        client_data = {
            "name": f"TestUser{random.randint(1000, 9999)}",
            "lastname": f"Lastname{random.randint(1000, 9999)}",
            "email": f"test{random.randint(1000, 9999)}@example.com",
            "telephone": f"+1{random.randint(1000000000, 9999999999)}"
        }
        self.client.post("/clients", json=client_data)

    @task(2)
    def create_product(self):
        """Create a new product."""
        product_data = {
            "name": f"Product{random.randint(1000, 9999)}",
            "price": round(random.uniform(10.0, 500.0), 2),
            "stock": random.randint(0, 100),
            "category_id": random.randint(1, 10)
        }
        self.client.post("/products", json=product_data)

    @task(15)
    def health_check(self):
        """Check API health (lightest operation)."""
        self.client.get("/health_check")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Print configuration when Locust starts."""
    if isinstance(environment.runner, MasterRunner):
        print("""
╔══════════════════════════════════════════════════════════════╗
║          🔥 E-commerce API Load Test - Master Node         ║
╚══════════════════════════════════════════════════════════════╝
        """)


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Print when test starts."""
    print("""
📊 Load Test Started
Target: 400 concurrent users
Ramp-up: Gradual increase to simulate real traffic
        """)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Print summary when test completes."""
    print("""
✅ Load Test Completed

Review the statistics above for:
  • Response times (50th, 95th, 99th percentile)
  • Requests per second (RPS)
  • Failure rate (should be < 1%)
  • Database connection pool usage
        """)


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║        FastAPI E-commerce API - Load Testing Tool          ║
╚══════════════════════════════════════════════════════════════╝

USAGE:
------

1. Install Locust:
   pip install locust

2. Start your API server:
   python run_production.py

3. Run load test (Web UI):
   locust -f load_test.py --host=http://localhost:8000

   Then open: http://localhost:8089
   Configure:
     • Number of users: 400
     • Spawn rate: 50 users/second
     • Host: http://localhost:8000

4. Run load test (Headless - 400 users):
   locust -f load_test.py \\
     --host=http://localhost:8000 \\
     --users 400 \\
     --spawn-rate 50 \\
     --run-time 5m \\
     --headless

5. Run distributed load test (multiple machines):
   # Master:
   locust -f load_test.py --master --host=http://your-api-url

   # Workers (run on multiple machines):
   locust -f load_test.py --worker --master-host=<master-ip>

METRICS TO MONITOR:
-------------------
✓ Response Time p95 < 200ms (good)
✓ Response Time p99 < 500ms (acceptable)
✓ Requests/sec > 100 RPS
✓ Failure rate < 1%
✓ Database connections < max_connections

DATABASE MONITORING (while testing):
------------------------------------
docker exec ecommerce_postgres_prod psql -U postgres -c \\
  "SELECT count(*) as active, max_conn FROM pg_stat_activity, \\
   (SELECT setting::int as max_conn FROM pg_settings \\
    WHERE name='max_connections') max \\
   GROUP BY max_conn;"

API MONITORING:
---------------
# Watch logs
docker-compose -f docker-compose.production.yaml logs -f api

# Monitor container stats
docker stats ecommerce_api_prod

EXPECTED RESULTS:
-----------------
With proper configuration:
  • 400 concurrent users ✓
  • ~100-200 RPS sustained ✓
  • p95 response time < 200ms ✓
  • 0% error rate ✓
  • Database pool < 600 connections ✓

    """)