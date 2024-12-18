import random
import time
from dataclasses import dataclass
from typing import List, Dict, Optional
from queue import Queue, Full
from threading import Lock
import asyncio
from enum import Enum

class ServerStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"

@dataclass
class RetryMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    retry_attempts: int = 0
    current_amplification: float = 1.0  # Current load amplification factor
    retry_budget_remaining: float = 1.0  # Percentage of retry budget remaining

class RetryBudget:
    def __init__(self, ratio: float = 0.1):  # 10% retry budget
        self.ratio = ratio
        self.success_count = 0
        self.retry_count = 0
        self._lock = Lock()

    def can_retry(self) -> bool:
        with self._lock:
            max_retries = max(1, int(self.success_count * self.ratio))
            return self.retry_count < max_retries

    def record_success(self):
        with self._lock:
            self.success_count += 1

    def record_retry(self):
        with self._lock:
            self.retry_count += 1

    def get_retry_budget_remaining(self) -> float:
        with self._lock:
            max_retries = max(1, int(self.success_count * self.ratio))
            if max_retries == 0:
                return 0
            return max(0, (max_retries - self.retry_count) / max_retries)

class RetryableRequest:
    def __init__(self,
                 request_id: str,
                 retry_budget: RetryBudget,
                 max_retries: int = 3,
                 base_delay: float = 0.05,
                 max_delay: float = 1.0):
        self.request_id = request_id
        self.retry_budget = retry_budget
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.attempt_count = 0
        self.metrics = RetryMetrics()

    async def execute_with_retry(self, server) -> bool:
        initial_request = True
        while self.attempt_count <= self.max_retries:
            if not initial_request:  # This is a retry
                if not self.retry_budget.can_retry():
                    return False

                delay = min(self.base_delay * (2 ** self.attempt_count), self.max_delay)
                delay = random.uniform(0, delay)
                await asyncio.sleep(delay)

                self.retry_budget.record_retry()
                self.metrics.retry_attempts += 1
                server.metrics.retry_attempts += 1

            try:
                initial_request = False
                self.attempt_count += 1
                self.metrics.total_requests += 1
                server.metrics.total_requests += 1

                success = await server.simulate_processing(self.request_id)

                if success:
                    self.retry_budget.record_success()
                    self.metrics.successful_requests += 1
                    server.metrics.successful_requests += 1
                    return True
                else:
                    self.metrics.failed_requests += 1
                    server.metrics.failed_requests += 1

            except Exception as e:
                self.metrics.failed_requests += 1
                server.metrics.failed_requests += 1

        return False

@dataclass
class Server:
    def __init__(self, id: str, capacity: int):
        self.id = id
        self.capacity = capacity
        self.current_load = 0
        self.status = ServerStatus.HEALTHY
        self.metrics = RetryMetrics()
        self.processing_time = 0.01
        self.queue = Queue(maxsize=capacity)

    async def simulate_processing(self, request_id: str) -> bool:
        """Simulates request processing with load-aware failures"""
        try:
            # Check server status first
            if self.status == ServerStatus.FAILED:
                return False

            # Try to add to queue
            try:
                self.queue.put_nowait(request_id)
            except Full:
                return False

            self.current_load += 1

            # Calculate load-based failure probability
            load_ratio = self.current_load / self.capacity
            failure_prob = max(0, (load_ratio - 0.6) * 2)  # Start failing at 60% load

            # Add extra failure probability if degraded
            if self.status == ServerStatus.DEGRADED:
                failure_prob += 0.3  # 30% extra failure chance when degraded

            # Simulate processing with load-aware latency
            actual_time = self.processing_time * (1 + load_ratio * 2)
            if self.status == ServerStatus.DEGRADED:
                actual_time *= 3  # 3x slower when degraded

            await asyncio.sleep(actual_time)

            # Determine if request fails
            if random.random() < failure_prob:
                return False

            return True

        finally:
            self.current_load = max(0, self.current_load - 1)
            try:
                self.queue.get_nowait()
            except:
                pass

    async def execute_with_retry(self, server) -> bool:
        while self.attempt_count <= self.max_retries:
            if self.attempt_count > 0:
                # Check retry budget before attempting retry
                if not self.retry_budget.can_retry():
                    print(f"Request {self.request_id} retry rejected - budget exceeded")
                    return False

                # Calculate delay with exponential backoff and jitter
                delay = min(self.base_delay * (2 ** self.attempt_count), self.max_delay)
                delay = random.uniform(0, delay)  # Full jitter
                await asyncio.sleep(delay)

                self.retry_budget.record_retry()
                self.metrics.retry_attempts += 1
                print(f"Request {self.request_id} retry attempt {self.attempt_count}")

            try:
                self.attempt_count += 1
                self.metrics.total_requests += 1

                success = await server.simulate_processing(self.request_id)

                if success:
                    self.retry_budget.record_success()
                    self.metrics.successful_requests += 1
                    return True
                else:
                    self.metrics.failed_requests += 1
                    print(f"Request {self.request_id} failed, attempt {self.attempt_count}")

            except Exception as e:
                self.metrics.failed_requests += 1
                print(f"Request {self.request_id} error: {str(e)}")

        return False


class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_timeout: float):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED = normal, OPEN = stopped, HALF-OPEN = testing
        self._lock = Lock()

    def record_failure(self):
        with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "OPEN"

    def record_success(self):
        with self._lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failures = 0

    def allow_request(self) -> bool:
        with self._lock:
            if self.state == "CLOSED":
                return True
            elif self.state == "OPEN":
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF-OPEN"
                    return True
            return False

class RateLimiter:
    def __init__(self, requests_per_second: int):
        self.rate = requests_per_second
        self.tokens = requests_per_second
        self.last_update = time.time()
        self._lock = Lock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.time()
            time_passed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + time_passed * self.rate)
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

class LoadBalancer:
    def __init__(self):
        self.servers: List[Server] = []
        self.retry_budget = RetryBudget(ratio=0.1)  # 10% retry budget
        self.metrics = RetryMetrics()

    def add_server(self, server: Server):
        self.servers.append(server)

    async def process_request(self, request_id: str) -> bool:
        server = self._select_server()
        if not server:
            return False

        retryable_request = RetryableRequest(
            request_id=request_id,
            retry_budget=self.retry_budget
        )

        success = await retryable_request.execute_with_retry(server)

        # Update load balancer metrics
        self.metrics.total_requests += 1
        if success:
            self.metrics.successful_requests += 1
        else:
            self.metrics.failed_requests += 1

        # Calculate current load amplification
        total_load = sum(s.current_load for s in self.servers)
        normal_load = len(self.servers) * 0.5  # Assuming 50% normal load
        self.metrics.current_amplification = total_load / normal_load if normal_load > 0 else 1.0

        return success

    def _select_server(self) -> Optional[Server]:
        available_servers = [s for s in self.servers if s.current_load < s.capacity]
        if not available_servers:
            return None
        return min(available_servers, key=lambda s: s.current_load)

    def print_metrics(self):
        print("\nSystem Metrics:")

        # Track retries through retry budget
        total_retries = self.retry_budget.retry_count
        base_requests = self.metrics.total_requests - total_retries

        # Calculate amplification including retries
        amplification = self.metrics.total_requests / max(1, base_requests) if base_requests > 0 else 1.0

        # Calculate retry rate
        retry_rate = total_retries / max(1, self.metrics.total_requests)

        print(f"Load Amplification: {amplification:.2f}x")
        print(f"Retry Budget Remaining: {self.retry_budget.get_retry_budget_remaining():.1%}")
        print(f"Success Rate: {(self.metrics.successful_requests / max(1, self.metrics.total_requests)):.1%}")
        print(f"Retry Rate: {retry_rate:.1%}")

async def simulate_failure_scenario():
    lb = LoadBalancer()

    # Add servers with very low capacity to make failures more likely
    for i in range(3):
        lb.add_server(Server(f"server{i}", capacity=5))

    print("\n=== Scenario 1: Complete Server Failure ===")
    print("Phase 1: Normal operation")
    for i in range(10):
        await lb.process_request(f"request_{i}")
        await asyncio.sleep(0.05)
    lb.print_metrics()

    # Force server failure
    print("\nForcing server1 to FAILED state...")
    lb.servers[0].status = ServerStatus.FAILED

    print("\nPhase 2: High load with failed server")
    tasks = []
    for i in range(20):
        tasks.append(lb.process_request(f"request_fail_{i}"))
        if len(tasks) >= 5:  # Send requests in batches of 5
            await asyncio.gather(*tasks)
            tasks = []
            await asyncio.sleep(0.01)
    if tasks:
        await asyncio.gather(*tasks)
    lb.print_metrics()

    # Reset load balancer for next scenario
    lb = LoadBalancer()
    for i in range(3):
        server = Server(f"server{i}", capacity=5)
        server.processing_time = 0.05  # Increased base processing time
        lb.add_server(server)

    print("\n=== Scenario 2: Load-induced Failures ===")
    print("Phase 1: Normal operation")
    for i in range(10):
        await lb.process_request(f"request_normal_{i}")
        await asyncio.sleep(0.05)
    lb.print_metrics()

    print("\nPhase 2: Extreme load (should cause load-based failures)")
    tasks = []
    for i in range(30):
        tasks.append(lb.process_request(f"request_load_{i}"))
        if len(tasks) >= 10:  # Send requests in batches of 10
            await asyncio.gather(*tasks)
            tasks = []
            await asyncio.sleep(0.01)
    if tasks:
        await asyncio.gather(*tasks)
    lb.print_metrics()

    # Reset load balancer for final scenario
    lb = LoadBalancer()
    for i in range(3):
        server = Server(f"server{i}", capacity=5)
        server.processing_time = 0.05
        lb.add_server(server)

    print("\n=== Scenario 3: Degraded Performance ===")
    print("Phase 1: Normal operation")
    for i in range(10):
        await lb.process_request(f"request_pre_{i}")
        await asyncio.sleep(0.05)
    lb.print_metrics()

    print("\nDegrading all servers...")
    for server in lb.servers:
        server.status = ServerStatus.DEGRADED

    print("\nPhase 2: Operating with degraded servers")
    tasks = []
    for i in range(20):
        tasks.append(lb.process_request(f"request_deg_{i}"))
        if len(tasks) >= 5:
            await asyncio.gather(*tasks)
            tasks = []
            await asyncio.sleep(0.01)
    if tasks:
        await asyncio.gather(*tasks)
    lb.print_metrics()

if __name__ == "__main__":
    async def main():
        await simulate_failure_scenario()

    asyncio.run(main())
