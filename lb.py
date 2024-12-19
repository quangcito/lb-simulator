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
    current_amplification: float = 1.0
    retry_budget_remaining: float = 1.0

class RetryBudget:
    def __init__(self, ratio: float = 0.1):
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
            if not initial_request:
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
        try:
            if self.status == ServerStatus.FAILED:
                return False

            try:
                self.queue.put_nowait(request_id)
            except Full:
                return False

            self.current_load += 1

            load_ratio = self.current_load / self.capacity
            failure_prob = max(0, (load_ratio - 0.6) * 2)

            if self.status == ServerStatus.DEGRADED:
                failure_prob += 0.3

            actual_time = self.processing_time * (1 + load_ratio * 2)
            if self.status == ServerStatus.DEGRADED:
                actual_time *= 3

            await asyncio.sleep(actual_time)

            if random.random() < failure_prob:
                return False

            return True

        finally:
            self.current_load = max(0, self.current_load - 1)
            try:
                self.queue.get_nowait()
            except:
                pass

class MemoryLeakServer(Server):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.peak_processing_time = self.processing_time

    async def simulate_processing(self, request_id: str) -> bool:
        if self.processing_time > self.peak_processing_time:
            self.peak_processing_time = self.processing_time
            print(f"\nNew peak processing time: {self.processing_time:.3f}s")
            print(f"Degradation factor: {self.processing_time / 0.01:.1f}x")
        return await super().simulate_processing(request_id)

class LoadBalancer:
    def __init__(self):
        self.servers: List[Server] = []
        self.retry_budget = RetryBudget(ratio=0.1)
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

        self.metrics.total_requests += 1
        if success:
            self.metrics.successful_requests += 1
        else:
            self.metrics.failed_requests += 1

        total_load = sum(s.current_load for s in self.servers)
        normal_load = len(self.servers) * 0.5
        self.metrics.current_amplification = total_load / normal_load if normal_load > 0 else 1.0

        return success

    def _select_server(self) -> Optional[Server]:
        available_servers = [s for s in self.servers if s.current_load < s.capacity]
        if not available_servers:
            return None
        return min(available_servers, key=lambda s: s.current_load)

    def print_metrics(self):
        print("\nSystem Metrics:")
        total_retries = self.retry_budget.retry_count
        base_requests = self.metrics.total_requests - total_retries
        amplification = self.metrics.total_requests / max(1, base_requests) if base_requests > 0 else 1.0
        retry_rate = total_retries / max(1, self.metrics.total_requests)

        print(f"Load Amplification: {amplification:.2f}x")
        print(f"Retry Budget Remaining: {self.retry_budget.get_retry_budget_remaining():.1%}")
        print(f"Success Rate: {(self.metrics.successful_requests / max(1, self.metrics.total_requests)):.1%}")
        print(f"Retry Rate: {retry_rate:.1%}")

async def monitor_and_report_metrics(lb: LoadBalancer, scenario_name: str, interval: float = 0.5):
    start_time = time.time()
    try:
        while True:
            await asyncio.sleep(interval)
            elapsed = time.time() - start_time
            print(f"\n{scenario_name} Metrics at {elapsed:.1f}s:")
            lb.print_metrics()

            server_states = [
                f"Server {s.id}: {s.status.value} (load: {s.current_load}/{s.capacity})"
                for s in lb.servers
            ]
            print("\nServer States:")
            for state in server_states:
                print(state)
    except asyncio.CancelledError:
        elapsed = time.time() - start_time
        print(f"\nFinal {scenario_name} Metrics at {elapsed:.1f}s:")
        lb.print_metrics()

async def run_scenario(name: str, func):
    print(f"\n{'='*20}")
    print(f" {name} ")
    print(f"{'='*20}\n")
    await func()
    print(f"\nEnd of {name}\n")

def reset_load_balancer(num_servers=3, capacity=5):
    lb = LoadBalancer()
    for i in range(num_servers):
        server = Server(f"server{i}", capacity=capacity)
        lb.add_server(server)
    return lb

async def simulate_failure_scenarios():
    async def scenario_1():
        lb = reset_load_balancer()
        print("Phase 1: Normal operation")
        for i in range(10):
            await lb.process_request(f"request_{i}")
            await asyncio.sleep(0.05)
        lb.print_metrics()

        print("\nForcing server1 to FAILED state...")
        lb.servers[0].status = ServerStatus.FAILED

        print("\nPhase 2: High load with failed server")
        tasks = []
        for i in range(20):
            tasks.append(lb.process_request(f"request_fail_{i}"))
            if len(tasks) >= 5:
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(0.01)
        if tasks:
            await asyncio.gather(*tasks)
        lb.print_metrics()

    async def scenario_2():
        lb = reset_load_balancer()
        for server in lb.servers:
            server.processing_time = 0.05

        print("Phase 1: Normal operation")
        for i in range(10):
            await lb.process_request(f"request_normal_{i}")
            await asyncio.sleep(0.05)
        lb.print_metrics()

        print("\nPhase 2: Extreme load")
        tasks = []
        for i in range(50):
            tasks.append(lb.process_request(f"request_load_{i}"))
            if len(tasks) >= 15:
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(0.01)
        if tasks:
            await asyncio.gather(*tasks)
        lb.print_metrics()

    async def scenario_3():
        lb = reset_load_balancer()
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

    async def scenario_4():
        lb = reset_load_balancer()
        monitor_task = asyncio.create_task(
            monitor_and_report_metrics(lb, "Cascading Failure", interval=0.5)
        )

        print("\nStarting with one failed server...")
        lb.servers[0].status = ServerStatus.FAILED

        batch_size = 3  # Smaller batches to see progression better
        total_requests = 30

        for i in range(total_requests):
            success = await lb.process_request(f"request_cascade_{i}")

            print(f"\nCurrent load ratios:")
            # Check servers more frequently
            for server in lb.servers[1:]:
                load_ratio = server.current_load / server.capacity
                print(f"Server {server.id}: {load_ratio:.2f}")  # Add this line

                if server.status == ServerStatus.HEALTHY and load_ratio > 0.7:
                    server.status = ServerStatus.DEGRADED
                    print(f"\nServer {server.id} degraded (load ratio: {load_ratio:.2f})")
                elif server.status == ServerStatus.DEGRADED and load_ratio > 0.9:
                    server.status = ServerStatus.FAILED
                    print(f"\nServer {server.id} failed (load ratio: {load_ratio:.2f})")

            await asyncio.sleep(0.1)  # Longer sleep to allow load to build up

        monitor_task.cancel()
        await asyncio.sleep(0.1)

    async def scenario_5():
        lb = reset_load_balancer()
        print("Servers gradually degrade and recover in sequence")

        monitor_task = asyncio.create_task(
            monitor_and_report_metrics(lb, "Performance Wave", interval=1.0)
        )

        async def degrade_and_recover(server, delay_start, degraded_duration):
            await asyncio.sleep(delay_start)
            print(f"\nServer {server.id} starting degradation")
            print(f"Current load: {server.current_load}/{server.capacity}")  # Add this line
            server.status = ServerStatus.DEGRADED
            server.processing_time *= 3
            await asyncio.sleep(degraded_duration)
            print(f"\nServer {server.id} recovering to healthy state")
            print(f"Current load: {server.current_load}/{server.capacity}")  # Add this line
            server.processing_time /= 3
            server.status = ServerStatus.HEALTHY

        # Start degradation cycles for each server
        degrade_tasks = []
        for i, server in enumerate(lb.servers):
            degrade_tasks.append(asyncio.create_task(
                degrade_and_recover(server, i * 1.0, 0.8)))  # Longer duration

        # Send constant traffic during degradation
        request_tasks = []
        for i in range(30):
            request_tasks.append(lb.process_request(f"request_wave_{i}"))
            if len(request_tasks) >= 3:  # Smaller batches
                await asyncio.gather(*request_tasks)
                request_tasks = []
                await asyncio.sleep(0.1)  # Longer sleep between batches

        await asyncio.gather(*degrade_tasks)
        if request_tasks:
            await asyncio.gather(*request_tasks)

        monitor_task.cancel()
        await asyncio.sleep(0.1)

    async def scenario_6():
        lb = reset_load_balancer(num_servers=1, capacity=10)
        server = MemoryLeakServer("leaky_server", capacity=10)
        server.processing_time = 0.01
        lb.servers[0] = server

        async def memory_leak_simulation(server):
            try:
                while True:
                    await asyncio.sleep(0.1)  # Faster degradation
                    old_processing_time = server.processing_time
                    server.processing_time *= 1.2  # More aggressive growth

                    if server.processing_time > 0.1 and server.status == ServerStatus.HEALTHY:
                        server.status = ServerStatus.DEGRADED
                        print(f"\nServer status changed to DEGRADED (processing time: {server.processing_time:.3f}s)")
                    elif server.processing_time > 0.5 and server.status == ServerStatus.DEGRADED:
                        server.status = ServerStatus.FAILED
                        print(f"\nServer status changed to FAILED (processing time: {server.processing_time:.3f}s)")
            except asyncio.CancelledError:
                print("\nMemory leak simulation stopped")

        leak_task = asyncio.create_task(memory_leak_simulation(server))

        for i in range(30):
            success = await lb.process_request(f"request_leak_{i}")
            if i % 5 == 0:
                print(f"\nRequest batch {i//5} completed")
                print(f"Current processing time: {server.processing_time:.3f}s")
                lb.print_metrics()
            await asyncio.sleep(0.05)

        leak_task.cancel()
        await asyncio.sleep(0.1)


    # Run all scenarios
    await run_scenario("Scenario 1: Complete Server Failure", scenario_1)
    await run_scenario("Scenario 2: Load-induced Failures", scenario_2)
    await run_scenario("Scenario 3: Degraded Performance", scenario_3)
    await run_scenario("Scenario 4: Cascading Failure", scenario_4)
    await run_scenario("Scenario 5: Degraded Performance Wave", scenario_5)
    await run_scenario("Scenario 6: Memory Leak", scenario_6)

if __name__ == "__main__":
    asyncio.run(simulate_failure_scenarios())
