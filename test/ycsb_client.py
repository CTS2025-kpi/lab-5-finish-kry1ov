import asyncio
import grpc
import random
import time
from statistics import mean
import database_pb2
import database_pb2_grpc
import sys

COORDINATOR_ADDRESS = "localhost:50050"
TABLE = "ycsb_users"

N_OPERATIONS = int(sys.argv[1])
READ_RATIO = float(sys.argv[2])


async def register_table(stub):
    try:
        await stub.RegisterTable(
            database_pb2.RegisterTableRequest(
                table_name=TABLE,
                primary_key=database_pb2.PrimaryKeySchema(
                    partition_key_name="user_id",
                    sort_key_name="field_id"
                )
            )
        )
    except grpc.RpcError as e:
        # Ignore if already exists
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            print(f"Warning: RegisterTable failed: {e.details()}")


async def put_record(stub, partition, sort, i):
    record = database_pb2.Record(
        partition_key=partition,
        sort_key=sort,
        attributes={"name": f"User{i}", "score": str(random.randint(0, 1000))}
    )
    
    start = time.perf_counter()
    try:
        await stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name=TABLE,
                record=record
            ),
            timeout=3
        )
        return (time.perf_counter() - start) * 1000
    except grpc.RpcError:
        return None


async def get_record(stub, partition, sort):
    start = time.perf_counter()
    try:
        await stub.ReadRecord(
            database_pb2.ReadRecordRequest(
                table_name=TABLE,
                partition_key=partition,
                sort_key=sort
            ),
            timeout=3
        )
        return (time.perf_counter() - start) * 1000
    except grpc.RpcError:
        return None


async def run_benchmark():
    async with grpc.aio.insecure_channel(COORDINATOR_ADDRESS) as channel:
        stub = database_pb2_grpc.CoordinatorStub(channel)
        
        print(f"Connecting to {COORDINATOR_ADDRESS}...")
        await register_table(stub)
        
        latencies_put, latencies_get = [], []
        created_keys = []

        users_count = 100

        print("Populating initial data...")
        for i in range(users_count):
            partition = f"user{i//users_count}"
            sort = f"id{i}"
            latency = await put_record(stub, partition, sort, i)
            if latency:
                created_keys.append((partition, sort))

        print("Initial data populated. Sleeping for 5 seconds, letting replicas catch up...")
        time.sleep(5)
        print(f"Starting benchmark ({N_OPERATIONS} ops, {READ_RATIO*100}% reads)...")
        start_all = time.perf_counter()
        
        for i in range(N_OPERATIONS):
            if random.random() < READ_RATIO and created_keys:
                partition, sort = random.choice(created_keys)
                latency = await get_record(stub, partition, sort)
                if latency:
                    latencies_get.append(latency)
            else:
                partition = f"user{i//users_count}"
                sort = f"id{i}"
                latency = await put_record(stub, partition, sort, i)
                if latency:
                    latencies_put.append(latency)
                    created_keys.append((partition, sort))

        total_time = time.perf_counter() - start_all
        total_ops = len(latencies_put) + len(latencies_get)
        throughput = total_ops / total_time if total_time > 0 else 0

        put_avg = mean(latencies_put) if latencies_put else 0
        get_avg = mean(latencies_get) if latencies_get else 0

        print("\n=== BENCHMARK RESULTS ===")
        print(f"Total operations: {total_ops}")
        print(f"Total time: {total_time:.2f}s")
        print(f"Throughput: {throughput:.2f} ops/sec")
        print(f"PUT avg latency: {put_avg:.2f} ms ({len(latencies_put)} ops)")
        print(f"GET avg latency: {get_avg:.2f} ms ({len(latencies_get)} ops)")
        print("===========================")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
