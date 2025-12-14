import grpc
import time
import uuid
import sys
import database_pb2
import database_pb2_grpc

def run():
    print("Connecting to Coordinator...")
    channel = grpc.insecure_channel('localhost:50050')
    stub = database_pb2_grpc.CoordinatorStub(channel)

    table_name = "test_rebal"
    print(f"Registering table {table_name}...")
    try:
        stub.RegisterTable(database_pb2.RegisterTableRequest(
            table_name=table_name,
            primary_key=database_pb2.PrimaryKeySchema(partition_key_name="pk", sort_key_name="sk")
        ))
    except grpc.RpcError as e:
        print(f"RegisterTable failed: {e.details()}")

    # 2. Insert Records
    records_count = 1000
    print(f"Inserting {records_count} records...")
    for i in range(records_count):
        pk = f"user{i}"
        sk = "profile"
        try:
            stub.CreateRecord(database_pb2.CreateRecordRequest(
                table_name=table_name,
                record=database_pb2.Record(
                    partition_key=pk,
                    sort_key=sk,
                    attributes={"data": f"value-{i}"}
                )
            ))
        except grpc.RpcError as e:
            print(f"Insert failed for {pk}: {e.details()}")

    print("Insertion complete. System should be stable.")
    print("NOW: Please scale up the system (e.g., docker compose scale shard=4).")
    print("Waiting 30 seconds for manual scale up and rebalancing...")
    time.sleep(30)
    
    print("Verifying data availability...")
    found = 0
    for i in range(records_count):
        pk = f"user{i}"
        sk = "profile"
        try:
            resp = stub.ReadRecord(database_pb2.ReadRecordRequest(
                table_name=table_name,
                partition_key=pk,
                sort_key=sk
            ))
            if resp.found:
                found += 1
            else:
                print(f"Record {pk} NOT FOUND")
        except grpc.RpcError as e:
            print(f"Read failed for {pk}: {e.details()}")

    print(f"Found {found}/{records_count} records.")
    if found == records_count:
        print("SUCCESS: specific data verified despite rebalancing.")
    else:
        print("FAILURE: Data loss or inconsistency.")

if __name__ == "__main__":
    run()
