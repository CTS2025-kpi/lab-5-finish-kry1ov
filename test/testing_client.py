import grpc
import time

import database_pb2
import database_pb2_grpc


def register_table(stub, table_name, partition_key_name, sort_key_name):
    try:
        response = stub.RegisterTable(
            database_pb2.RegisterTableRequest(
                table_name=table_name,
                primary_key=database_pb2.PrimaryKeySchema(
                    partition_key_name=partition_key_name, sort_key_name=sort_key_name
                ),
            )
        )
        print(f"Response: {response.message}\n")
    except grpc.RpcError as e:
        print(f"Error: {e.details()}\n")


def create_record(stub, table_name, record):
    try:
        response = stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name=table_name,
                record=record,
            )
        )
        print(
            f"Created {record.partition_key}:{record.sort_key} - {response.success}\n"
        )
    except grpc.RpcError as e:
        print(f"Error: {e.details()}\n")


def read_record(stub, table_name, pk, sk):
    try:
        response = stub.ReadRecord(
            database_pb2.ReadRecordRequest(
                table_name=table_name,
                partition_key=pk,
                sort_key=sk,
            )
        )
        if response.found:
            print(f"Found {pk}:{sk} - {response.records[0]}\n")
        else:
            print(f"Record {pk}:{sk} not found.\n")
    except grpc.RpcError as e:
        print(f"Error: {e.details()}\n")


def exists_record(stub, table_name, pk, sk):
    try:
        response = stub.ExistsRecord(
            database_pb2.ExistsRecordRequest(
                table_name=table_name,
                partition_key=pk,
                sort_key=sk,
            )
        )
        print(f"Exists {pk}:{sk} - {response.exists}\n")
    except grpc.RpcError as e:
        print(f"Error: {e.details()}\n")


def delete_record(stub, table_name, pk, sk):
    try:
        response = stub.DeleteRecord(
            database_pb2.DeleteRecordRequest(
                table_name=table_name,
                partition_key=pk,
                sort_key=sk,
            )
        )
        print(f"Deleted {pk}:{sk} - {response.success}\n")
    except grpc.RpcError as e:
        print(f"Error: {e.details()}\n")


def run_test():
    with grpc.insecure_channel("localhost:50050") as channel:
        stub = database_pb2_grpc.CoordinatorStub(channel)

        print('--- 1. Registering Table "users" ---')
        register_table(stub, "users", "username", "tweet_id")

        records = {
            "alice": database_pb2.Record(
                partition_key="alice",
                sort_key="1001",
                attributes={"email": "alice@example.com", "city": "London"},
            ),
            "bob": database_pb2.Record(
                partition_key="bob",
                sort_key="2002",
                attributes={"email": "bob@example.com", "city": "New York"},
            ),
            "charlie": database_pb2.Record(
                partition_key="charlie",
                sort_key="3003",
                attributes={"email": "charlie@example.com", "city": "Tokyo"},
            ),
            "dave": database_pb2.Record(
                partition_key="dave",
                sort_key="4004",
                attributes={"email": "dave@example.com", "city": "Sydney"},
            ),
            "eve": database_pb2.Record(
                partition_key="eve",
                sort_key="5005",
                attributes={"email": "eve@example.com", "city": "Paris"},
            ),
            "frank": database_pb2.Record(
                partition_key="frank",
                sort_key="6006",
                attributes={"email": "frank@example.com", "city": "Berlin"},
            ),
            "george": database_pb2.Record(
                partition_key="george",
                sort_key="7007",
                attributes={"email": "george@example.com", "city": "London"},
            ),
            "harry": database_pb2.Record(
                partition_key="harry",
                sort_key="8008",
                attributes={"email": "harry@example.com", "city": "New York"},
            ),
        }

        print("--- 2. Creating Records ---")
        for pk, record in records.items():
            create_record(stub, "users", record)
        print("")

        print("--- 2. Waiting for 4 seconds while replicas catch up ---")
        time.sleep(4)

        for pk, record in records.items():
            print(f'--- 3. Reading Record "{pk}" ---')
            read_record(stub, "users", pk, record.sort_key)

        for pk, record in records.items():
            print(f'--- 4. Checking Existence for "{pk}" (should be True) ---')
            exists_record(stub, "users", pk, record.sort_key)

        print('--- 5. Checking Existence for "marie" (should be False) ---')
        exists_record(stub, "users", "marie", "9009")
        print('--- 5. Checking Existence for "eliot" (should be False) ---')
        exists_record(stub, "users", "eliot", "10010")

        print("--- 6. Deleting Records ---")
        for pk, record in records.items():
            delete_record(stub, "users", pk, record.sort_key)

        print("--- 6. Waiting for 4 seconds while replicas catch up ---")
        time.sleep(4)

        print("--- 7. Reading Records (should be Not Found) ---")
        for pk, record in records.items():
            read_record(stub, "users", pk, record.sort_key)

        print("--- 8. Checking Existence for all records (should be False) ---")
        for pk, record in records.items():
            exists_record(stub, "users", pk, record.sort_key)


if __name__ == "__main__":
    run_test()
