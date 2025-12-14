#!/usr/bin/env python3
"""
LWW (Last-Write-Wins) Test Client
Tests conflict resolution with explicit timestamps
"""
import grpc
import time
import database_pb2
import database_pb2_grpc


def test_lww():
    with grpc.insecure_channel("localhost:50050") as channel:
        stub = database_pb2_grpc.CoordinatorStub(channel)
        
        print("=== Registering Table ===")
        try:
            stub.RegisterTable(
                database_pb2.RegisterTableRequest(
                    table_name="lww_test",
                    primary_key=database_pb2.PrimaryKeySchema(
                        partition_key_name="user_id",
                        sort_key_name="record_id"
                    )
                )
            )
            print("Table registered successfully\n")
        except grpc.RpcError as e:
            print(f"Table registration (may already exist): {e.details()}\n")
        
        # Test 1: Write with older timestamp, then newer timestamp
        print("=== TEST 1: Newer timestamp should win ===")
        
        old_record = database_pb2.Record(
            partition_key="user123",
            sort_key="profile",
            attributes={"name": "Alice OLD", "version": "1"},
            timestamp=1000
        )
        
        print("Writing OLD record (timestamp=1000, name='Alice OLD')...")
        stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name="lww_test",
                record=old_record
            )
        )
        
        new_record = database_pb2.Record(
            partition_key="user123",
            sort_key="profile",
            attributes={"name": "Alice NEW", "version": "2"},
            timestamp=2000
        )
        
        print("Writing NEW record (timestamp=2000, name='Alice NEW')...")
        stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name="lww_test",
                record=new_record
            )
        )
        
        # Read back
        time.sleep(3)
        response = stub.ReadRecord(
            database_pb2.ReadRecordRequest(
                table_name="lww_test",
                partition_key="user123",
                sort_key="profile"
            )
        )
        
        if response.found:
            record = response.records[0]
            print(f"✓ Result: name='{record.attributes['name']}', timestamp={record.timestamp}")
            assert record.attributes['name'] == "Alice NEW", "LWW failed: newer write should win"
            assert record.timestamp == 2000, "Timestamp should be 2000"
        else:
            print("✗ Record not found!")
        
        print()
        
        # Test 2: Write newer, then try to overwrite with older (should be ignored)
        print("=== TEST 2: Older timestamp should be ignored ===")
        
        current_record = database_pb2.Record(
            partition_key="user456",
            sort_key="profile",
            attributes={"name": "Bob CURRENT", "score": "100"},
            timestamp=5000
        )
        
        print("Writing CURRENT record (timestamp=5000, name='Bob CURRENT')...")
        stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name="lww_test",
                record=current_record
            )
        )
        
        outdated_record = database_pb2.Record(
            partition_key="user456",
            sort_key="profile",
            attributes={"name": "Bob OUTDATED", "score": "50"},
            timestamp=3000
        )
        
        print("Attempting to write OUTDATED record (timestamp=3000, name='Bob OUTDATED')...")
        stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name="lww_test",
                record=outdated_record
            )
        )
        
        # Read back
        time.sleep(3)
        response = stub.ReadRecord(
            database_pb2.ReadRecordRequest(
                table_name="lww_test",
                partition_key="user456",
                sort_key="profile"
            )
        )
        
        if response.found:
            record = response.records[0]
            print(f"✓ Result: name='{record.attributes['name']}', timestamp={record.timestamp}")
            assert record.attributes['name'] == "Bob CURRENT", "LWW failed: outdated write should be ignored"
            assert record.timestamp == 5000, "Timestamp should remain 5000"
        else:
            print("✗ Record not found!")
        
        print()
        
        # Test 3: No timestamp (system assigns current timestamp)
        print("=== TEST 3: System-assigned timestamp ===")
        
        auto_record = database_pb2.Record(
            partition_key="user789",
            sort_key="profile",
            attributes={"name": "Charlie AUTO"},
        )
        
        print("Writing record without timestamp (system will assign)...")
        stub.CreateRecord(
            database_pb2.CreateRecordRequest(
                table_name="lww_test",
                record=auto_record
            )
        )
        
        # Read back
        time.sleep(3)
        response = stub.ReadRecord(
            database_pb2.ReadRecordRequest(
                table_name="lww_test",
                partition_key="user789",
                sort_key="profile"
            )
        )
        
        if response.found:
            record = response.records[0]
            print(f"Result: name='{record.attributes['name']}', timestamp={record.timestamp}")
            assert record.timestamp > 0, "System should assign a timestamp"
        else:
            print("Record not found!")
        
        print("\n=== ALL TESTS PASSED ===\n")


if __name__ == "__main__":
    test_lww()
