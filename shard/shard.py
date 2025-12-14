import grpc
from concurrent import futures
import time
import os
import threading
import logging

import boto3
import json
import uuid
import socket

import json
import database_pb2
import database_pb2_grpc

from prometheus_client import Histogram, Counter, start_http_server

SHARD_REQUEST_LATENCY = Histogram("shard_request_latency_seconds", "Shard Request latency", ["method"])
SHARD_REQUEST_COUNT = Counter("shard_request_count_total", "Shard Request count", ["method", "status"])

def get_trace_id(context):
    if context:
        for key, value in context.invocation_metadata():
            if key == "x-trace-id":
                return value
    return None

class ShardService(database_pb2_grpc.ShardServicer):
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.is_leader = False
        self.s3_bucket = os.environ.get("S3_BUCKET")
        self.s3_endpoint = os.environ.get("S3_ENDPOINT_URL")

        if self.s3_bucket:
            if self.s3_endpoint:
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.s3_endpoint,
                    aws_access_key_id="minioadmin",
                    aws_secret_access_key="minioadmin",
                )
                logging.log(
                    logging.INFO,
                    f"[{self.shard_id}] Using Custom S3 Endpoint: {self.s3_endpoint}",
                )
            else:
                self.s3_client = boto3.client("s3", region_name="eu-central-1")
        else:
            self.s3_client = None

        # { "table_name": { "partition_key": { "sort_key": Record } } }
        self.data = {}

        # Follower state
        self.last_applied_key = ""

        logging.log(
            logging.INFO,
            f"[{self.shard_id}] Shard initialized. Bucket: {self.s3_bucket}",
        )

    def _write_to_s3(self, table_name, record):
        if not self.s3_client or not self.s3_bucket:
            return

        # Create a unique, ordered key: shard_id/timestamp-uuid.json
        timestamp = int(time.time() * 1000)
        unique_id = str(uuid.uuid4())
        key = f"{self.shard_id}/{timestamp}-{unique_id}.json"

        payload = {
            "table_name": table_name,
            "record": {
                "partition_key": record.partition_key,
                "sort_key": record.sort_key,
                "attributes": dict(record.attributes),
                "timestamp": record.timestamp,
            },
        }

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket, Key=key, Body=json.dumps(payload)
            )
            logging.log(
                logging.INFO, f"[{self.shard_id}] Wrote transaction log to S3: {key}"
            )
        except Exception as e:
            logging.log(logging.ERROR, f"[{self.shard_id}] Failed to write to S3: {e}")

    def ShardCreateRecord(self, request, context):
        start_time = time.time()
        status = "success"
        try:
            return self._shard_create_record_impl(request, context)
        except Exception:
            status = "error"
            raise
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardCreateRecord").observe(time.time() - start_time)
            SHARD_REQUEST_COUNT.labels(method="ShardCreateRecord", status=status).inc()

    def _shard_create_record_impl(self, request, context):
        if not self.is_leader:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Not the leader")

        table_name = request.table_name
        record = request.record
        pk = record.partition_key
        sk = record.sort_key

        pk = record.partition_key
        sk = record.sort_key

        logging.info(
            f"[{self.shard_id}] Creating record for table: {table_name}",
            extra={"trace_id": get_trace_id(context)}
        )

        if table_name not in self.data:
            self.data[table_name] = {}

        if pk not in self.data[table_name]:
            self.data[table_name][pk] = {}

        if pk in self.data[table_name] and sk in self.data[table_name][pk]:
             existing_record = self.data[table_name][pk][sk]
             if record.timestamp < existing_record.timestamp:
                 logging.log(logging.INFO, f"[{self.shard_id}] Ignoring outdated write for {pk}/{sk} (Old: {existing_record.timestamp}, New: {record.timestamp})")
                 return database_pb2.CreateRecordResponse(success=True, message="Ignored outdated write")

        self.data[table_name][pk][sk] = record
        logging.log(
            logging.INFO, f"[{self.shard_id}] Data stored locally (partition {pk})"
        )

        # Replicate transaction log to S3
        self._write_to_s3(table_name, record)

        return database_pb2.CreateRecordResponse(
            success=True, message="Record created on shard"
        )

    def ShardReadRecord(self, request, context):
        start_time = time.time()
        status = "success"
        try:
            return self._shard_read_record_impl(request, context)
        except Exception:
            status = "error"
            raise
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardReadRecord").observe(time.time() - start_time)
            SHARD_REQUEST_COUNT.labels(method="ShardReadRecord", status=status).inc()

    def _shard_read_record_impl(self, request, context):
        logging.log(
            logging.INFO,
            f"[{self.shard_id}] Reading record from table: {request.table_name}",
        )
        table_name = request.table_name
        pk = request.partition_key
        sk = request.sort_key

        if table_name not in self.data:
            return database_pb2.ReadRecordResponse(found=False)

        if pk not in self.data[table_name]:
            return database_pb2.ReadRecordResponse(found=False)

        partition = self.data[table_name][pk]

        if sk:
            if sk not in partition:
                return database_pb2.ReadRecordResponse(found=False)
            return database_pb2.ReadRecordResponse(found=True, records=[partition[sk]])
        else:
            return database_pb2.ReadRecordResponse(
                found=True, records=list(partition.values())
            )

    def ShardDeleteRecord(self, request, context):
        start_time = time.time()
        status = "success"
        try:
            return self._shard_delete_record_impl(request, context)
        except Exception:
            status = "error"
            raise
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardDeleteRecord").observe(time.time() - start_time)
            SHARD_REQUEST_COUNT.labels(method="ShardDeleteRecord", status=status).inc()

    def _shard_delete_record_impl(self, request, context):
        # Replicate deletion to S3
        if self.is_leader and self.s3_client and self.s3_bucket:
            timestamp = int(time.time() * 1000)
            unique_id = str(uuid.uuid4())
            key = f"{self.shard_id}/{timestamp}-{unique_id}.json"

            payload = {
                "operation": "DELETE",
                "table_name": request.table_name,
                "partition_key": request.partition_key,
                "sort_key": request.sort_key,
            }

            try:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket, Key=key, Body=json.dumps(payload)
                )
                logging.log(
                    logging.INFO,
                    f"[{self.shard_id}] Wrote delete transaction log to S3: {key}",
                )
            except Exception as e:
                logging.log(
                    logging.ERROR,
                    f"[{self.shard_id}] Failed to write delete to S3: {e}",
                )
        if not self.is_leader:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "Not the leader")

        logging.log(
            logging.INFO,
            f"[{self.shard_id}] Deleting record from table: {request.table_name}",
        )
        table_name = request.table_name
        pk = request.partition_key
        sk = request.sort_key

        if (
            table_name not in self.data
            or pk not in self.data[table_name]
            or sk not in self.data[table_name][pk]
        ):
            logging.log(
                logging.INFO, f"[{self.shard_id}] Record not found for deletion."
            )
            return database_pb2.DeleteRecordResponse(success=False)

        del self.data[table_name][pk][sk]

        if not self.data[table_name][pk]:
            del self.data[table_name][pk]

        logging.log(logging.INFO, f"[{self.shard_id}] Record deleted.")
        return database_pb2.DeleteRecordResponse(success=True)

    def ShardExistsRecord(self, request, context):
        start_time = time.time()
        status = "success"
        try:
            return self._shard_exists_record_impl(request, context)
        except Exception:
            status = "error"
            raise
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardExistsRecord").observe(time.time() - start_time)
            SHARD_REQUEST_COUNT.labels(method="ShardExistsRecord", status=status).inc()

    def _shard_exists_record_impl(self, request, context):
        logging.log(
            logging.INFO,
            f"[{self.shard_id}] Checking existence for table: {request.table_name}",
        )
        table_name = request.table_name
        pk = request.partition_key
        sk = request.sort_key

        exists = (
            table_name in self.data
            and pk in self.data[table_name]
            and sk in self.data[table_name][pk]
        )

        return database_pb2.ExistsRecordResponse(exists=exists)

    def PromoteToLeader(self, request, context):
        logging.log(
            logging.INFO,
            f"[{self.shard_id}] Received promotion request. Becoming LEADER.",
        )
        self.is_leader = True
        return database_pb2.PromoteToLeaderResponse(
            success=True, message="Promoted to leader"
        )


def follower_loop(shard_service):
    logging.log(logging.INFO, f"[{shard_service.shard_id}] Starting follower loop...")
    while True:
        if shard_service.is_leader:
            time.sleep(5)
            continue

        if not shard_service.s3_client or not shard_service.s3_bucket:
            time.sleep(5)
            continue

        try:
            # List objects after the last applied key
            list_kwargs = {
                "Bucket": shard_service.s3_bucket,
                "Prefix": f"{shard_service.shard_id}/",
            }
            if shard_service.last_applied_key:
                list_kwargs["StartAfter"] = shard_service.last_applied_key

            response = shard_service.s3_client.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                # Sort by Key to ensure order
                sorted_contents = sorted(response["Contents"], key=lambda x: x["Key"])

                for obj in sorted_contents:
                    key = obj["Key"]
                    logging.log(
                        logging.INFO, f"[{shard_service.shard_id}] Applying log: {key}"
                    )

                    obj_body = shard_service.s3_client.get_object(
                        Bucket=shard_service.s3_bucket, Key=key
                    )
                    payload = json.loads(obj_body["Body"].read().decode("utf-8"))

                    table_name = payload["table_name"]
                    operation = payload.get("operation", "CREATE")

                    if operation == "DELETE":
                        pk = payload["partition_key"]
                        sk = payload["sort_key"]

                        if (
                            table_name in shard_service.data
                            and pk in shard_service.data[table_name]
                            and sk in shard_service.data[table_name][pk]
                        ):
                            del shard_service.data[table_name][pk][sk]

                            if not shard_service.data[table_name][pk]:
                                del shard_service.data[table_name][pk]
                    else:
                        record_data = payload["record"]

                        # Reconstruct Record object
                        timestamp = record_data.get("timestamp", 0)
                        record = database_pb2.Record(
                            partition_key=record_data["partition_key"],
                            sort_key=record_data["sort_key"],
                            attributes=record_data["attributes"],
                            timestamp=timestamp
                        )

                        # Apply to local state
                        if table_name not in shard_service.data:
                            shard_service.data[table_name] = {}

                        pk = record.partition_key
                        sk = record.sort_key

                        if pk not in shard_service.data[table_name]:
                            shard_service.data[table_name][pk] = {}

                        # LWW Check for Follower
                        should_update = True
                        if pk in shard_service.data[table_name] and sk in shard_service.data[table_name][pk]:
                             existing_record = shard_service.data[table_name][pk][sk]
                             if record.timestamp < existing_record.timestamp:
                                 should_update = False
                                 logging.log(logging.INFO, f"[{shard_service.shard_id}] Follower ignoring outdated log for {pk}/{sk}")

                        if should_update:
                            shard_service.data[table_name][pk][sk] = record

                    shard_service.last_applied_key = key

        except Exception as e:
            logging.log(
                logging.ERROR, f"[{shard_service.shard_id}] Error in follower loop: {e}"
            )

        time.sleep(2)


def register_with_coordinator(
    shard_id, shard_address, coordinator_address, shard_service
):
    while True:
        try:
            with grpc.insecure_channel(coordinator_address) as channel:
                stub = database_pb2_grpc.CoordinatorStub(channel)
                logging.log(
                    logging.INFO,
                    f"[{shard_id}] Attempting to register with coordinator at {coordinator_address}...",
                )
                response = stub.RegisterShard(
                    database_pb2.RegisterShardRequest(
                        shard_id=shard_id, address=shard_address
                    )
                )
                if response.success:
                    logging.log(
                        logging.INFO,
                        f"[{shard_id}] Successfully registered. Leader: {response.is_leader}",
                    )
                    shard_service.is_leader = response.is_leader

                    if not response.is_leader:
                        # Start follower loop
                        threading.Thread(
                            target=follower_loop, args=(shard_service,), daemon=True
                        ).start()
                    return
                else:
                    logging.log(
                        logging.INFO,
                        f"[{shard_id}] Coordinator rejected registration: {response.message}. Retrying in 5s...",
                    )
        except grpc.RpcError as e:
            logging.log(
                logging.WARN,
                f"[{shard_id}] Failed to connect to coordinator: {e.details()}. Retrying in 5s...",
            )

        time.sleep(5)


def serve():
    port = os.environ.get("PORT", "50051")
    shard_id = os.environ.get("SHARD_ID", "unknown-shard")
    coordinator_address = os.environ.get("COORDINATOR_ADDRESS", "coordinator:50050")

    # The address this shard will be reachable at *from the coordinator*
    # Use the container's IP address to ensure uniqueness and reachability
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    shard_address = f"{ip_address}:{port}"

    # We keep shard_id as the logical ID (e.g., "shard-1") so the coordinator knows they belong to the same group.
    shard_service = ShardService(shard_id)

    reg_thread = threading.Thread(
        target=register_with_coordinator,
        args=(shard_id, shard_address, coordinator_address, shard_service),
        daemon=True,
    )
    reg_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_ShardServicer_to_server(shard_service, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    metrics_port = int(port) + 1
    start_http_server(metrics_port)
    logging.log(logging.INFO, f"[{shard_id}] Shard server started on port {port}")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "level": record.levelname,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
            "trace_id": getattr(record, "trace_id", None)
        }
        return json.dumps(log_record)

if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    serve()
