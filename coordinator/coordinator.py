import grpc
from concurrent import futures
import time
import os
import hashlib
import bisect
import threading
import logging

import json
import logging
import uuid
import database_pb2
import database_pb2_grpc

from prometheus_client import Histogram, Counter, start_http_server

REQUEST_LATENCY = Histogram("coordinator_request_latency_seconds", "Request latency", ["method"])
REQUEST_COUNT = Counter("coordinator_request_count_total", "Request count", ["method", "status"])

VIRTUAL_NODES = 10  # Number of virtual nodes per shard for better distribution


def _hash_key(key):
    return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest(), "big")


class CoordinatorService(database_pb2_grpc.CoordinatorServicer):
    def __init__(self):
        # { "table_name": PrimaryKeySchema }
        self._tables = {}

        # --- Sharding state ---

        # A sorted list of (hash, shard_address)
        # Actual hash ring.
        self._ring = []

        self._shard_nodes = set()

        # A dictionary to cache gRPC stubs (clients) for each shard
        # { "address": ShardStub }
        self._shard_stubs = {}

        # { "shard_id": [address1, address2, ...] }
        # Index 0 is Leader.
        self._shard_replicas = {}

        self._lock = threading.Lock()

        logging.log(logging.INFO, "Coordinator initialized in SHARDED mode.")

    def _get_shard_address(self, partition_key):
        """
        Gets the responsible shard address for a given partition key
        using the consistent hash ring.
        """
        if not self._ring:
            return None

        key_hash = _hash_key(partition_key)

        # Find the first node on the ring at or after the key's hash
        idx = bisect.bisect_right(self._ring, (key_hash,))

        if idx == len(self._ring):
            idx = 0

        _hash, address = self._ring[idx]
        return address

    def _get_shard_stub(self, address):
        with self._lock:
            if address not in self._shard_stubs:
                logging.log(
                    logging.INFO, f"Creating new gRPC channel/stub for {address}"
                )
                channel = grpc.insecure_channel(address)

                stub = database_pb2_grpc.ShardStub(channel)
                self._shard_stubs[address] = stub
            return self._shard_stubs[address]

    def _handle_leader_failure(self, failed_address):
        with self._lock:
            # Find which shard group this address belongs to
            target_shard_id = None
            for shard_id, replicas in self._shard_replicas.items():
                if replicas and replicas[0] == failed_address:
                    target_shard_id = shard_id
                    break

            if not target_shard_id:
                logging.log(
                    logging.ERROR,
                    f"Could not find shard group for failed address {failed_address}",
                )
                return None

            # Remove failed node
            if failed_address in self._shard_replicas[target_shard_id]:
                self._shard_replicas[target_shard_id].remove(failed_address)

            if failed_address in self._shard_nodes:
                self._shard_nodes.remove(failed_address)
                # Rebuild ring without failed node
                self._ring = [node for node in self._ring if node[1] != failed_address]

            if not self._shard_replicas[target_shard_id]:
                logging.log(
                    logging.ERROR, f"No replicas left for shard {target_shard_id}"
                )
                return None

            # Promote new leader (next in list)
            new_leader_address = self._shard_replicas[target_shard_id][0]
            logging.log(
                logging.INFO,
                f"Promoting {new_leader_address} to LEADER for {target_shard_id}",
            )

            # Add new leader to Hash Ring
            if new_leader_address not in self._shard_nodes:
                self._shard_nodes.add(new_leader_address)
                for i in range(VIRTUAL_NODES):
                    shard_node_id = f"{new_leader_address}#{i}"
                    shard_hash = _hash_key(shard_node_id)
                    bisect.insort(self._ring, (shard_hash, new_leader_address))

        # Call PromoteToLeader RPC on new leader (outside lock to avoid deadlock if RPC hangs)
        try:
            stub = self._get_shard_stub(new_leader_address)
            response = stub.PromoteToLeader(
                database_pb2.PromoteToLeaderRequest(shard_id=target_shard_id)
            )
            if response.success:
                logging.log(logging.INFO, f"Successfully promoted {new_leader_address}")
                return new_leader_address
            else:
                logging.log(
                    logging.ERROR,
                    f"Failed to promote {new_leader_address}: {response.message}",
                )
                return None
        except Exception as e:
            logging.log(logging.ERROR, f"RPC error promoting {new_leader_address}: {e}")
            return None

    def RegisterTable(self, request, context):
        logging.log(logging.INFO, f"Registering table: {request.table_name}")
        if request.table_name in self._tables:
            return database_pb2.RegisterTableResponse(
                success=False, message="Table already exists"
            )

        if not request.primary_key.partition_key_name:
            return database_pb2.RegisterTableResponse(
                success=False, message="Partition key name is required"
            )

        self._tables[request.table_name] = request.primary_key

        return database_pb2.RegisterTableResponse(
            success=True, message=f"Table {request.table_name} registered"
        )

    def _route_request(self, request, method_name, context):
        trace_id = str(uuid.uuid4())
        start_time = time.time()
        status = "success"
        
        logging.info(f"Received request {method_name} for {request.table_name}", extra={"trace_id": trace_id})

        if request.table_name not in self._tables:
            context.abort(grpc.StatusCode.NOT_FOUND, "Table not found")

        if hasattr(request, "record"):
            pk = request.record.partition_key
        else:
            pk = request.partition_key

        if not pk:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Partition key is required")

        # Retry loop for failover
        max_retries = 3
        for attempt in range(max_retries):
            address = None
            with self._lock:
                address = self._get_shard_address(pk)

            if not address:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "No shards are registered to handle this request",
                )

            logging.log(
                logging.INFO,
                f'Routing {method_name} for PK "{pk}" to shard at {address} (Attempt {attempt + 1})',
            )

            try:
                stub = self._get_shard_stub(address)
                grpc_method = getattr(stub, method_name)
                metadata = (("x-trace-id", trace_id),)
                return grpc_method(request, metadata=metadata)

            except grpc.RpcError as e:
                logging.log(
                    logging.ERROR, f"Error routing request to {address}: {e.details()}"
                )
                if e.code() != grpc.StatusCode.OK:
                     status = "error"

                # If it's a write operation and we have replicas, try failover
                if method_name in ["ShardCreateRecord", "ShardDeleteRecord"] and (
                    e.code() == grpc.StatusCode.UNAVAILABLE
                    or e.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                ):
                    logging.log(
                        logging.WARN, f"Leader {address} failed. Attempting failover..."
                    )
                    new_leader = self._handle_leader_failure(address)

                    if new_leader:
                        logging.log(
                            logging.INFO,
                            f"Failover successful. New leader: {new_leader}. Retrying...",
                        )
                        continue  # Retry with new leader
                    else:
                        logging.log(
                            logging.ERROR, "Failover failed. No replicas available."
                        )
                        context.abort(
                            grpc.StatusCode.UNAVAILABLE,
                            "Leader failed and no replicas available",
                        )

                context.abort(e.code(), f"Error from shard {address}: {e.details()}")
            except Exception as e:
                logging.log(logging.ERROR, f"Unhandled exception in routing: {e}")
                status = "error"
                context.abort(grpc.StatusCode.INTERNAL, "Coordinator internal error")
            finally:
                REQUEST_LATENCY.labels(method=method_name).observe(time.time() - start_time)
                REQUEST_COUNT.labels(method=method_name, status=status).inc()

    def CreateRecord(self, request, context):
        if not request.record.timestamp:
            request.record.timestamp = int(time.time() * 1000)
        return self._route_request(request, "ShardCreateRecord", context)

    def ReadRecord(self, request, context):
        # Load balance reads across replicas
        if request.table_name not in self._tables:
            context.abort(grpc.StatusCode.NOT_FOUND, "Table not found")

        pk = request.partition_key
        if not pk:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Partition key is required")

        leader_address = None
        with self._lock:
            leader_address = self._get_shard_address(pk)

        if not leader_address:
            context.abort(grpc.StatusCode.UNAVAILABLE, "No shards registered")

        # Find which shard group this leader belongs to
        target_address = leader_address
        with self._lock:
            for shard_id, replicas in self._shard_replicas.items():
                if replicas and replicas[0] == leader_address:
                    # Found the group. Pick a random replica for reading.
                    import random

                    target_address = random.choice(replicas)
                    break

        logging.log(
            logging.INFO,
            f'Routing ReadRecord for PK "{pk}" to replica at {target_address} (Leader: {leader_address})',
        )

        try:
            stub = self._get_shard_stub(target_address)
            return stub.ShardReadRecord(request)
        except grpc.RpcError as e:
            logging.log(
                logging.ERROR, f"Error reading from {target_address}: {e.details()}"
            )
            context.abort(e.code(), f"Error from shard {target_address}: {e.details()}")

    def DeleteRecord(self, request, context):
        return self._route_request(request, "ShardDeleteRecord", context)

    def ExistsRecord(self, request, context):
        return self._route_request(request, "ShardExistsRecord", context)

    def RegisterShard(self, request, context):
        logging.log(
            logging.INFO,
            f"Received registration from shard: {request.shard_id} at {request.address}",
        )

        is_leader = False
        with self._lock:
            if request.shard_id not in self._shard_replicas:
                self._shard_replicas[request.shard_id] = []

            if request.address not in self._shard_replicas[request.shard_id]:
                self._shard_replicas[request.shard_id].append(request.address)
                logging.log(
                    logging.INFO,
                    f"Added {request.address} to replica set for {request.shard_id}",
                )

            # First node in the list is the Leader
            if self._shard_replicas[request.shard_id][0] == request.address:
                is_leader = True

            # Only add Leader to the Hash Ring
            if is_leader:
                if request.address not in self._shard_nodes:
                    logging.log(
                        logging.INFO,
                        f"Adding LEADER {request.address} to the consistent hash ring...",
                    )
                    self._shard_nodes.add(request.address)

                    for i in range(VIRTUAL_NODES):
                        shard_node_id = f"{request.address}#{i}"
                        shard_hash = _hash_key(shard_node_id)
                        bisect.insort(self._ring, (shard_hash, request.address))

                    logging.log(logging.INFO, f"Ring size is now {len(self._ring)}")

        return database_pb2.RegisterShardResponse(
            success=True, message="Registered successfully", is_leader=is_leader
        )


def serve():
    start_http_server(8000)
    port = os.environ.get("PORT", "50050")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    logging.log(logging.INFO, f"Coordinator server started on port {port}")
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
