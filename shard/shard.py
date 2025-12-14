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
import hashlib
import database_pb2
import database_pb2_grpc
from prometheus_client import Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST
from http.server import HTTPServer, BaseHTTPRequestHandler

# --- Metrics ---
SHARD_REQUEST_LATENCY = Histogram("shard_request_latency_seconds", "Shard Request latency", ["method"])
SHARD_REQUEST_COUNT = Counter("shard_request_count_total", "Shard Request count", ["method", "status"])

# --- Helper ---
def _hash_key(key):
    return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest(), "big")

# --- HTTP Server ---
class MetricsHealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        elif self.path == '/version':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"1.0.0")
        elif self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        return

def start_metrics_server(port):
    server = HTTPServer(('0.0.0.0', port), MetricsHealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info(f"Metrics server started on port {port}")

class ShardService(database_pb2_grpc.ShardServicer):
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.is_leader = False
        self.data = {} # {table: {pk: {sk: Record}}}
        self.last_applied_key = ""
        
        # S3 Setup
        self.s3_bucket = os.environ.get("S3_BUCKET")
        self.s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
        self.s3_client = None
        
        if self.s3_bucket:
            if self.s3_endpoint:
                self.s3_client = boto3.client(
                    "s3", endpoint_url=self.s3_endpoint,
                    aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin"
                )
            else:
                self.s3_client = boto3.client("s3", region_name="eu-central-1")
        
        logging.info(f"[{shard_id}] Shard initialized.")

    def _write_to_s3(self, table_name, record=None, delete_key=None):
        if not self.s3_client or not self.s3_bucket: return
        
        timestamp = int(time.time() * 1000)
        unique_id = str(uuid.uuid4())
        key = f"{self.shard_id}/{timestamp}-{unique_id}.json"
        
        if record:
            payload = {
                "operation": "CREATE",
                "table_name": table_name,
                "record": {
                    "partition_key": record.partition_key,
                    "sort_key": record.sort_key,
                    "attributes": dict(record.attributes),
                    "timestamp": record.timestamp,
                }
            }
        elif delete_key:
            payload = {
                "operation": "DELETE",
                "table_name": table_name,
                "partition_key": delete_key[0],
                "sort_key": delete_key[1]
            }
            
        try:
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=key, Body=json.dumps(payload))
        except Exception as e:
            logging.error(f"S3 Write failed: {e}")

    # --- RPC Implementations ---
    def ShardCreateRecord(self, request, context):
        start = time.time()
        try:
            if not self.is_leader: context.abort(grpc.StatusCode.PERMISSION_DENIED, "Not leader")
            
            table = request.table_name
            rec = request.record
            pk = rec.partition_key
            sk = rec.sort_key
            
            if table not in self.data: self.data[table] = {}
            if pk not in self.data[table]: self.data[table][pk] = {}
            
            # LWW
            if pk in self.data[table] and sk in self.data[table][pk]:
                curr = self.data[table][pk][sk]
                if rec.timestamp < curr.timestamp:
                    return database_pb2.CreateRecordResponse(success=True, message="Ignored outdated")
            
            self.data[table][pk][sk] = rec
            self._write_to_s3(table, record=rec)
            
            return database_pb2.CreateRecordResponse(success=True, message="Created")
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardCreateRecord").observe(time.time() - start)
            
    def ShardReadRecord(self, request, context):
        start = time.time()
        try:
            table = request.table_name
            pk = request.partition_key
            sk = request.sort_key
            
            if table not in self.data or pk not in self.data[table]:
                return database_pb2.ReadRecordResponse(found=False)
                
            partition = self.data[table][pk]
            if sk:
                if sk in partition:
                    return database_pb2.ReadRecordResponse(found=True, records=[partition[sk]])
                return database_pb2.ReadRecordResponse(found=False)
            else:
                return database_pb2.ReadRecordResponse(found=True, records=list(partition.values()))
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardReadRecord").observe(time.time() - start)

    def ShardDeleteRecord(self, request, context):
        start = time.time()
        try:
            if not self.is_leader: context.abort(grpc.StatusCode.PERMISSION_DENIED, "Not leader")
            
            table = request.table_name
            pk = request.partition_key
            sk = request.sort_key
            
            if table in self.data and pk in self.data[table] and sk in self.data[table][pk]:
                del self.data[table][pk][sk]
                if not self.data[table][pk]: del self.data[table][pk]
                self._write_to_s3(table, delete_key=(pk, sk))
                return database_pb2.DeleteRecordResponse(success=True)
            return database_pb2.DeleteRecordResponse(success=False)
        finally:
            SHARD_REQUEST_LATENCY.labels(method="ShardDeleteRecord").observe(time.time() - start)

    def ShardExistsRecord(self, request, context):
        start = time.time()
        try:
            exists = False
            if request.table_name in self.data:
                if request.partition_key in self.data[request.table_name]:
                    if request.sort_key in self.data[request.table_name][request.partition_key]:
                        exists = True
            return database_pb2.ExistsRecordResponse(exists=exists)
        finally:
             SHARD_REQUEST_LATENCY.labels(method="ShardExistsRecord").observe(time.time() - start)

    def PromoteToLeader(self, request, context):
        logging.info(f"[{self.shard_id}] Promoted to LEADER.")
        self.is_leader = True
        return database_pb2.PromoteToLeaderResponse(success=True)

    def TransferData(self, request, context):
        start = time.time()
        count = 0
        try:
            target = request.destination_address
            logging.info(f"[{self.shard_id}] Starting TransferData to {target}")
            
            # Connect to destination
            channel = grpc.insecure_channel(target)
            stub = database_pb2_grpc.ShardStub(channel)
            
            # Identify records to move
            for table_name, partition in self.data.items():
                for pk, records in partition.items():
                    h = _hash_key(pk)
                    should_move = False
                    
                    for r in request.ranges:
                        # Range is (start, end]
                        r_start = int(r.start)
                        r_end = int(r.end)
                        
                        if r_start < r_end:
                            if r_start < h <= r_end: should_move = True
                        else: # Wrap around
                            if h > r_start or h <= r_end: should_move = True
                    
                        if should_move: break
                    
                    if should_move:
                        # Send all records for this PK
                        for sk, record in records.items():
                            # We use ShardCreateRecord on target
                            # Note: Target needs to be LEADER for ShardCreateRecord
                            # But in rebalancing, the new node IS the leader of its own group (it's a new shard).
                            try:
                                stub.ShardCreateRecord(database_pb2.CreateRecordRequest(
                                    table_name=table_name,
                                    record=record
                                ))
                                count += 1
                                logging.info(f"Transferred {pk}/{sk} to {target}")
                            except Exception as e:
                                logging.error(f"Failed to transfer {pk}/{sk}: {e}")
            
            return database_pb2.TransferDataResponse(success=True, records_transferred=count)
        except Exception as e:
            logging.error(f"TransferData failed: {e}")
            return database_pb2.TransferDataResponse(success=False)
        finally:
            SHARD_REQUEST_LATENCY.labels(method="TransferData").observe(time.time() - start)

    def Shutdown(self, request, context):
        logging.info(f"[{self.shard_id}] Received SHUTDOWN command. Exiting...")
        # Start a thread to exit so we can return response first
        def exit_process():
            time.sleep(1) # Allow response to be sent
            os._exit(0)
            
        threading.Thread(target=exit_process, daemon=True).start()
        return database_pb2.google_dot_protobuf_dot_empty__pb2.Empty()


def follower_loop(shard_service):
    logging.info(f"[{shard_service.shard_id}] Starting follower loop...")
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
                    logging.info(f"[{shard_service.shard_id}] Applying log: {key}")

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
                        timestamp = record_data.get("timestamp", 0)
                        record = database_pb2.Record(
                            partition_key=record_data["partition_key"],
                            sort_key=record_data["sort_key"],
                            attributes=record_data["attributes"],
                            timestamp=timestamp
                        )
                        
                        if table_name not in shard_service.data: shard_service.data[table_name] = {}
                        if record.partition_key not in shard_service.data[table_name]: shard_service.data[table_name][record.partition_key] = {}
                        
                        shard_service.data[table_name][record.partition_key][record.sort_key] = record

                    shard_service.last_applied_key = key

        except Exception as e:
            logging.error(f"[{shard_service.shard_id}] Error in follower loop: {e}")

        time.sleep(2)

def register_loop(shard_id, address, coord_addr, service):
    while True:
        try:
            channel = grpc.insecure_channel(coord_addr)
            stub = database_pb2_grpc.CoordinatorStub(channel)
            resp = stub.RegisterShard(database_pb2.RegisterShardRequest(shard_id=shard_id, address=address))
            if resp.success:
                logging.info(f"Registered (Leader={resp.is_leader})")
                service.is_leader = resp.is_leader
                
                if not resp.is_leader:
                     threading.Thread(target=follower_loop, args=(service,), daemon=True).start()
                return
        except Exception as e:
            logging.warning(f"Register failed: {e}")
        time.sleep(5)

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({"level": record.levelname, "message": record.getMessage()})

def serve():
    port = os.environ.get("PORT", "50051")
    # Dynamic ID
    if "SHARD_ID" in os.environ:
        shard_id = os.environ["SHARD_ID"]
    else:
        shard_id = f"shard-{socket.gethostname()}"
        
    coordinator_address = os.environ.get("COORDINATOR_ADDRESS", "coordinator:50050")
    
    # Address setup
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    # If using scale, we must use IP.
    shard_address = f"{ip_address}:{port}"

    service = ShardService(shard_id)
    
    # Start Metrics
    start_metrics_server(int(port) + 1) # e.g. 50052

    threading.Thread(target=register_loop, args=(shard_id, shard_address, coordinator_address, service), daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_ShardServicer_to_server(service, server)
    server.add_insecure_port(f"[::]:{port}")
    logging.info(f"Shard {shard_id} listening on {port}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    serve()
