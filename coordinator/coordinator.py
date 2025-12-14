import grpc
from concurrent import futures
import time
import os
import hashlib
import bisect
import threading
import logging
import json
import uuid
import requests
import subprocess
import database_pb2
import database_pb2_grpc
from prometheus_client import Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST

# --- Custom HTTP Server for Health & Metrics ---
from http.server import HTTPServer, BaseHTTPRequestHandler

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
        return # Squelch standard HTTP logging

def start_metrics_server(port):
    server = HTTPServer(('0.0.0.0', port), MetricsHealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info(f"Metrics/Health server started on port {port}")

# --- Metrics ---
REQUEST_LATENCY = Histogram("coordinator_request_latency_seconds", "Request latency", ["method"])
REQUEST_COUNT = Counter("coordinator_request_count_total", "Request count", ["method", "status"])

VIRTUAL_NODES = 10 

def _hash_key(key):
    return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest(), "big")

# --- Autoscaler ---
class Autoscaler(threading.Thread):
    def __init__(self, coordinator, check_interval=10, cooldown=30):
        super().__init__(daemon=True)
        self.coordinator = coordinator
        self.check_interval = check_interval
        self.cooldown = cooldown
        self.last_scale_time = 0
        self.prometheus_url = "http://prometheus:9090"
        self.latency_threshold = 0.5  # 500ms
        self.min_shards = 2
        self.max_shards = 6

    def run(self):
        logging.info("Autoscaler started.")
        # Give Prometheus some time to start
        time.sleep(15)
        
        while True:
            time.sleep(self.check_interval)
            
            if time.time() - self.last_scale_time < self.cooldown:
                continue

            try:
                # Get P99 Latency
                query = 'histogram_quantile(0.99, rate(coordinator_request_latency_seconds_bucket[1m]))'
                response = requests.get(f"{self.prometheus_url}/api/v1/query", params={'query': query})
                results = response.json()['data']['result']
                
                latency = 0
                if results:
                    val = results[0]['value'][1]
                    if val != 'NaN':
                        latency = float(val)

                # Count active shards
                with self.coordinator._lock:
                    current_shards = len(self.coordinator._shard_replicas)
                    
                logging.info(f"[Autoscaler] Current shards: {current_shards}, P99 Latency: {latency:.4f}s")

                if latency > self.latency_threshold and current_shards < self.max_shards:
                    logging.info("High latency detected. Scaling UP.")
                    self.scale(current_shards + 1)
                
                # Logic for scale down
                elif latency < 0.05 and current_shards > self.min_shards:
                    logging.info("Low latency detected. Scaling DOWN.")
                    # Downscale: Graceful removal via RPC
                    # Pick a node to remove
                    victim = None
                    with self.coordinator._lock:
                         # Get keys (shard ids) sorted?
                         sids = sorted(list(self.coordinator._shard_replicas.keys()))
                         if sids:
                             victim_id = sids[-1]
                             if self.coordinator._shard_replicas[victim_id]:
                                 victim = self.coordinator._shard_replicas[victim_id][0]
                    
                    if victim:
                        logging.info(f"Scaling DOWN: Eliminating {victim}")
                        self.coordinator._rebalance_and_leave(victim)
                        self.last_scale_time = time.time()

            except Exception as e:
                logging.error(f"Autoscaler error: {e}")

    def scale_up(self, replicas):
        try:
            cmd = ["docker-compose", "up", "-d", "--scale", f"shard={replicas}", "--no-build", "--no-recreate"]
            
            logging.info(f"Executing command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
            logging.info(f"Scaling UP successful: shard={replicas}")
            
            # Update last scale time
            self.last_scale_time = time.time()
            
        except Exception as e:
            logging.error(f"Scaling UP failed: {e}")


class CoordinatorService(database_pb2_grpc.CoordinatorServicer):
    def __init__(self):
        self._tables = {}
        self._ring = []
        self._shard_nodes = set()
        self._shard_stubs = {}
        self._shard_replicas = {}
        self._lock = threading.RLock()
        logging.info("Coordinator initialized in SHARDED mode.")
        
        self.autoscaler = Autoscaler(self)
        self.autoscaler.start()

    def _get_shard_address(self, partition_key):
        if not self._ring:
            return None
        key_hash = _hash_key(partition_key)
        idx = bisect.bisect_right(self._ring, (key_hash,))
        if idx == len(self._ring):
            idx = 0
        _hash, address = self._ring[idx]
        return address

    def _get_shard_stub(self, address):
        with self._lock:
            if address not in self._shard_stubs:
                channel = grpc.insecure_channel(address)
                stub = database_pb2_grpc.ShardStub(channel)
                self._shard_stubs[address] = stub
            return self._shard_stubs[address]
            
    def RegisterShard(self, request, context):
        logging.info(f"Received registration from shard: {request.shard_id} at {request.address}")
        is_leader = False
        
        # We need to handle rebalancing synchronously or async. 
        # If we do it sync, we block registration.
        # Let's do it sync for simplicity of state but with timeouts.
        
        with self._lock:
            if request.shard_id not in self._shard_replicas:
                self._shard_replicas[request.shard_id] = []
            
            if request.address not in self._shard_replicas[request.shard_id]:
                self._shard_replicas[request.shard_id].append(request.address)
            
            if self._shard_replicas[request.shard_id][0] == request.address:
                is_leader = True
            
            logging.info(f"Registered shard {request.shard_id}. Current shards keys: {list(self._shard_replicas.keys())}")
            
            if is_leader and request.address not in self._shard_nodes:
                # NEW LEADER JOINING -> REBALANCE
                # Run in background so we correct return response and let Shard know it is leader
                threading.Thread(target=self._rebalance_and_join, args=(request.address,), daemon=True).start()

        return database_pb2.RegisterShardResponse(
            success=True, message="Registered", is_leader=is_leader
        )

    def _rebalance_and_join(self, new_address):
        time.sleep(2)
        logging.info(f"Starting Rebalancing for new node {new_address}...")
        
        transfers = {} # { source_address: [Range, Range] }
        
        # Helper to find current owner of a hash
        def find_owner(h):
            if not self._ring: return None, None
            idx = bisect.bisect_right(self._ring, (h,))
            if idx == len(self._ring): idx = 0
            return self._ring[idx] # (hash, address)

        for i in range(VIRTUAL_NODES):
            shard_node_id = f"{new_address}#{i}"
            h = _hash_key(shard_node_id)
            
            # Find who currently owns 'h'. That node and its virtual predecessor define the range.
            # Actually, we are inserting 'h'. 
            # The range (Prev_Hash, h] is currently owned by Successor(h).
            # We want to move (Prev_Hash, h] FROM Successor TO NewNode.
            
            # Find Successor of h in CURRENT ring
            if not self._ring: continue
            
            idx = bisect.bisect_right(self._ring, (h,))
            if idx == len(self._ring): idx = 0
            
            curr_v_hash, curr_owner = self._ring[idx]
            
            if curr_owner == new_address: continue
            
            pred_idx = idx - 1 if idx > 0 else len(self._ring) - 1
            pred_hash, _ = self._ring[pred_idx]
            
            if curr_owner not in transfers:
                transfers[curr_owner] = []
            
            transfers[curr_owner].append(database_pb2.Range(start=str(pred_hash), end=str(h)))
         # 2. Execute Transfers
        for source, ranges in transfers.items():
            logging.info(f"Requesting transfer from {source} to {new_address} ({len(ranges)} ranges)")
            for r in ranges:
                 logging.info(f"  Range: start={r.start}, end={r.end}")
            try:
                stub = self._get_shard_stub(source)
                resp = stub.TransferData(database_pb2.TransferDataRequest(
                    destination_address=new_address,
                    ranges=ranges
                ))
                if resp.success:
                    logging.info(f"Transfer from {source} successful. Moved {resp.records_transferred} records.")
                else:
                    logging.error(f"Transfer from {source} failed.")
            except Exception as e:
                logging.error(f"RPC error during transfer from {source}: {e}")

        # 3. Add to Ring
        self._shard_nodes.add(new_address)
        for i in range(VIRTUAL_NODES):
            shard_node_id = f"{new_address}#{i}"
            h = _hash_key(shard_node_id)
            bisect.insort(self._ring, (h, new_address))
            
        logging.info(f"Joined {new_address} to ring. Ring size: {len(self._ring)}")

    def _rebalance_and_leave(self, leave_address):
        logging.info(f"Starting Rebalancing for LEAVING node {leave_address}...")
        
        # 1. Identify ranges stored on leave_address
        # 2. Transfer them to what WOULD be the owner if leave_address is gone.
        
        temp_ring = [node for node in self._ring if node[1] != leave_address]
        temp_ring.sort()
        
        transfers = {} # { destination: [Range] }
        
        for idx, (h, owner) in enumerate(self._ring):
            if owner == leave_address:
                pred_idx = idx - 1 if idx > 0 else len(self._ring) - 1
                pred_h, _ = self._ring[pred_idx]
                
                if not temp_ring:
                     logging.error("No nodes left to transfer data to!")
                     return

                t_idx = bisect.bisect_right(temp_ring, (h,))
                if t_idx == len(temp_ring): t_idx = 0
                _, dest_addr = temp_ring[t_idx]
                
                if dest_addr not in transfers: transfers[dest_addr] = []
                transfers[dest_addr].append(database_pb2.Range(start=str(pred_h), end=str(h)))
                
        # 2. Execute Transfers (FROM Leaving Node TO Destinations)
        for dest, ranges in transfers.items():
            logging.info(f"Requesting transfer from LEAVING {leave_address} to {dest}")
            try:
                stub = self._get_shard_stub(leave_address)
                resp = stub.TransferData(database_pb2.TransferDataRequest(
                    destination_address=dest,
                    ranges=ranges
                ))
                if resp.success:
                     logging.info(f"Transfer successful: {resp.records_transferred} records.")
                else:
                     logging.error(f"Transfer failed.")
            except Exception as e:
                logging.error(f"RPC error during transfer: {e}")
                
        # 3. Shutdown Node
        try:
             stub = self._get_shard_stub(leave_address)
             stub.Shutdown(database_pb2.google_dot_protobuf_dot_empty__pb2.Empty())
        except Exception as e:
             logging.warning(f"Shutdown RPC failed (node might be gone): {e}")

        self._shard_nodes.remove(leave_address)
        self._ring = temp_ring
        target_sid = None
        for sid, reps in self._shard_replicas.items():
            if leave_address in reps:
                reps.remove(leave_address)
                if not reps: target_sid = sid
                break
        if target_sid: 
            del self._shard_replicas[target_sid]
            
        logging.info(f"Node {leave_address} removed. Ring size: {len(self._ring)}")

    # Proxy methods
    def RegisterTable(self, request, context):
        logging.info(f"Registering table: {request.table_name}")
        self._tables[request.table_name] = request.primary_key
        return database_pb2.RegisterTableResponse(success=True, message="Registered")

    def CreateRecord(self, request, context):
        if not request.record.timestamp:
            request.record.timestamp = int(time.time() * 1000)
        return self._route_request(request, "ShardCreateRecord", context)

    def ReadRecord(self, request, context):
        pk = request.partition_key
        if not pk: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "PK required")
        
        with self._lock:
            leader = self._get_shard_address(pk)
        if not leader: context.abort(grpc.StatusCode.UNAVAILABLE, "No shards")
        
        target = leader
        with self._lock:
            if leader in self._shard_replicas:
                for sid, reps in self._shard_replicas.items():
                    if reps and reps[0] == leader:
                        import random
                        target = random.choice(reps)
                        break
        
        try:
            return self._get_shard_stub(target).ShardReadRecord(request)
        except grpc.RpcError as e:
            context.abort(e.code(), e.details())
            
    def DeleteRecord(self, request, context):
        return self._route_request(request, "ShardDeleteRecord", context)
        
    def ExistsRecord(self, request, context):
        return self._route_request(request, "ShardExistsRecord", context)

    def _route_request(self, request, method_name, context):
        trace_id = str(uuid.uuid4())
        start_time = time.time()
        status = "success"
        
        try:
            if hasattr(request, 'record') and request.HasField('record'):
                 pk = request.record.partition_key
            else:
                 pk = request.partition_key
            
            with self._lock:
                addr = self._get_shard_address(pk)
            
            if not addr:
                 context.abort(grpc.StatusCode.UNAVAILABLE, "No shards")
                 
            stub = self._get_shard_stub(addr)
            method = getattr(stub, method_name)
            metadata = (("x-trace-id", trace_id),)
            return method(request, metadata=metadata)
        except grpc.RpcError as e:
            status = "error"
            context.abort(e.code(), e.details())
        finally:
            REQUEST_LATENCY.labels(method=method_name).observe(time.time() - start_time)
            REQUEST_COUNT.labels(method=method_name, status=status).inc()

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "level": record.levelname,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt)
        })

def serve():
    port = os.environ.get("PORT", "50050")
    
    # Start Metrics Server
    start_metrics_server(8000)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info(f"Coordinator running on {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    serve()
