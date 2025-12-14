
import unittest
from unittest.mock import MagicMock, patch
import bisect
import hashlib
import sys
import os

# Add parent dir to path to import coordinator
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'coordinator')))

# Mock grpc before importing coordinator
sys.modules['grpc'] = MagicMock()
sys.modules['database_pb2'] = MagicMock()
mock_grpc = MagicMock()
mock_grpc.CoordinatorServicer = object
sys.modules['database_pb2_grpc'] = mock_grpc
sys.modules['prometheus_client'] = MagicMock()
sys.modules['requests'] = MagicMock()

# Now import
import coordinator

class TestRebalancing(unittest.TestCase):
    def setUp(self):
        self.coord = coordinator.CoordinatorService()
        self.coord._shard_stubs = {}
        self.coord._get_shard_stub = MagicMock()
        
    def test_rebalance_logic(self):
        initial_node = "shard1"
        self.coord._shard_nodes.add(initial_node)
        self.coord._ring = []
        
        for i in range(10):
            h = coordinator._hash_key(f"{initial_node}#{i}")
            bisect.insort(self.coord._ring, (h, initial_node))
            
        print(f"Initial Ring Size: {len(self.coord._ring)}")
            
        mock_stub = MagicMock()
        mock_stub.TransferData.return_value = MagicMock(success=True, records_transferred=10)
        self.coord._get_shard_stub.return_value = mock_stub
        
        new_node = "shard2"
        
        coordinator.logging = MagicMock()
        
        self.coord._rebalance_and_join(new_node)
        
        if coordinator.logging.error.called:
            print("Errors logged:")
            for call in coordinator.logging.error.call_args_list:
                print(call)
                
        if not self.coord._get_shard_stub.called:
            print("get_shard_stub NOT called. No transfers identified?")
        else:
             print(f"get_shard_stub called {self.coord._get_shard_stub.call_count} times")
             
        self.assertTrue(mock_stub.TransferData.called)
        
        stats_pb2 = sys.modules['database_pb2']
        self.assertTrue(stats_pb2.TransferDataRequest.called)
        
        req_calls = stats_pb2.TransferDataRequest.call_args_list
        for call in req_calls:
            kwargs = call[1]
            self.assertEqual(kwargs['destination_address'], new_node)
            self.assertTrue(len(kwargs['ranges']) > 0)
            
        print(f"TransferDataRequest instantiated {len(req_calls)} times")
        
        self.assertIn(new_node, self.coord._shard_nodes)
        self.assertTrue(len(self.coord._ring) > 10) # Should be 20 now
        
if __name__ == '__main__':
    unittest.main()
