import grpc
import json
from raft_pb2_grpc import RaftServiceStub
from raft_pb2 import ServeClientArgs

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.current_leader_id = None
        self.stubs = {node_id: RaftServiceStub(grpc.insecure_channel(address))
                      for node_id, address in node_addresses.items()}
    
    def send_request(self, request):
        for _ in range(len(self.node_addresses)):  # Retry up to the number of nodes
            stub = self.get_leader_stub()
            if stub is None:
                print("No leader found.")
                return None, "No leader found."
            
            try:
                response = stub.ServeClient(ServeClientArgs(Request=json.dumps(request)))
                if response.Success:
                    return response.Data, None  # Return data and no error
                else:
                    # Update the current leader ID and retry
                    self.current_leader_id = response.LeaderID
            except grpc.RpcError as e:
                # Handle RPC error and retry
                print(f"RPC failed to leader {self.current_leader_id}: {e}")
                self.current_leader_id = None
        return None, "Request failed after retries"

    def get_leader_stub(self):
        if self.current_leader_id is not None:
            return self.stubs[self.current_leader_id]
        
        # If the leader is not known, try to find it using the existing stubs
        for node_id, stub in self.stubs.items():
            try:
                # Use ServeClient RPC with a simple request to force a response
                # that includes the LeaderID
                response = stub.ServeClient(ServeClientArgs(Request=json.dumps({'operation': 'GET', 'key': 'dummy'})))
                if response.Success:
                    # This means the node we called is the leader
                    self.current_leader_id = node_id
                    return stub
                elif response.LeaderID:
                    # If LeaderID is provided, update the current leader ID and get the stub
                    self.current_leader_id = response.LeaderID
                    return self.stubs.get(self.current_leader_id)
            except grpc.RpcError as e:
                # Log error and try the next node
                print(f"RPC error when trying to find the leader: {e}")

        # If we reach here, we couldn't find the leader
        print("Failed to find the leader.")
        return None
    
    def set(self, key, value):
        request = {'operation': 'SET', 'key': key, 'value': value}
        data, error = self.send_request(request)
        if error:
            print(f"SET request failed: {error}")
        return data

    def get(self, key):
        request = {'operation': 'GET', 'key': key}
        data, error = self.send_request(request)
        if error:
            print(f"GET request failed: {error}")
        return data


