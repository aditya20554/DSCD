from raft_client import RaftClient
import time

def test_raft_client():
    # Define the addresses of the nodes in the cluster
    node_addresses = {
        'node_1': 'localhost:50051',
        'node_2': 'localhost:50052',
        'node_3': 'localhost:50053',
        'node_4': 'localhost:50054',
        'node_5': 'localhost:50055'
    }

    # Create a RaftClient instance
    client = RaftClient(node_addresses)

    # Wait for a leader to be elected and a NO-OP entry to be appended
    time.sleep(5)  # Adjust this based on your implementation

    # Perform SET and GET requests
    client.set('key1', 'value1')
    assert client.get('key1') == 'value1'

    client.set('key2', 'value2')
    assert client.get('key2') == 'value2'

    client.set('key3', 'value3')
    assert client.get('key3') == 'value3'

    # Add more test cases as needed

if __name__ == '__main__':
    test_raft_client()
