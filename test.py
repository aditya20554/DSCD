import sys
from raft_node import RaftNode

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)

    node_id = sys.argv[1]
    node_addresses = {
        "node_0": "localhost:50051",
        "node_1": "localhost:50052",
        "node_2": "localhost:50053",
        "node_3": "localhost:50054",
        "node_4": "localhost:50055",
    }

    if node_id not in node_addresses:
        print(f"Invalid node_id: {node_id}")
        sys.exit(1)

    node = RaftNode(node_id, node_addresses)
    node.run()
