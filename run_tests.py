import time
import grpc
from raft_client import RaftClient
import json
import raft_pb2
import raft_pb2_grpc


def wait_for_leader_election(node_addresses, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        for node_id, address in node_addresses.items():
            log_file_path = f'/Users/vaibhavwali/Downloads/DSCD-main 2/logs_node_{node_id}/logs.txt'
            try:
                with open(log_file_path, 'r') as file:
                    lines = file.readlines()
                    for line in reversed(lines):
                        entry = json.loads(line.strip())
                        if entry['operation'] == 'NO-OP':
                            print(f"Leader election detected in {node_id} for term {entry['term']}")
                            return node_id, entry['term']
            except FileNotFoundError:
                # This can happen if the log file doesn't exist yet because the node hasn't started.
                continue
            except json.JSONDecodeError:
                # This can happen if the log file is not in a proper JSON format.
                continue
        time.sleep(1)
    raise TimeoutError("Leader election timed out")

def retrieve_logs(node_addresses):
    logs = {}
    for node_id in node_addresses.keys():
        log_file_path = f'/Users/vaibhavwali/Downloads/DSCD-main 2/logs_node_{node_id}/logs.txt'
        try:
            with open(log_file_path, 'r') as file:
                logs[node_id] = file.readlines()
        except Exception as e:
            print(f"Failed to retrieve logs from {node_id}: {e}")
    return logs

def verify_log_replication(logs):
    # Assuming 'logs' is a dictionary with node_id as keys and list of log entries as values
    first_node_log = next(iter(logs.values()))
    
    for node_id, node_log in logs.items():
        
        if node_log != first_node_log:
            print(f"Log replication failed: Logs of {node_id} differ from the first node")
            return False
    print("Log replication verified successfully")
    return True

# Example usage in the main function:
def main():
    node_addresses = {
        "node_0": "localhost:50051",
        "node_1": "localhost:50052",
        "node_2": "localhost:50053",
        "node_3": "localhost:50054",
        "node_4": "localhost:50055",
    }


    leader_node_id, term = wait_for_leader_election(node_addresses)
    print(f"Leader election complete: {leader_node_id} for term {term}")

    logs = retrieve_logs(node_addresses)

    print(logs)
    if verify_log_replication(logs):
        print("Log replication test passed.")
    else:
        print("Log replication test failed.")

if __name__ == "__main__":
    main()
