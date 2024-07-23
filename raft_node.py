import json
import random
import threading
import time
import os
import logging
from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc


class RaftNode:
    def __init__(self, node_id, node_addresses):

        self.node_id = node_id
        self.node_addresses = node_addresses
        self.state = 'follower'

        self.votes_received = {}
        self.total_nodes = 0

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.lastLogTerm = 0
        self.lastLogIndex = 0
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Volatile state on leaders
        self.next_index = {}
        self.match_index = {}
        
        # Timers and networking
        self.election_timeout = self.reset_election_timer()
        self.heartbeat_interval = 1  

        self.lease_duration = 5  # in seconds
        self.lease_expiration = 0

        # Create the directory for node logs if it doesn't exist
        self.node_dir = f'logs_node_{node_id}'
        os.makedirs(self.node_dir, exist_ok=True)

        # Initialize logger with dump file
        self.logger = logging.getLogger(f'Node{node_id}')
        file_handler = logging.FileHandler(os.path.join(self.node_dir, 'dump.txt'))
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.INFO)

        # File paths for logs and metadata
        self.log_file_path = os.path.join(self.node_dir, 'logs.txt')
        self.metadata_file_path = os.path.join(self.node_dir, 'metadata.txt')
        self.dump_file_path = os.path.join(self.node_dir, 'dump.txt')
        # Node state
        self.node_id = node_id
        self.node_addresses = node_addresses
        self.state = 'follower'
        
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        # Initialize state_lock
        self.state_lock = threading.Lock()

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Volatile state on leaders
        self.next_index = {}
        self.match_index = {}
        
        self.logger = logging.getLogger(f'Node{node_id}')
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(f'logs_node_{node_id}/dump.txt')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.logger.addHandler(file_handler)

        self.node_dir = f'logs_node_{node_id}'
        os.makedirs(self.node_dir, exist_ok=True)

        self.log_file_path = os.path.join(self.node_dir, 'logs.txt')
        self.metadata_file_path = os.path.join(self.node_dir, 'metadata.txt')
        self.dump_file_path = os.path.join(self.node_dir, 'dump.txt')
        # Initialize logger with dump file
        self.logger = logging.getLogger(f'Node{node_id}')
        file_handler = logging.FileHandler(self.dump_file_path)
        
        self.initialize_grpc_clients()

    grpc_clients = {}

    def initialize_grpc_clients(self):
        for peer_id, address in self.node_addresses.items():
            if peer_id != self.node_id and peer_id not in self.grpc_clients:
                self.grpc_clients[peer_id] = grpc.insecure_channel(address)
            
    def send_heartbeat(self):
        print("Sending Heartbeats")
        for peer_id, client in self.grpc_clients.items():
            request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=len(self.log) - 1,
                prevLogTerm=self.log[-1]['term'] if self.log else 0,
                entries=[],  # Empty for heartbeat
                leaderCommit=self.commit_index
            )
            response = client.AppendEntries(request)


    def log_info(self, message):
        """
        Logs an informational message to both the console and the dump file.
        """
        print(message)
        self.logger.info(message)

        
    def save_state_to_file(self):
        # Save term and voted_for to metadata.txt
        with open(f'logs_node_{self.node_id}/metadata.txt', 'w') as file:
            json.dump({
                'currentTerm': self.current_term,
                'votedFor': self.voted_for
            }, file)

    def load_state_from_file(self):
        try:
            # Load term and voted_for from metadata.txt
            with open(f'logs_node_{self.node_id}/metadata.txt', 'r') as file:
                state = json.load(file)
                self.current_term = state['currentTerm']
                self.voted_for = state['votedFor']
        except FileNotFoundError:
            print("Metadata file not found")

    def FetchLogs(self, request, context):
        return raft_pb2.FetchLogsResponse(entries=self.log)

    def append_entry_to_log(self, operation, key=None, value=None):
        # Construct a new log entry
        new_entry = {
            'term': self.current_term,
            'operation': operation,
            'key': key,
            'value': value
        }
        # Append to the log list and persist to disk
        self.log.append(new_entry)
        self.write_to_log_file(new_entry)

    def write_to_log_file(self, entry):
        # Append a single log entry to logs.txt
        with open(f'logs_node_{self.node_id}/logs.txt', 'a') as file:
            file.write(f'{json.dumps(entry)}\n')

    def set_key_value(self, key, value):
        # with self.state_lock:
            if self.state != 'leader':
                return False  # Only the leader can initiate log replication

            # Append the new entry to the leader's log
            new_entry = {
                'term': self.current_term,
                'operation': 'SET',
                'key': key,
                'value': value
            }
            self.log.append(new_entry)
            self.write_to_log_file(new_entry)

            # Replicate the new entry to all followers
            self.replicate_logs()

            return True  # Assume success for simplicity; in practice, you would wait for acknowledgments

    def get_key_value(self, key):
        # Retrieve the latest committed value for a given key
        for entry in reversed(self.log):
            if entry.get('operation') == 'SET' and entry.get('key') == key:
                return entry.get('value', '')
        return ''  # Default to empty string if key not found

    def commit_entry(self, index):
    # Assuming index is valid and entry exists
        if index >= len(self.log) or index < 0:
            self.log_info(f"Attempted to commit an entry at invalid index {index}.")
            return False

        # Check if the entry is already committed
        if index <= self.commit_index:
            self.log_info(f"Attempted to commit an entry at index {index} which is already committed.")
            return False

        # Now, ensure that this entry is replicated on a majority of nodes before committing
        count = 1  # Assuming the leader itself has the entry, thus starting count at 1
        for node_id, match_index in self.match_index.items():
            if match_index >= index:
                count += 1
        if count > len(self.node_addresses) // 2:
            entry = self.log[index]
            self.commit_index = index  # Update the commit index

            # Commit logic here (could involve applying to a state machine)
            self.apply_entry_to_state_machine(entry)

            # After committing, log this event in the dump file and return True to indicate success
            self.log_info(f"Committed entry at index {index}: {json.dumps(entry)}")
            return True
        else:
            self.log_info(f"Attempted to commit an entry at index {index} without a majority.")
            return False

    

    def append_no_op_entry(self):
        self.append_entry_to_log('NO-OP')
        # Assume replication and commit logic will follow

    
    def start_election(self):
            self.log_info(f"Time : {time.time()} Node {self.node_id} election timer timed out, Starting election.")
        # with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id  # Vote for self
            self.votes_received = {self.node_id}  # Keep track of votes received
            self.election_timeout = self.reset_election_timer()
            self.max_old_leader_lease = 0  # Reset the maximum old leader lease duration

            # Send RequestVote RPCs to all other nodes
            for node_id in self.node_addresses:
                if node_id != self.node_id:
                    self.send_request_vote(node_id)

    
    def RequestVote(self, request, context):
        # with self.state_lock:
            if request.term < self.current_term:
                return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

            if (request.lastLogTerm, request.lastLogIndex) < (self.lastLogTerm, self.lastLogIndex):
                return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

            # Only update term if it's higher than the current term
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None

            if self.voted_for is None or self.voted_for == request.candidateId:
                self.voted_for = request.candidateId
                self.election_timeout = self.reset_election_timer()
                return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True)
            else:
                return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

    def has_majority_votes(self):
        # Calculate the number of votes received, including self-vote
        num_votes = len(self.votes_received) + 1  # Include self-vote
        return num_votes > len(self.node_addresses) / 2

    def send_request_vote(self, node_id):
        if node_id == "node_4":
            return

        stub = raft_pb2_grpc.RaftServiceStub(self.grpc_clients[node_id])
        request = raft_pb2.RequestVoteRequest(
            term=self.current_term,
            candidateId=self.node_id,
            lastLogIndex=self.lastLogIndex,  # Use attribute, not method call
            lastLogTerm=self.lastLogTerm     # Use attribute, not method call
        )
        try:
            response = stub.RequestVote(request)
            if response.voteGranted:
                self.votes_received.add(node_id)
                if len(self.votes_received) > (len(self.node_addresses) // 2) and self.state != 'leader':
                    self.become_leader()
        except grpc.RpcError as e:
            print(f"Error sending RequestVote RPC to node {node_id}: {e}")


            


    def AppendEntries(self, request, context):
        if request.term >= self.current_term:
            self.current_term = request.term
            self.leader_id = request.leaderId
            if self.state != 'follower':
                self.state = 'follower'
                self.voted_for = None
        return raft_pb2.AppendEntriesResponse(success=True, term=self.current_term)
    
    def reset_election_timer(self):
        return time.time() + random.uniform(5, 10) 

        

        def check_lease_renewal(self):
            if self.state == 'leader' and not self.renew_lease():
                self.log_info(f"Leader {self.node_id} lease renewal failed. Stepping Down.")
                self.step_down()
    


    def check_election_timeout(self):
        # with self.state_lock:
            if time.time() >= self.election_timeout:
                self.start_election()

    def main_loop(self):
            while True:
                if time.time() > self.election_timeout:

                    self.start_election()
                    time.sleep(1)
                
            # ...
    
    # def on_receive_request_vote(self, candidate_id, term, last_log_index, last_log_term):
    #     with self.state_lock:
    #         if term < self.current_term:
    #             return False  # Don't vote for candidates in older terms

    #         # Check if log is at least as up-to-date as candidate's log
    #         log_up_to_date = (last_log_index, last_log_term) >= self.last_log_info()
    #         if term > self.current_term or (self.voted_for in [None, candidate_id] and log_up_to_date):
    #             self.voted_for = candidate_id
    #             self.current_term = term
    #             self.election_timeout = self.reset_election_timer()  # Reset election timer
    #             return True
    #         return False

    def last_log_info(self):
        if len(self.log) > 0:
            last_index = len(self.log) - 1
            last_term = self.log[last_index]['term']
            return last_index, last_term
        return 0, 0
    
    def become_leader(self):
            print("Leader")
        # with self.state_lock:
            self.state = 'leader'
            self.current_leader_id = self.node_id
            self.next_index = {node_id: len(self.log) for node_id in self.node_addresses}
            self.match_index = {node_id: 0 for node_id in self.node_addresses}
            self.log_info(f"New Leader waiting for Old Leader Lease to timeout.")
            # Wait for old leader's lease to timeout
            time.sleep(max(0, self.max_old_leader_lease - time.time()))

            # Start the leader lease
            self.lease_expires = time.time() + self.lease_duration

            self.log_info(f"Node {self.node_id} became the leader for term {self.current_term}.")

            # Append a NO-OP entry to the log
            self.append_entry_to_log('NO-OP')

            # Send initial empty AppendEntries RPCs (heartbeats) to all followers
            self.send_heartbeats()

    def old_leader_lease_remaining(self):
        # Assuming self.lease_expires stores the timestamp when the old leader's lease expires
        return max(0, self.lease_expires - time.time())

    def send_heartbeats(self):
        print("Hello")
        self.log_info(f"Leader {self.node_id} sending heartbeat & Renewing Lease")
        while self.state == 'leader':
        # with self.state_lock:
            # Renew the leader lease
            self.lease_expires = time.time() + self.lease_duration
            prev_log_index = len(self.log) - 1 if self.log else 0
            prev_log_term = self.log[prev_log_index]['term'] if self.log else 0

            request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                # entries=[raft_pb2.LogEntry(term=entry['term'], command=json.dumps(entry)) for entry in entries],
                entries = [],
                leaderCommit=self.commit_index)
            # Send AppendEntries RPC (heartbeat) to all followers
            for node_id in self.node_addresses:
                if node_id != self.node_id:
                    self.send_append_entries_rpc(node_id, request)


            time.sleep(self.heartbeat_interval)  # Convert milliseconds to seconds

    def replicate_logs(self):
        # This method is called whenever there is a new log entry to be replicated
        for node_id in self.node_addresses:
            if node_id != self.node_id:
                # Replicate log asynchronously or in a separate thread
                self.replicate_log_to_follower(node_id)


    def replicate_log_to_follower(self, follower_id):
        # Get the next log index to send to the follower
        next_index = self.next_index[follower_id]
        # Prepare the entries to be sent
        entries = self.log[next_index:]
        # Prepare the AppendEntries RPC arguments
        prev_log_index = next_index - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

        request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                entries=[raft_pb2.LogEntry(term=entry['term'], command=json.dumps(entry)) for entry in entries],
                leaderCommit=self.commit_index)
        # Call the AppendEntries RPC on the follower
        success = self.send_append_entries_rpc(
            follower_id, request
        )

        if success:
            # Update the next_index and match_index for the follower
            self.next_index[follower_id] = len(self.log)
            self.match_index[follower_id] = self.next_index[follower_id] - 1
            # Check if there are entries that can be committed
            self.try_commit_entries()


    
    def send_append_entries_rpc(self, follower_id, request):
        channel = self.grpc_clients[follower_id]
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        try:
                response = stub.AppendEntries(request)
            # with self.state_lock:
                if response.success:
                    print("#############################")
                    # Update next_index and match_index for the follower
                    self.next_index[follower_id] = request.prevLogIndex + len(request.entries) + 1
                    self.match_index[follower_id] = self.next_index[follower_id] - 1
                    # Try committing entries if possible
                    self.try_commit_entries()
                else:
                    print("***********************************")
                    self.next_index[follower_id] = max(0, self.next_index[follower_id] - 1)
                    self.replicate_log_to_follower(follower_id)
        except grpc.RpcError as e:
            self.log_info(f"RPC error occurred when sending AppendEntries to {follower_id}: {e}")


    def on_receive_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        # with self.state_lock:
            if term < self.current_term:
                return False

            self.current_term = term
            self.election_timeout = self.reset_election_timer()  # Reset election timer
            self.leader_id = leader_id

            # If log doesn't contain an entry at prev_log_index whose term matches prev_log_term, return False
            if not self.log_contains_entry(prev_log_index, prev_log_term):
                return False

            # If an existing entry conflicts with a new one, delete the existing entry and all that follow it
            self.log = self.log[:prev_log_index] + entries

            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self.apply_commited_entries()

            return True

    
    def log_contains_entry(self, index, term):
        if index < len(self.log) and self.log[index]['term'] == term:
            return True
        return False


    # Other methods for election, RPC handlers, etc.
    def try_commit_entries(self):
        # with self.state_lock:
            if self.state != 'leader':
                return

            # Find the highest index that a majority of followers have replicated
            # and that belongs to the current term
            for index in range(len(self.log) - 1, self.commit_index, -1):
                print("check 1 check 1 check 1")
                if self.log[index]['term'] == self.current_term:
                    print("check 2 check 2 check 2")
                    count = 1  # Include the leader itself
                    for node_id in self.match_index:
                        if self.match_index[node_id] >= index:
                            print("check 3 check 3 check 3")
                            count += 1
                    if count > len(self.node_addresses) // 2:
                        print("checking checking checking !!!!!!!!!!!!!!!!!!")
                        self.commit_index = index
                        self.apply_committed_entries()
                        break


    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            # Apply the entry to the state machine (e.g., update a key-value store)
            # For illustration, we'll just print the applied entry
            self.log_info(f"Node {self.node_id} (follower) committed the entry {entry['operation']} to the state machine.")
            print(f"Applied to state machine: {entry}")


    def apply_entry_to_state_machine(self, entry):
        """
        This method applies the entry to the node's state machine.
        """
        # In an actual implementation, this would modify the node's state based on the entry.
        # For example, if it's a SET operation, it would update a key-value store.
        # This is a simplified version for illustration purposes.
        if entry['operation'] == 'SET':
            self.state_machine[entry['key']] = entry['value']
            print(f"Applied to state machine: {entry}")


    def on_receive_append_entries(self, request, context):
        # with self.state_lock:
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
            
            # Update the follower's log based on the received entries
            # (this is simplified; in practice, you would need to handle log inconsistencies)
            self.log = self.log[:request.prevLogIndex] + request.entries
            self.write_all_log_entries_to_file()

            # Update the commit index if necessary
            if request.leaderCommit > self.commit_index:
                self.commit_index = min(request.leaderCommit, len(self.log) - 1)
                self.apply_committed_entries()

            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)
        

    def ServeClient(self, request, context):
        operation = json.loads(request.Request)
        if self.state == 'leader':
            # Handle SET and GET operations
            if operation['operation'] == 'SET':
                self.set_key_value(operation['key'], operation['value'])
                return raft_pb2.ServeClientReply(Success=True, Data="Set operation successful")
            elif operation['operation'] == 'GET':
                value = self.get_key_value(operation['key'])
                return raft_pb2.ServeClientReply(Success=True, Data=value)
        else:
            # Redirect to the current leader
            return raft_pb2.ServeClientReply(Success=False, LeaderID=self.current_leader_id)


    def start(self):
        # Load state from persistent storage
        self.load_state_from_file()

        # Set up and start the gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.node_addresses[self.node_id])
        self.server.start()

        # Start the main loop in a separate thread
        threading.Thread(target=self.main_loop, daemon=True).start()

        # Print a message to indicate that the node has started
        print(f"Node {self.node_id} started as {self.state}")

    # ... other existing methods ...
        
    def run(self):
        # Start the gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.node_addresses[self.node_id])
        self.server.start()

        # Print a message to indicate that the node has started
        print(f"Node {self.node_id} started as {self.state}")

        # Enter the main loop
        self.main_loop()


