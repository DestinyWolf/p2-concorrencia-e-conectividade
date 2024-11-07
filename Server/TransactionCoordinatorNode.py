
import requests
from Server.TwoPhaseCommitNode import *
from Server.TransactionProtocolState import *
from Server.utils.twoPhaseCommit import *
from Server.utils.database import *
from hashlib import sha256
import datetime

class TransationCoordinator(TwoPhaseCommitNode):

    def __init__(self, host_id, host_name, host_port=8000):
        super().__init__(host_id, host_name, host_port)
        self.logger.name = f"{type(self).__name__} - {self.host_name.value}"
    
    def setup_transaction(self, routes:list[list[str]], client_ip:str) -> TransactionProtocolState:
        timestamp = self.clock.increment_clock(self.host_id.value)
        transaction_id = (self.host_ip+str(datetime.datetime.now())+client_ip+str(timestamp)).encode()
        transaction_id = sha256(transaction_id).hexdigest()

        transaction_state  = TransactionProtocolState(coordinator=self.host_name.value, transaction_id=transaction_id, timestamp=timestamp)
        
        for route in routes:
            participant:str = route[2]
            transaction_state.participants.add(participant) 
            
            if participant in transaction_state.intentions:
                transaction_state.intentions[participant].append((route[0], route[1]))
            else:
                transaction_state.intentions[participant] = [(route[0], route[1])]
            

        for participant in transaction_state.participants:
            transaction_state.preparedToCommit[participant] = None
            transaction_state.done[participant] = None

        transaction_state.status = TransactionStatus.PREPARE

        return transaction_state


    def prepare_transaction(self, transaction: TransactionProtocolState) -> str:
        
        self.db_handler.insert_data(transaction.to_db_entry(), CollectionsName.LOG.value)
        self.logger.info(f'Transaction {transaction.transaction_id} initiated')

        for participant in transaction.participants:
            try:
                if participant != self.host_name.value:
                    response = requests.post(f'http://{SERVERIP[participant]}:{SERVERPORT[participant]}/newtransaction', json=transaction.to_request_msg(participant), headers={"Content-Type": "application/json"}, timeout=30)
                    transaction.preparedToCommit[participant] = True if response.json().get('msg') == TransactionStatus.READY.value else False
            except (ConnectionAbortedError, ConnectionRefusedError, ConnectionError, requests.Timeout, TimeoutError, requests.ConnectionError) as err:
                transaction.preparedToCommit[participant] = False 
        
        self.logger.info(f"{self.host_name} send PREPARE request to participants of transaction {transaction.transaction_id}")
        if self.host_name.value in transaction.participants:
            for route in transaction.intentions[self.host_name.value]: #locking routes
                self.graph.path_locks[route].acquire()

            for route in transaction.intentions[self.host_name.value]: #checking if there are sits available
                if self.graph.graph[route[0]][route[1]]['sits'] == 0:
                    transaction.preparedToCommit[self.host_name.value] = False
                    break

            else:
                transaction.preparedToCommit[self.host_name.value] = True
        
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry(), )

        if all(transaction.preparedToCommit.values()):
            transaction.status = TransactionStatus.COMMIT
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
            self.logger.info(f'Transaction {transaction.transaction_id} COMMITED')
            
            if self.host_name.value in transaction.participants:
                self.__commit_local_transaction(transaction)
                transaction.done[self.host_name.value] = True
                
        else:
            transaction.status = TransactionStatus.ABORTED
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
            self.logger.warning(f'Transaction {transaction.transaction_id} ABORTED')
            if self.host_name.value in transaction.participants:           
                for route in transaction.intentions[self.host_name.value]: #unlocking routes
                    self.graph.path_locks[route].release()

        for participant in transaction.participants:
            try:
                if participant != self.host_name.value:
                    response = requests.post(f'http://{SERVERIP[participant]}:{SERVERPORT[participant]}/committransaction', json={'transaction_id': transaction.transaction_id, 'decision': transaction.status.value}, headers={"Content-Type": "application/json"}, timeout=30)
                    transaction.done[participant] = True if response.json().get('msg')== TransactionStatus.DONE.value else False
            except (ConnectionAbortedError, ConnectionRefusedError, ConnectionError, requests.Timeout, TimeoutError, requests.ConnectionError) as err:
                pass

        
        if transaction.status == TransactionStatus.COMMIT:
            transaction.status = TransactionStatus.DONE
        
        
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
        self.logger.info(f'Transaction {transaction.transaction_id} {transaction.status.value}')
        return transaction.status.value
    

    def __commit_local_transaction(self, transaction: TransactionProtocolState):
        new_values = []
        
        for route in transaction.intentions[self.host_name.value]:
            u , v = route
            self.graph.graph[u][v]['sits'] -= 1
            print(self.graph.graph[u][v]['sits'])
            if self.graph.graph[u][v]['sits'] == 0:
                self.graph.graph[u][v]['weight'] = 999
                self.graph.graph[u][v]['company'][self.host_name.value] = 999
                self.graph.update_global_edge_weight((u,v))
                for peer, ip in SERVERIP.items():
                    if peer != self.host_name.value:
                        try:
                            response = requests.post(f'http://{ip}:{SERVERPORT[peer]}/updateroute', json={'whoIsMe': self.host_name.value, 'routeToUpdate': route, 'msg':999}, headers={"Content-Type": "application/json"})
                        except (ConnectionAbortedError, ConnectionRefusedError, ConnectionError, requests.Timeout, TimeoutError, requests.ConnectionError) as err:
                            continue
            
            attrs = self.graph.graph[u][v].copy()
            del attrs['company']
            del attrs['globalWeight']
            new_values.append(({'_id':f'{u}|{v}'}, {'_id': f'{u}|{v}', u:{v:attrs}}))
            self.graph.path_locks[(u, v)].release()
        
        self.db_handler.update_many(CollectionsName.GRAPH.value, new_values)


    def handle_ready_RPC(self, transaction_id:str, server_name:str, ready:bool):
        transaction = TransactionProtocolState()
        transaction.load_transaction_from_db(transaction_id, self.db_handler)

        if transaction.status == TransactionStatus.COMMIT:
            self.logger.info(f"Received READY message from {server_name} for commited transaction {transaction_id}")
            #send commit msg
        elif transaction.status == TransactionStatus.PREPARE:
            self.logger.info(f"Received READY message from {server_name} for transaction {transaction_id}")
            transaction.preparedToCommit[server_name] = True
        elif transaction.status == TransactionStatus.ABORTED:
            self.logger.info(f"Received READY message from {server_name} for aborted transaction {transaction_id}")
            #send abort msg
        elif transaction.status == TransactionStatus.DONE:
            self.logger.info(f"Received READY message from {server_name} for done transaction {transaction_id}")
            #send done msg

    def handle_done_RPC(self, transaction_id, server_name):
        transaction = TransactionProtocolState()
        transaction.load_transaction_from_db(transaction_id, self.db_handler)

        transaction.done[server_name] = True
        self.logger.info(f"Received DONE from {server_name} for {transaction_id}")

        if all(transaction.done.values()):
            self.logger.info(f"{transaction_id} -> DONE. Deleting transaction")
            self.db_handler.delete_data_by_filter({'_id': transaction_id}, CollectionsName.LOG.value)
        





            
        
            

    


