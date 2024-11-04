
from TwoPhaseCommitNode import *
from TransactionProtocolState import *
from utils.twoPhaseCommit import *
from utils.database import *
from hashlib import sha256
import datetime

class TransationCoordinator(TwoPhaseCommitNode):

    def __init__(self, host_id, host_name, host_port=8000):
        super().__init__(host_id, host_name, host_port)
        self.logger.name = f"{type(self).__name__} - {self.host_name.value}"
    
    def setup_transaction(self, routes:list[list[str,str,str]], client_ip:str) -> TransactionProtocolState:
        timestamp = self.clock.increment_clock(self.host_id.value)
        transaction_id = (self.host_ip+str(datetime.datetime.now())+client_ip+str(timestamp)).encode()
        transaction_id = sha256(transaction_id).hexdigest()

        transaction_state  = TransactionProtocolState(self.host_name.value, transaction_id, timestamp)
        
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

        #TODO send prepare msg to all participants
        #if coordinator is a participant also, check for available sits

        self.logger.info(f"{self.host_name} send PREPARE request to participants of transaction {transaction.transaction_id}")
        
        if all(transaction.preparedToCommit.values()):
            transaction.status = TransactionStatus.COMMIT
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
            self.logger.info(f'Transaction {transaction.transaction_id} COMMITED')
            #send commit msgs
            ##if coordinator is a participant also, commit changes
        else:
            transaction.status = TransactionStatus.ABORTED
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
            self.logger.warning(f'Transaction {transaction.transaction_id} ABORTED')
            #send abort msg
            #if coordinator is a participant also, release locks

        transaction.status = TransactionStatus.DONE
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction.transaction_id}, transaction.to_db_entry())
        self.logger.info(f'Transaction {transaction.transaction_id} DONE')
        return transaction.status.value
   
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
        





            
        
            

    


