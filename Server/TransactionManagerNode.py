from utils.twoPhaseCommit import *
from TwoPhaseCommitNode import *
from Transaction import *

class TransactionManager(TwoPhaseCommitNode):

    def __init__(self, host_id, host_name, host_port=8000):
        super().__init__(host_id, host_name, host_port)
        self.logger.name = f"{type(self).__name__} - {self.host_name.value}"
            

    def handle_prepare_RPC(self, transaction:Transaction) -> TransactionStatus:
        recovered_transaction = self.db_handler.get_data_by_filter({'_id': transaction.transaction_id}, CollectionsName.LOG.value)
        
        if recovered_transaction:
            self.logger.warning(f"Transaction {transaction.transaction_id} already exists.")
            return recovered_transaction[0]['status']
        else:
            #Write transaction to log
            transaction.status = TransactionStatus.PREPARE
            self.db_handler.insert_data(transaction.to_db_entry(), CollectionsName.LOG.value)
            self.logger.info(f"Transaction {transaction.transaction_id} accepted")
        
            
        for route in transaction.intentions: #locking routes
            self.graph.path_locks.get(route).acquire()
        
        for route in transaction.intentions: #checking if there are sits available
            if self.graph.graph[route[0]][route[1]]['sits'] == 0:
                transaction.status = TransactionStatus.ABORTED
                break

        if transaction.status == TransactionStatus.PREPARE:
            transaction.status = TransactionStatus.READY 
        else:       
            for route in transaction.intentions: #unlocking routes
                self.graph.path_locks.get(route).release()
                    
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id':transaction.transaction_id}, transaction.to_db_entry())
        self.logger.info(f"{transaction.transaction_id} -> {transaction.status.value}")
        return transaction.status.value

    def handle_commit_RPC(self, transaction_id:str) -> TransactionStatus:
        recovered_transaction:Transaction = Transaction()
        recovered_transaction.load_transaction_from_db(transaction_id, self.db_handler)
        
        if recovered_transaction.status == TransactionStatus.READY:                        
        
            recovered_transaction.status = TransactionStatus.COMMIT
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction_id}, recovered_transaction.to_db_entry())
            self.logger.info(f'Transaction {transaction_id} {recovered_transaction.status.value}')

            for route in recovered_transaction.intentions:
                self.graph.graph[route[0]][route[1]]['sits'] -= 1

                #TODO: update in db with atomic transation
                
                if self.graph.graph[route[0]][route[1]]['sits'] == 0:
                    #send update request
                    pass
                
                self.graph.path_locks[(route[0], route[1])].release()
        else:
            self.logger.info(f"Illegal commit for {transaction_id}. Current state is {recovered_transaction.status}")

        recovered_transaction.status = TransactionStatus.DONE
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction_id}, recovered_transaction.to_db_entry())
        self.logger.info(f'Transaction {transaction_id} {recovered_transaction.status.value}')
        
        return recovered_transaction.status.value
    
    def handle_abort_RPC(self, transaction_id:str) -> TransactionStatus:
        recovered_transaction:Transaction = Transaction()
        recovered_transaction.load_transaction_from_db(transaction_id, self.db_handler)

        if recovered_transaction.status == TransactionStatus.READY:
        
            recovered_transaction.status = TransactionStatus.ABORTED
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': transaction_id}, recovered_transaction.to_db_entry())
            self.logger.warning(f'{transaction_id} -> ABORTED')

            for route in recovered_transaction.intentions:
                self.graph.path_locks[(route[0], route[1])].release()

        else:
            self.logger.warning(f"Illegal abort for {transaction_id}. Current state is {recovered_transaction.status.value}") 


        return recovered_transaction.status.value
