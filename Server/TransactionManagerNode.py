from utils.twoPhaseCommit import *
from TwoPhaseCommitNode import *


class TransactionManager(TwoPhaseCommitNode):
    def __init__(self, transaction_id, coordinator_id, participants=None, intentions=None, host_port=8000):
        super().__init__(host_port)
        self.coordinator_id:str = coordinator_id
        self.transaction_id:str = transaction_id
        self.participants:list[str] = participants
        self.intentions:list[str] = intentions
        self.status:TransactionStates = TransactionStates.PREPARE
        self.logger.name = f"{type(self).__name__} - ({self.id.value}, {self.transaction_id})"

    def to_db_entry(self) -> dict:
        return {'id': self.transaction_id, 'status': self.status.value, 'coordinator': self.coordinator_id, 'participants': self.participants, 'intentions': self.intentions}
    
    def to_rpc_msg(self):
        return {'id': self.transaction_id, 'status': self.status.value}
    
    def begin_transaction(self) -> str:
        transaction_info = self.db_handler.get_data_by_filter({'id': self.transaction_id}, CollectionsName.LOG.value)
        
        #Checking if the transaction is new (not written in log)
        if transaction_info:
            self.logger.error("Transaction already exists.")
            return transaction_info[0]['status']

        else:
            #Write transaction to log
            self.db_handler.insert_data(self.to_db_entry(), CollectionsName.LOG.value)
            self.logger.info("Transaction accepted")
            return self.status.value

    def handle_prepare_RPC(self) -> str:
        trans_status = self.begin_transaction()
        
        try:
            if trans_status == "prepare": #transaction added to log
                for route in self.intentions:
                    self.graph.path_locks.get(route).acquire()
                
                for route in self.intentions:
                    if self.graph[route[0]][route[1]]['sits'] == 0:
                        self.status = TransactionStates.ABORTED
                        break

                assert self.status == TransactionStates.PREPARE
                self.status = TransactionStates.READY 
            else:
                self.logger(f"Recover status: {trans_status}")
                return trans_status        
                    
        except AssertionError:
            for route in self.intentions: 
                self.graph.path_locks.get(route).release()
        
        finally:
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'id': self.transaction_id}, self.to_db_entry())
            self.logger(self.status.value)
            return self.status.value

    def handle_commit_RPC(self) -> str:
        transaction_info:dict = self.db_handler.get_data_by_filter({'id': self.transaction_id}, CollectionsName.LOG.value)[0]
        self.participants = transaction_info.get('participants')
        self.status =  TransactionStates(transaction_info.get('status'))
        self.intentions = transaction_info.get('intentions')
        try:
            assert self.status == TransactionStates.READY
        except AssertionError:
            self.logger(f"Illegal commit. Current state is {self.status.value}")
            return None #bug: must return a msg to coordinator
        
        self.status = TransactionStates.COMMIT
        self.logger.info('Commited')

        for route in self.intentions:
            self.graph[route[0]][route[1]]['sits'] -= 1
            if self.graph[route[0]][route[1]]['sits'] == 0:
                #send uddate request
                pass
            self.graph.path_locks[route[0]][route[1]].release()

        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, self.to_db_entry())

        return self.status.value
    
    def handle_abort_RPC(self) -> str:
        transaction_info:dict = self.db_handler.get_data_by_filter({'id': self.transaction_id}, CollectionsName.LOG.value)[0]
        self.participants = transaction_info.get('participants')
        self.status =  TransactionStates(transaction_info.get('status'))
        self.intentions = transaction_info.get('intentions')

        try:
            assert self.status == TransactionStates.READY or self.status == TransactionStates.PREPARE
        except AssertionError:
            self.logger(f"Illegal commit. Current state is {self.status.value}")
            return None #bug: must return a msg to coordinator

        self.status = TransactionStates.ABORTED
        self.logger.warning('Aborted transaction')

        for route in self.intentions:
            self.graph[route[0]][route[1]].release()

        return self.status.value
