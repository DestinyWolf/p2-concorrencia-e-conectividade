
import TwoPhaseCommitNode
from utils.twoPhaseCommit import *
from utils.database import *

class TransationCoordinator(TwoPhaseCommitNode):
    def __init__(self, host_port=8000):
        super().__init__(host_port)
        self.intentions = {ServerIds.A.value: [], ServerIds.B.value:[], ServerIds.C.value: []}
        self.participants:set = set()
        self.prepared_to_commit = {}
        self.done = {}
        self.transaction_id = None
        self.status:TransactionStates = TransactionStates.PREPARE
        self.logger.name = f"{type(self).__name__} - ({self.id.value}, {self.transaction_id})"

    def to_db_entry(self):
        return {'id': self.transaction_id, 'status': self.status, 'coordinator': self.id.value, 'participants': list(self.participants), 'intentions': self.intentions, 
                'preparedToCommit': self.prepared_to_commit, 'done': self.done}
    
    def setup(self, routes:list[list[str,str,str]]) -> None:        
        for route in routes:
            self.participants.add(route[2])
            self.intentions[route[2]].append([route[0],route[1]])

        for server in self.participants:
            self.prepared_to_commit[server] = None
            self.done[server] = None

        timestamp = self.clock.increment_clock()
        self.transaction_id = self.host_ip+'-'+str(timestamp) #bug: id not unique

    def update_self(self) -> None:
        transaction_info:dict = self.db_handler.get_data_by_filter({'id': self.transaction_id}, CollectionsName.LOG.value)[0]
        self.status = TransactionStates(transaction_info['status'])
        self.participants = set(transaction_info['participants'])
        self.intentions = transaction_info['intentions']
        self.prepared_to_commit = transaction_info['preparedToCommit']
        self.done = transaction_info['done']

    def prepare_transaction(self, routes:list[list[str,str,str]]):
        self.setup(routes)
        self.db_handler.insert_data(self.to_db_entry(), CollectionsName.LOG.value)
        self.logger('Transaction initiated')

        try:
            assert self.status == TransactionStates.PREPARE
        except AssertionError:
            self.logger('Illegal prepare request')
            return #bug: must return something
        
        #TODO send prepare msg to all participants
        #if coordinator is a participant also, check for available sits

        self.logger("Send PREPARE request to participants")
        
        if all(self.prepared_to_commit.values()):
            self.status = TransactionStates.COMMIT
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, self.to_db_entry())
            self.logger.info('Transaction commited')
            self.commit_transaction()
        else:
            self.status = TransactionStates.ABORTED
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, self.to_db_entry())
            self.logger.warning('Transaction aborted')
            self.abort_transaction()


    def handle_ready_RPC(self, server_id:str, ready:bool):
        self.update_self()

        if self.status.value == 'commited':
            self.logger.info(f"Received READY message from {server_id} for commited transaction")
            #send commit msg
        elif self.status.value == 'prepare':
            self.logger.info(f"Received READY message from {server_id} for transaction")
        elif self.status.value == 'aborted':
            self.logger.info(f"Received READY message from {server_id} for aborted transaction")
            #send abort msg


        
            








            
        
            

    


