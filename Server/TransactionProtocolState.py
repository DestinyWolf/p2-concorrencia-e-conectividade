from Server.Transaction import *
import json

class TransactionProtocolState(Transaction):
    def __init__(self, coordinator:str=None, transaction_id:str=None, participants=set(), intentions={}, timestamp = None):
        super().__init__(coordinator, transaction_id, participants, intentions, timestamp)
        self.preparedToCommit = {}
        self.done = {}

    def load_transaction_from_db(self, transaction_id, db_handler):
        restored_data = db_handler.get_data_by_filter({'_id': transaction_id}, CollectionsName.LOG.value)[0]
        self.coordinator = restored_data['coordinator']
        self.participants = restored_data['participants']
        self.intentions = restored_data['intentions']
        self.status = TransactionStatus(restored_data['status'])
        self.timestamp = restored_data['timestamp']
        self.preparedToCommit = restored_data['preparedToCommit']
        self.done = restored_data['done']


    def to_db_entry(self) -> dict:
        return {'_id': self.transaction_id, 'coordinator': self.coordinator, 'participants': list(self.participants),
                'intentions': self.intentions, 'status': self.status.value, 'timestamp': self.timestamp, 'preparedToCommit': self.preparedToCommit,
                'done': self.done}
    
    def to_request_msg(self, peer) -> dict:
        return {'transaction_id': self.transaction_id, 'coordinator':self.coordinator, 'timestamp': self.timestamp,
                'participants': list(self.participants), 'intentions': self.intentions[peer]}
    

        


    

        

    