from utils.twoPhaseCommit import *
from utils.database import *
from database.mongoHandler import *

class Transaction():
    def __init__(self, coordinator:str=None, transaction_id:str=None, participants=None, intentions=None):
        self.coordinator:str = coordinator
        self.transaction_id:str = transaction_id
        self.participants:set = participants
        self.intentions = intentions #resources to use
        self.status:TransactionStatus = None

    def load_transaction_from_db(self, transaction_id, db_handler:MongoHandler):
        restored_data = db_handler.get_data_by_filter({'_id': transaction_id}, CollectionsName.LOG.value)[0]
        self.transaction_id = transaction_id
        self.coordinator = restored_data['coordinator']
        self.participants = set(restored_data['participants'])
        self.intentions = restored_data['intentions']
        self.status = TransactionStatus(restored_data['status'])
    
    def to_db_entry(self) -> dict:
        return {'_id': self.transaction_id, 'coordinator': self.coordinator, 'participants': list(self.participants), 'intentions': self.intentions, 'status': self.status.value}
