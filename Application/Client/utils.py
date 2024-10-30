from enum import Enum
import os

class FilePathsManagement(Enum):
    PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    NODES_FILE_PATH = PARENT_DIR+'\\DB\\matches_and_destinations.txt'
    ROUTES_DATA_FILE_PATH = PARENT_DIR+'\\DB\\routes_data.json'
    USERS_FILE_PATH = PARENT_DIR+'\\DB\\users.json'
    TICKETS_FILE_PATH = PARENT_DIR+'\\DB\\tickets.json'
    GRAPH_FILE_PATH = PARENT_DIR+'\\DB\\graph.json'

class Route:

    def __init__(self, match = '', destination = '', sits = 0, id = None, company = ''):
        self.match = match
        self.destination = destination
        self.sits = sits
        self.id = id
        self.company = company

    def to_string(self):
        return {'match': self.match, 'destination': self.destination, 'sits': self.sits, 'id': self.id, 'company':self.company}
    
    def from_string(self, data):
        self.match = data['match']
        self.destination = data['destination']
        self.sits = data['sits']
        self.id = data['id']
        self.company = data['company']

