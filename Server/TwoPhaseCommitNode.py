
from  concurrent.futures import *
import logging as log
from utils.twoPhaseCommit import *
from utils.database import *
from utils.graph import *
from vector_clock import *
from mongoHandler import *
from threading import Lock
import socket



class TwoPhaseCommitNode():
    
    id:ServerIds = ServerIds.A
    
    clock:VectorClock = VectorClock(id.value)
    clock_lock = Lock()

    graph = RoutesGraph()

    def __init__(self, host_port=8000):
        self.host_ip:str = socket.gethostbyname(socket.gethostname())
        self.host_port:int = host_port
        self.logger:log.Logger = log.getLogger(f"{type(self).__name__} - ({self.host_ip}, {self.host_port})")
        self.db_handler = MongoHandler(CollectionsName.CONNECT_STRING.value, CollectionsName.COMPANY.value) 


    def recover_log(self):
        raise NotImplementedError()
        
        