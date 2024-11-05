
from  concurrent.futures import *
import logging as log
from Server.utils.twoPhaseCommit import *
from Server.utils.database import *
from Server.utils.graph import *
from Server.vector_clock import *
from Server.database.mongoHandler import *
from threading import Lock




class TwoPhaseCommitNode():
    
    host_id:ServerIds = None
    host_name:ServerName = None
    
    clock:VectorClock = VectorClock()
    
    graph: RoutesGraph = None


    def __init__(self, host_id: ServerIds, host_name:ServerName, host_port=8000):
        self.__class__.host_id = host_id
        self.__class__.host_name = host_name
        self.__class__.graph = RoutesGraph(host_name.value)
        self.host_ip:str = SERVERIP[host_name.value]
        self.host_port:int = host_port
        self.db_handler = MongoHandler(connect_string=CollectionsName.CONNECT_STRING.value, companhia=host_name.value) 

        log.basicConfig(filename=self.host_ip+".log", filemode="a+", level=log.INFO)
        self.logger:log.Logger = log.getLogger(f"{type(self).__name__} - ({self.host_ip}, {self.host_port})")

    def recover_log(self):
        raise NotImplementedError()
    
       


    


        
        