from enum import Enum
from utils.socketCommunicationProtocol import ConstantsManagement as cm

 
NODES_COUNT = 3 #Número de servidores no cluster


##
#   @brief: Classe utilizada para gerenciar os possiveis estados que uma transação pode assumir
#   @note: estende a classe Enum
##
class TransactionStatus(Enum):
    PREPARE = "PREPARE"
    READY = "READY"
    COMMIT = "COMMITED"
    ABORTED = "ABORTED"
    DONE = "DONE"

##
#   @brief: Classe utilizada para gerenciar os ids de cada servidor no cluster
#   @note: estende a classe Enum
##
class ServerIds(Enum):
    A = 0
    B = 1
    C = 2

##
#   @brief: Classe utilizada para gerenciar os nomes de cada servidor no cluster
#   @note: estende a classe Enum
##
class ServerName(Enum):
    A = 'Server-A'
    B = 'Server-B'
    C = 'Server-C'

#Gerenciamento dos IPs de cada servidor no cluster
SERVERIP:dict = {ServerName.A.value: '127.0.0.1', ServerName.B.value: '127.0.0.2', ServerName.C.value: '127.0.0.3'}


#Gerenciamento das portas de cada servidor no cluster
SERVERPORT:dict = {ServerName.A.value: '5000', ServerName.B.value: '5001', ServerName.C.value: '5002'}
