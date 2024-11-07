
from enum import Enum

## 
#   @brief: Classe utilizada para o gerenciamento dos arquivos utilizados no armazenamento de dados do servidor
#   @note: Extende a classe Enum
##
class CollectionsName(Enum):
    USER = 'userCollection'
    TICKET = 'ticketCollection'
    ROUTE = 'routeCollection'
    GRAPH = 'graphCollection'
    LOG = 'logCollection'
    CONNECT_STRING = 'mongodb://localhost:27018/'
    COMPANY = 'company'
