
from enum import Enum

## 
#   @brief: Classe utilizada para o gerenciamento das collections armazenadas no banco de dados
#   @note: Extende a classe Enum
##
class CollectionsName(Enum):
    USER = 'userCollection'
    TICKET = 'ticketCollection'
    ROUTE = 'routeCollection'
    GRAPH = 'graphCollection'
    LOG = 'logCollection'
    CONNECT_STRING = 'mongodb://localhost:27017/'
    COMPANY = 'company'
