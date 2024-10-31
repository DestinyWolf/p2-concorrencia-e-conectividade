from enum import Enum
from database.mongoHandler import MongoHandler
import hashlib

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
    CONNECT_STRING = 'string de conecxao'
    COMPANY = 'company'

##
#   @brief: Classe utilizada para o armazenamento das informações de cada rota
##
class Route:

    def __init__(self, match:str = '', destination:str = '', sits:int = 0, id:str = ''):
        self.match = match
        self.destination = destination
        self.sits = sits
        self.id = id

    def __repr__(self):
        return f"Route('{self.match}', '{self.destination}',{self.sits}, '{self.id}')"
    
##
#   @brief: Método retorna um dict representativo da instância
#   @return dict com todos os valores dos atributos da instância
##
    def as_dict(self):
        return {'match': self.match, 'destination': self.destination, 'sits': self.sits, 'id': self.id}

    ## 
    #   @brief: Método utilizado para atualizar os atributos de instância de um objeto
    ##
    def from_dict(self, data):
        self.match = data['match']
        self.destination = data['destination']
        self.sits = data['sits']
        self.id = data['id']
            
##
#   @brief: Classe usada para gerenciamento do arquivo de users
## 

class UsersData:
    ##
    #   @brief: Método usado para carregar o arquivo de usuários
    #   @return: dict contém as informaões do arquivo JSON.
    #
    #   @raises: FileNotFoundError caso o arquivo não seja encontrado
    ##
    @classmethod
    def load_users(cls):
        try:
            db = MongoHandler(CollectionsName.CONNECT_STRING.value, CollectionsName.COMPANY.value)
            users = db.get_all_itens_in_group(CollectionsName.USER.value)
            return users
        
        except FileNotFoundError:
            print(f'[SERVER] Could not find users in database')
            raise
    
##
#   @brief: Método atualiza o arquivo de usuários com a nova informação
#   @param: email - email do novo user
#   @param: token - token do novo user
#   @return: True se a operação for bem sucedida. Caso contrário, False
## 
    @classmethod
    def save_user(cls, email:str, token):
        db = MongoHandler(CollectionsName.CONNECT_STRING.value, CollectionsName.COMPANY.value)
        if db.get_data_by_filter({'_id':int(hashlib.md5(email.encode('utf-8')).hexdigest(), 16)}, CollectionsName.USER.value) != None:
            print(f'[SERVER] User already exists')
            return False
        else:
            dic = {'_id':int(hashlib.md5(email.encode('utf-8')).hexdigest(), 16), 'token':token, 'email':email}
            db.insert_data(dic,  CollectionsName.USER.value)
            return True
        
        