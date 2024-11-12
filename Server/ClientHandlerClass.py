from hashlib import sha256
from utils.database import *
from utils.socketCommunicationProtocol import *
import socket
from utils.customExceptions import InvalidTokenException
from database.mongoHandler import *


##
#   @brief: Classe utilizada para gerenciar as requisições dos clientes
##
class ClientHandler:

    def __init__(self, conn, addr):  
        self.conn = conn
        self.addr = addr
    
    ##
    #   @brief: Cria um novo usuário no sistema
    #   @param: email: email do cliente a ser cadastrado
    #   @param: db_handler: instância da classe MongoHandler
    #   @return: token do novo usuário ou None caso a operação tenha falhado
    ##
    def create_user(self, email:str, db_handler:MongoHandler):
        token = sha256(email.encode(ConstantsManagement.FORMAT.value)).hexdigest()
        if db_handler.get_data_by_filter({'_id':token}, CollectionsName.USER.value):
            return None
        else:
            db_handler.insert_data({'_id': token, 'email': email}, CollectionsName.USER.value)
            return token

    ##
    #   @brief: Busca o token de um usuário no sistema 
    #   @param: email: email a ser buscado
    #   @param: db_handler: instância da classe MongoHandler
    #   @return: token do usuário (str)
    #   @raises: KeyError caso o usuário não esteja cadastrado no sistema
    ##
    def get_token(self, email:str, db_handler:MongoHandler):
        try:
            token = db_handler.get_data_by_filter({'email':email}, CollectionsName.USER.value)
            if token:
                return token[0]['_id']
            else:
                raise KeyError()
        except KeyError:
            raise 
    
##
#   @brief: Autentica o token passado como argumento
#   @param: token - chave a ser autenticada
#   @param: db_handler: instância da classe MongoHandler
#   @return: True caso o token não esteja cadastrado no banco de dados
#   @raises: InvalidToken caso o token não pertença a um usuário
##

    def auth_token(self, db_handler: MongoHandler, token = None):
        try:
            recovered_data = db_handler.get_data_by_filter({'_id':token}, CollectionsName.USER.value)
            if recovered_data:
                return True
            else:
                raise InvalidTokenException()
        except InvalidTokenException:
            raise
    

    ##
    #   @brief: Realiza a busca de todos os tickets já emitidos a um cliente
    #   @param: token - chave de busca autenticada
    #   @param: db_handler: instância da classe MongoHandler
    #   @return: lista com as informações de todos os tickets. Caso nenhum seja encontrado, retorna None
    ##
        
    def get_tickets(self, token:str, db_handler:MongoHandler):
        tickets = db_handler.get_data_by_filter({'token': token}, CollectionsName.TICKET.value)
        if tickets:
            del tickets[0]['_id']
            return tickets[0]
        else:
            return None


    ##
    #   @brief: Realiza o recebimento de pacotes do cliente
    #   @return: Objeto do tipo Request com os dados da rquisição do cliente
    #   @raises: socket.error caso ocorra uma falha na conexão
    ##
    def receive_pkt(self):
        try:
            pkt = Request()
            pkt_size = self.conn.recv(ConstantsManagement.MAX_PKT_SIZE.value).decode(ConstantsManagement.FORMAT.value)
            if pkt_size:
                pkt_size = int(pkt_size)
                #recebendo segundo pacote -> requisição
                msg = self.conn.recv(pkt_size).decode(ConstantsManagement.FORMAT.value)

                if msg:
                    pkt.from_json(msg)
                    return pkt
                else:
                    print(f"[SERVER] Package reception from {self.addr} failed!\n")
                    return None

            else:
                print(f"[SERVER] Connection test message or package reception from {self.addr} failed!\n")
                return None
            

        except socket.error as err:
            print(f"[SERVER] Package reception from {self.addr} failed! {str(err)}\n")
            return None
        
        

        
    ##
    #   @brief: Realiza o envio de pacotes do cliente
    #
    #   @raises: socket.error caso ocorra uma falha na conexão
    ##
    def send_pkt(self, pkt:Response):
        pkt_json = pkt.to_json()
        try:
            pkt_len = str(len(pkt_json)).encode(ConstantsManagement.FORMAT.value)
            pkt_len += b' ' * (ConstantsManagement.MAX_PKT_SIZE.value - len(pkt_len))

            if (self.conn.send(pkt_len) != 0) and (self.conn.send(pkt_json.encode(ConstantsManagement.FORMAT.value)) != 0):
                return True
            else: 
                print(f"[SERVER] Package transfer to {self.addr} failed! \n")
                return False
            
        except socket.error as err:
            print(f"[SERVER] Package transfer to {self.addr} failed! {str(err)}\n")
            return False
        
 


    
    
