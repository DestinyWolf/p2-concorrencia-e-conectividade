from hashlib import sha256
from utils.utils import *
from utils.socketCommunicationProtocol import *
import socket
from server.customExceptions import InvalidTokenException
from concurrent.futures import ThreadPoolExecutor, wait


##
#   @brief: Classe utilizada para gerenciar as requisições dos clientes
##
class ClientHandler:

    pool = ThreadPoolExecutor(max_workers=1)


    def __init__(self, conn, addr):  
        self.conn = conn
        self.addr = addr
    
    ##
    #   @brief: Cria um novo usuário no sistema 
    #   @return: token do novo usuário ou None caso a operação tenha falhado
    ##
    def create_user(self, email:str):
        token = sha256(email.encode(ConstantsManagement.FORMAT.value)).hexdigest()
        created_status = UsersData.save_user(email, token)
        return token if created_status else None

    ##
    #   @brief: Busca o token de um usuário no sistema 
    #   @return: token do usuário (str)
    #   @raises: FileNotFound caso o arquivo de usuários não seja encontrado
    #            KeyError caso o usuário não esteja cadastrado no sistema
    ##
    def get_token(self, email:str):
        try:

            users:dict[str,str] = UsersData.load_users()
            return users[email]
        
        except FileNotFoundError:
            raise
        except KeyError:
            print(f'[SERVER] {self.addr} Client not registered')
            raise 
    
##
#   @brief: Autentica o token passado como argumento
#   @param: token - chave a ser autenticada
#   @raises: FileNotFound caso o arquivo de usuários não seja encontrado
#            InvalidToken caso o token não pertença a um usuário
##

    def auth_token(self, token = None):
        try:
            users:dict = UsersData.load_users()
            if (not token) or (token not in users.values()):
                raise InvalidTokenException('Token is not registered!')
            return True
        except FileExistsError:
            raise
        except InvalidTokenException:
            print(f'[SERVER] {self.addr} Invalid token')
            raise

##
#   @brief: Realiza a operação de compra de da lista de voos passadas
#   @param: server_data - objeto do tipo ServerData
#   @param: token - chave autenticada
#   @param: routes - lista de voos a serem comprados
#   @raises: FileNotFound caso o arquivo de usuários não seja encontrado
#            
##

    def buy_routes(self, server_data:ServerData, token:str, routes:list[tuple[str,str]]):
        try:
            email = self.__get_email(token)
            future = ClientHandler.pool.submit(server_data.dec_all_routes, routes)
            wait([future])
            if future.result():
                ticket = Ticket(email, routes)
                ticket.save()
                return ticket.to_json()
            else:
                return None
        except FileNotFoundError:
            raise
        except InvalidTokenException:
            raise
        
##
#   @brief: Realiza a busca do email do cliente por meio do token
#   @param: token - chave de busca autenticada
#
#   @raises: FileNotFound caso o arquivo de usuários não seja encontrado 
##
            
    def __get_email(self, token:str):
        try:
            users:dict[str,str] = UsersData.load_users()
            for user, client_token in users.items():
                if client_token == token:
                    return user
            
            raise InvalidTokenException('Client not found')
        except FileNotFoundError:
            raise
        except InvalidTokenException:
            print(f'[SERVER] {self.addr} Client not found')
            raise

    ##
    #   @brief: Realiza a busca de todos os tickets já emitidos a um cliente
    #   @param: token - chave de busca autenticada
    #
    #   @raises: FileNotFound caso o arquivo de usuários não seja encontrado 
    ##
        
    def get_tickets(self, token:str):
        try:
            email = self.__get_email(token)
            all_tickets:dict = Ticket.load_tickets()
            return all_tickets.get(email)

        except (InvalidTokenException, FileNotFoundError):
            raise

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
        
 
        

    
    
