from server.requests import ConstantsManagement
from serverSide.ClientHandlerClass import ClientHandler
import socket
from threading import Lock

##
#   @brief Classe utilizada para o gerenciamento das informações de funcionamento do servidor
##
class Server:
    _instance = None
    backlog_clients:list[ClientHandler] = []
    backlog_lock = Lock()

    def __new__(cls): #padrão singleton
        if not cls._instance:
            cls._instance = super().__new__(cls)
        
        return cls._instance

    def __init__(self): #criação do socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #Encerra o socket caso o programa seja encerrado
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    ##
    #   @brief: Método utilizado para o estabelecimento do canal de conexão (inicialização do socket)
    #   @param: port - porta a ser usada
    #   @return True se a inicialização foi bem sucedida. Caso contrário, False
    ##
    def init_socket(self, port): 
        status = False
        addr_socket = (ConstantsManagement.HOST.value, port)

        try:
            #Bind do socket
            self.server_socket.bind(addr_socket)
            self.server_socket.listen(5)
            print(f"[SERVER] Server started at address {addr_socket[0]} and port {port}\n")

            status = True
        except socket.error as err:
            print(f"[SERVER] Failed to initialize socket!{err}")

        return status
    
    ##
    #   @brief: Método utilizado para a adicionar um cliente ao backlog de conexões do servidor. Implementa técnicas
    #   para o gerenciamento de condições de corrida
    #   @param: client: cliente a ser adicionado
    ##
    @classmethod
    def add_client(cls, client:ClientHandler):
        with cls.backlog_lock:
            cls.backlog_clients.append(client)

     ##
    #   @brief: Método utilizado para a remover um cliente ao backlog de conexões do servidor. Implementa técnicas
    #   para o gerenciamento de condições de corrida
    #   @param: client: cliente a ser removido
    ##
    @classmethod
    def remove_client(cls, client:ClientHandler):
        with cls.backlog_clients:
            cls.backlog_clients.remove(client)



