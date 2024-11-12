
import socket
import logging as log



##
#   @brief: Classes utilizada para o gerenciamento da conexão socket com o ciente
##
class SocketManager():
    def __init__(self, host_ip:str=None, host_port:int=8000):
        self.host_ip = socket.gethostbyname(socket.gethostname()) if host_ip is None else host_ip
        self.host_port = host_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #Encerra o socket caso o programa seja encerrado
        log.basicConfig(filename="socketManagement.log", filemode="a+", level=log.INFO)
        self.logger:log.Logger = log.getLogger(f"{type(self).__name__} - ({self.host_ip}, {self.host_port})")
        
    ##
    #   @brief: Método utilizado para o estabelecimento do canal de conexão (inicialização do socket)
    #   @param: port - porta a ser usada
    #   @return True se a inicialização foi bem sucedida. Caso contrário, False
    ##
    def init_socket(self) -> bool:
        
        status = False
        addr_socket = (self.host_ip, self.host_port)

        try:
            #Bind do socket
            self.server_socket.bind(addr_socket)
            self.server_socket.listen(5)
            self.logger.info(f"Server started at address {addr_socket[0]} and port {self.host_port}\n")

            status = True
        except socket.error as err:
            self.logger.warning("Server failed to initialize socket!, Please check if the socket was already initialized", exc_info=True)

        return status
