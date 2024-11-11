import socket

##@brief: Classe usada pra gerenciar o socket do cliente e realizar conexão com o servidor
class ClientSocket:

    def __init__(self, ip=''):
        self.addr = None
        self.ip = ip
        self.port = 8000
        self.client_socket = None
        self.token = ''

    ##@brief: metodo responsavel por realizara a conexão
    # @return: 1 caso a conexão tenha ocorrido com sucesso, 0 caso contrario
    def connect(self):
        self.addr = (self.ip, self.port)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.settimeout(10)

        try:
            self.client_socket.connect(self.addr)
            return 1
        except socket.error as e:
            return 0

    ##@brief:encerra o socket e a conexão com o servidor
    def end(self):
        self.client_socket.close()




