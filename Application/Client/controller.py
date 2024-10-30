from Client import requests
from Client.ClientSockClass import *
from Client import utils

##@brief: constante de texto retornado quando o usuario não estiver conectado
TOKEN_NOT_DEFINED = "Usuario nao conectado"

##@brief:   Função responsavel por realizar o envio dos request para o servidor e retornar  a resposta
#  @param:  request - json, contendo a requesição a ser enviada ao servidor
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna um json contendo a resposta do servidor 
def send_request(request: str, client:ClientSocket):

    if client.connect():
        size_transfer = str(len(request)).encode(requests.ConstantsManagement.FORMAT.value)
        size_transfer += b' ' * (requests.ConstantsManagement.MAX_PKT_SIZE.value - len(size_transfer))

        try:
            client.client_socket.send(size_transfer)
            client.client_socket.send(request.encode(requests.ConstantsManagement.FORMAT.value))
            size_transfer = client.client_socket.recv(requests.ConstantsManagement.MAX_PKT_SIZE.value)
            response_str = client.client_socket.recv(int(size_transfer.decode(requests.ConstantsManagement.FORMAT.value))).decode(requests.ConstantsManagement.FORMAT.value)
            server_response = requests.Response()
            server_response.from_json(response_str)
            response = server_response

        except socket.error as e:
            response = requests.Response(data={'execpt':str(e)}, rs_type=requests.ConstantsManagement.NETWORK_FAIL.value, status=requests.ConstantsManagement.NETWORK_ERROR.value)
            print(str(e))
        client.end()
    else:
        response = requests.Response()
    return response

##@brief:   Função responsavel por realizar o request da compra realizada e retornar a resposta do servidor
#  @param:  routes - list[Route], lista das rotas escolhidas pelo cliente
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna uma tupla onde o primerio elemento é o status da requisição e o segundo os dados retornados pelo servidor
def buying(routes: list, client:ClientSocket):
    if client.token == '':
        return  requests.ConstantsManagement.INVALID_TOKEN.value, TOKEN_NOT_DEFINED

    buy_request = requests.Request(client_token=client.token, rq_data=[], rq_type=requests.ConstantsManagement.BUY.value)

    for fligth in routes:
        route = utils.Route()
        route.from_string(fligth)
        buy_request.rq_data.append((route.match, route.destination))

    response = send_request(buy_request.to_json(), client)

    return response.status, response.data

##@brief:   Função responsavel por realizar o request do token do usuario ao tentar logar e retornar a resposta do servidor
#  @param:  email - str, email do cliente
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna uma tupla onde o primerio elemento é o status da requisição e o segundo os dados retornados pelo servidor
def connect(email: str, client:ClientSocket):
    connect_request = requests.Request(rq_data=email, rq_type=requests.ConstantsManagement.GETTOKEN.value)
    response = send_request(connect_request.to_json(), client)

    return response.status, response.data


##@brief:   Função responsavel por realizar o request das rotas disponiveis, saindo e indo para onde o usuario escolheu, e retornar a resposta do servidor
#  @param:  match - str, local que o cliente está saindo
#  @param:  destination - str, local para onde o cliente deseja ir
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna uma tupla onde o primerio elemento é o status da requisição e o segundo os dados retornados pelo servidor
def search_routes(match:  str, destination: str, client:ClientSocket):
    route_request = requests.Request(client_token=client.token, rq_type=requests.ConstantsManagement.GETROUTES.value, rq_data={'match':match, 'destination':destination})
    response = send_request(route_request.to_json(), client)
    return response.status, response.data

##@brief:   Função responsavel por realizar o request para a criação de conta do usuario, gerar o token e retornar a resposta do servidor
#  @param:  email - str, email do cliente
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna uma tupla onde o primerio elemento é o status da requisição e o segundo os dados retornados pelo servidor
def create_account(email: str, client:ClientSocket):
    create_account_request = requests.Request(rq_type=requests.ConstantsManagement.CREATE_USER.value, rq_data=email)
    response = send_request(create_account_request.to_json(), client)

    return response.status, response.data

##@brief:   Função responsavel por realizar o request das comprar ja feitas por um usuario e retornar a resposta do servidor
#  @param:  client - ClientSocket, objeto do cliente que será usado para enviar a requisição
#  @return:  Retorna uma tupla onde o primerio elemento é o status da requisição e o segundo os dados retornados pelo servidor
def search_bougths(client:ClientSocket):
    if client.token == '':
        return  requests.ConstantsManagement.INVALID_TOKEN.value, TOKEN_NOT_DEFINED

    bougths_request = requests.Request(client_token=client.token, rq_data=[], rq_type=requests.ConstantsManagement.GETTICKETS.value)

    response = send_request(bougths_request.to_json(), client)

    return response.status, response.data
