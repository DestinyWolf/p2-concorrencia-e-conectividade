from Client import controller
from Client import ClientSockClass
from Client import requests as rq
import requests

#inserir aqui o ip do servidor
ip = '127.0.0.3'
port = 5002
email = 'usuario@test'
match = 'Salvador'
destination = 'Belem'
token = 'ac4ea10f85965aaad78f68093222a4c49d8aa043e8d5d8ad52b4d298c2c64839'
token = 'ac4ea10f85965aaad78f68093222a4c49d8aa043e8d5d8ad52b4d298c2c64839'
trecho1 = ('Salvador', 'Fortaleza', 'Server-A')
trecho2 = ('Fortaleza', 'Curitiba', 'Server-B')
trecho3 = ('Curitiba', 'Belem', 'Server-C')
trecho = []

trecho.append(trecho1)
trecho.append(trecho2)
trecho.append(trecho3)

client = ClientSockClass.ClientSocket(ip)
try:
    # dados = requests.post(f'http://{ip}:{port}/gettoken', json={'email':email}, headers={"Content-Type": "application/json"}).json()
    # if dados['status'] == rq.ConstantsManagement.OK.value:
    #     print('connected')
    #     token = dados['data']
    #     dados = requests.post(f'http://{ip}:{port}/getroute', json={'token':token, 'match':match, 'destination':destination}, headers={"Content-Type": "application/json"}).json()
    #     if dados['status']  == rq.ConstantsManagement.OK.value:
    #         for item in dados['data'][0]:
    #             trecho.append((item[0],item[1], item[2]))
            dados = requests.post(f'http://{ip}:{port}/buy', json={'token':token, 'ip':'127.0.0.0', 'routes':trecho}, headers={"Content-Type": "application/json"}).json()
            if dados['status']  == rq.ConstantsManagement.OK.value:
                print('Compra realizada com sucesso')
                exit(0)
            elif dados['status']  == rq.ConstantsManagement.OPERATION_FAILED.value:
                print('não foi possivel realizar a compra')
                exit(0)
except Exception as e:
    print(f'Erro: {str(e)}')