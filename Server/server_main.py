from SocketManagement import *
from threading import *
from concurrent.futures import *
from database.mongoHandler import *
from utils.database import CollectionsName
from utils.twoPhaseCommit import *
from ClientHandler import *
from TwoPhaseCommitNode import *

from flask import Flask, request, jsonify
from flask_cors import CORS
from time import sleep
import requests

app = Flask(__name__)
CORS(app)

node_info = TwoPhaseCommitNode(ServerIds.A, ServerName.A)


## Flask App Endpoints ##
@app.route('/')
def home():
    return {'msg':'working'}

@app.route('/serverstatus')
def new_server():    
    return {'msg':'connected'}, 200


#endpoint para a lista de adjacencias, retorna toda a lista de adjacencias de uma companhia

@app.route('/getgraph')
def get_graph():
    global node_info
    return jsonify(node_info.db_handler.get_all_itens_in_group(CollectionsName.GRAPH.value)), 200

'''
@app.route('/firstphase')
def  first_phase():
    #1 fase do 2pc
    rq = request.get_json()

    trans_id = rq['id']
    data = rq['data']
    time = rq['time']

    #thread pool

    return {'id':'transid','msg':'accept'}, 200 if 'algo' else {'id':'transid','msg':'denied'}, 200
    
@app.route('/secondphase')
def  second_phase():
    #2 fase do 2pc
    rq = request.get_json()
    trans_id = rq['id']
    msg = rq['msg']

    thread pool
    return {'msg':'ok'}, 200 if 'algo' else {'msg':'not ok'}, 200

@app.route('/notfinish')
def not_finish():
    rq = request.get_json()
    whoisme = rq['whoIsMe']
    trans_id = rq['id']
    msg = rq['msg']

    #se camila fizer

    return {'id':'transid', msg:'do'},200 if 'algo' else  {'id':'transid', 'msg':'not ok'}, 200

@app.route('/updateroute')
def update_route():
    rq = request.get_json()
    whoisme = rq['whoIsMe']
    route = rq['routeToUpdate']
    msg = rq['msg']

    #camila se vira pra implementar

    return {'msg':'success'}, 200

# funcao de inicializacao do servidor que faz o envio dos rq informando um novo servidor
payload = {'name':'nome da companhia',  'ip':cm.HOST}
for (name, ip) in serveriplist:
    rs = requests.post(f'http://{ip}/newserver', data=payload).json()

    #mudar o tratamento depois apenas para testes
    if rs['msg'] == 'fail':
        print(f'falha ao se comunicar com o servidor:{name}')
'''



def process_client(new_client: ClientHandler):
    pass




def socket_client_handler():
    global node_info
    socket_manager = SocketManager(SERVERIP[node_info.host_name.value])
    socket_manager.init_socket()

    with ThreadPoolExecutor(max_workers=10) as exec:
        while True:
            try:
                (conn, client) = socket_manager.server_socket.accept()
                new_client = ClientHandler(conn, client)
                exec.submit(process_client, new_client)
            except socket.error as er:
                node_info.logger.error(f"Error accepting new connection. Error: {er} Retrying...\n")
            except KeyboardInterrupt:
                return -1


def batch_executor():
    pass




def new_server_pool():
    global node_info
    up_links = {server: False for server in SERVERIP if server!=node_info.host_name.value}

    while True:   
        for server, status in up_links.items():
            try:
                response = requests.get('http://'+SERVERIP[server]+':5000/serverstatus', timeout=3)

                if response.status_code == 200 and not status:
                    up_links.update({server: True})
                    response = requests.get('http://'+SERVERIP[server]+':5000/getgraph', timeout=3)
                    #node_info.graph.merge_graph(response.json(), server)  bug: refactor merge_graph
                    node_info.logger.info(f'New connection with {server}. Routes merged!')

            except (requests.Timeout, requests.ConnectionError):
                up_links[server] = False
                #unmerge graph

        sleep(3)


    
            

        





if __name__ == "__main__":
    try:
        #socket_listener_thread = Thread(target=socket_client_handler)
        #batch_executor_thread = Thread(target=batch_executor)
        new_server_connections = Thread(target=new_server_pool)
        
        #flask_app_thread.start()
        #socket_listener_thread.start()
        #batch_executor_thread.start()
        new_server_connections.start()

        
        app.run(host=SERVERIP[ServerName.A.value],  port=5001, debug=True)
    except KeyboardInterrupt:    
        #flask_app_thread.join()
        #socket_listener_thread.join()
        #batch_executor_thread.join()
        exit(-1)






