from SocketManagement import *
from threading import *
from concurrent.futures import *
from database.mongoHandler import *
from utils.database import CollectionsName
from utils.twoPhaseCommit import *
from ClientHandlerClass import *
from TwoPhaseCommitNode import *
from TransactionCoordinatorNode import *
from TransactionManagerNode import *
from utils.socketCommunicationProtocol import *
from flask import Flask, request, jsonify
from flask_cors import CORS
from time import sleep
from datetime import *
from heapq import *
import requests
from utils.customExceptions import *
app = Flask(__name__)
CORS(app)

node_info = TwoPhaseCommitNode(ServerIds.A, ServerName.A)
tc = TransationCoordinator(node_info.host_id, node_info.host_name)
tm = TransactionManager(node_info.host_id, node_info.host_name)

queue_lock = Lock()
requests_queue = []

results_lock = Lock()
batch_execution_results = {}

## Flask App Endpoints ##
@app.route('/')
def home():
    return {'msg':'working'}

@app.route('/serverstatus')
def new_server(): 
    try:   
        return {'msg':'connected'}, 200
    except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError, requests.Timeout):
        pass


#endpoint para a lista de adjacencias, retorna toda a lista de adjacencias de uma companhia

@app.route('/getgraph')
def get_graph():
    global node_info
    try:
        return jsonify(node_info.db_handler.get_all_itens_in_group(CollectionsName.GRAPH.value)), 200
    except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError, requests.Timeout):
        pass


@app.route('/newtransaction', methods=['POST'])
def  new_transaction():
    global tm, requests_queue, queue_lock, node_info
    
    rq = request.get_json()
    print (f'{rq}')
    coordinator = rq.pop('coordinator')
    id = rq.pop('transaction_id')
    timestamp = rq.pop('timestamp')
    participants = set(rq.pop('participants'))
    intentions = [tuple(i) for i in rq.pop('intentions')]

    transaction = Transaction(coordinator, id, participants,intentions, timestamp)


    finish_event = Event()

    
    heap_entry = ((transaction,coordinator, datetime.now()), (finish_event, tm.handle_prepare_RPC))
    
    with queue_lock:
        heappush(requests_queue, heap_entry)
        
    finish_event.wait()

    with results_lock:
        result = batch_execution_results.pop(transaction.transaction_id)

    try:
        return {'id':transaction.transaction_id,'msg': result}, 200
    except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError, requests.Timeout):
        pass
    

@app.route('/committransaction', methods=['POST'])
def  commit_transaction():
    global tm, requests_queue, queue_lock, node_info
    
    rq = request.get_json()
    print (f'{rq}')
    id = rq.pop('transaction_id')
    decision = rq.pop('decision')
    

    transaction:Transaction = Transaction()
    transaction.load_transaction_from_db(id, node_info.db_handler)
    transaction.decision = decision
    finish_event = Event()

    task = tm.handle_commit_RPC if decision == TransactionStatus.COMMIT.value else tm.handle_abort_RPC
    heap_entry = ((transaction,transaction.coordinator, datetime.now()), (finish_event, task))
    
    with queue_lock:
        heappush(requests_queue, heap_entry)
        
    finish_event.wait()

    with results_lock:
        result = batch_execution_results.pop(transaction.transaction_id)
    try:
        return {'id':transaction.transaction_id,'msg': result}, 200
    except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError, requests.Timeout):
        pass

@app.route('/updateroute', methods=['POST'])
def update_route():
    global node_info

    rq = request.get_json()

    whoisme = rq['whoIsMe']
    route= tuple(rq['routeToUpdate'])
    msg = rq['msg']
    
    update_route(route, whoisme, msg)
    try:
        return {'msg':'success'}, 200
    except (ConnectionAbortedError, ConnectionError, ConnectionRefusedError, requests.Timeout):
        pass

def update_route(route, peer, msg):
    global node_info
    u,v=route
    try:
        with node_info.graph.path_locks[route]:
            node_info.graph.graph[u][v]['company'][peer] = msg
            node_info.graph.update_global_edge_weight(route)
    except KeyError:
        node_info.graph.graph[u][v]['company'][peer] = msg
        node_info.graph.update_global_edge_weight(route)
        
    

@app.route('/createuser', methods=['POST'])
def create_user():
    global node_info, requests_queue, tc

    rq = request.get_json()
    response = Response()
    try:
        email = rq['email']
        response.data = ClientHandler(1,1).create_user(email, node_info.db_handler)
        print(response.to_json())
        if response.data:
            response.status = ConstantsManagement.OK
            response.rs_type = ConstantsManagement.TOKEN_TYPE
        else:
            response.status = ConstantsManagement.OPERATION_FAILED
            response.rs_type = ConstantsManagement.NO_DATA_TYPE

    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    return {'type':response.rs_type.value, 'data':response.data, 'status':response.status.value},200

@app.route('/gettoken', methods=['POST'])
def get_token():
    global node_info, requests_queue, tc
    rq = request.get_json()
    try:
        response = Response()
        response.data = ClientHandler(1,1).get_token(email=rq['email'], db_handler=node_info.db_handler) # type: ignore
        response.status = ConstantsManagement.OK
        response.rs_type = ConstantsManagement.TOKEN_TYPE
    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    return {'type':response.rs_type.value, 'data':response.data, 'status':response.status.value},200

@app.route('/getroute', methods=['POST'])
def get_route():
    global node_info, requests_queue, tc
    response = Response()
    try:
        rq = request.get_json()
        ClientHandler(1,1).auth_token(node_info.db_handler,rq['token'])
        response.data = node_info.graph.search_route(rq['match'], rq['destination']) # type: ignore

        if response.data:
            response.status = ConstantsManagement.OK
            response.rs_type = ConstantsManagement.ROUTE_TYPE
        else:
            response.status = ConstantsManagement.NOT_FOUND
            response.rs_type = ConstantsManagement.NO_DATA_TYPE

    except InvalidTokenException: #autenticação do token falhou
        response.status = ConstantsManagement.INVALID_TOKEN
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE
    
    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    return {'type':response.rs_type.value, 'data':response.data, 'status':response.status.value},200

@app.route('/buy', methods=['POST'])
def buy():
    global node_info, requests_queue, tc
    rq = request.get_json()

    ip = rq['ip']
    
    response = Response()
    try:
        ClientHandler(1,1).auth_token(node_info.db_handler,rq['token'])
    
        transaction = tc.setup_transaction(rq['routes'], ip)
        finish_event = Event()

        heap_entry = ((transaction,node_info.host_name.value, datetime.now()), (finish_event, tc.prepare_transaction))
        with queue_lock:
            heappush(requests_queue, heap_entry)
            
        
        finish_event.wait()

        with results_lock:
            result = batch_execution_results.pop(transaction.transaction_id)

        
        if result == "DONE":
            response.status = ConstantsManagement.OK
            response.rs_type = ConstantsManagement.TICKET_TYPE
            response.data = Ticket(rq['token'], rq['routes']).to_json()

            node_info.db_handler.insert_data(response.data, CollectionsName.TICKET.value)

        else:
            response.status = ConstantsManagement.OPERATION_FAILED
            response.rs_type = ConstantsManagement.NO_DATA_TYPE
            response.data = None
    except InvalidTokenException: #autenticação do token falhou
        response.status = ConstantsManagement.INVALID_TOKEN
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    return {'type':response.rs_type.value, 'data':response.data, 'status':response.status.value},200

@app.route('/gettickets', methods=['POST'])
def get_tickets():
    global node_info, requests_queue, tc
    rq = request.get_json()
    response = Response()
    try:
        ClientHandler(1,1).auth_token(node_info.db_handler,rq['token'])

        response.data = ClientHandler(1,1).get_tickets(rq['token'], node_info.db_handler)

        if response.data:
            response.status = ConstantsManagement.OK
            response.rs_type = ConstantsManagement.TICKET_TYPE
        else:
            response.status = ConstantsManagement.NOT_FOUND
            response.rs_type = ConstantsManagement.NO_DATA_TYPE
    except InvalidTokenException: #autenticação do token falhou
        response.status = ConstantsManagement.INVALID_TOKEN
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE
    
    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE
    
    return {'type':response.rs_type.value, 'data':response.data, 'status':response.status.value},200

def batch_executor():
    global node_info, tc, requests_queue, batch_execution_results

    with ThreadPoolExecutor(max_workers=1) as exec:
        while True:
            
            with queue_lock:
                
                if len(requests_queue) == 0:
                    continue
                heapify(requests_queue)
                keys, data = heappop(requests_queue)
                
                transaction, server, timestamp = keys
                event, task = data
                
                future = exec.submit(task, transaction)
                
                wait([future])

                result = future.result()
                with results_lock:
                    batch_execution_results[transaction.transaction_id] = result
                
                event.set()

def new_server_pool():
    global node_info
    up_links = {server: False for server in SERVERIP if server!=node_info.host_name.value}
    merged = {server: False for server in SERVERIP if server!=node_info.host_name.value}

    while True:   
        for server, status in up_links.items():
            try:
                response = requests.get(f'http://{SERVERIP[server]}:{SERVERPORT[server]}/serverstatus', timeout=3)

                if response.status_code == 200 and not status:
                    up_links.update({server: True})
                    response = requests.get(f'http://{SERVERIP[server]}:{SERVERPORT[server]}/getgraph', timeout=3)
                    node_info.graph.merge_graph(response.json(), server)
                    merged.update({server: True})
                    node_info.logger.info(f'New connection with {server}. Routes merged!')

            except (ConnectionAbortedError, ConnectionRefusedError, ConnectionError, requests.Timeout, TimeoutError, requests.ConnectionError) as err:
                
                if merged[server]:
                    node_info.graph.unmerge_graph(server)
                    merged[server] = False
                    node_info.logger.info(f'Connection lost with {server}. Routes unmerged!')
                up_links[server] = False
                

        sleep(3)


if __name__ == "__main__":
    try:
        batch_executor_thread = Thread(target=batch_executor)
        new_server_connections = Thread(target=new_server_pool, daemon=True)
        batch_executor_thread.start()
        new_server_connections.start()

        app.run(host=SERVERIP[ServerName.A.value],  port=SERVERPORT[ServerName.A.value], threaded=True)
    except KeyboardInterrupt:    
        exit(-1)
    finally:
        batch_executor_thread.join()
        pass


