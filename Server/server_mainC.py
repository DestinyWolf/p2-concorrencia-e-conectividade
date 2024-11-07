from Server.SocketManagement import *
from threading import *
from concurrent.futures import *
from Server.database.mongoHandler import *
from Server.utils.database import CollectionsName
from Server.utils.twoPhaseCommit import *
from Server.ClientHandlerClass import *
from Server.TwoPhaseCommitNode import *
from Server.TransactionCoordinatorNode import *
from Server.TransactionManagerNode import *
from Server.utils.socketCommunicationProtocol import *
from flask import Flask, request, jsonify
from flask_cors import CORS
from time import sleep
from datetime import *
from heapq import *
import requests
from Server.utils.customExceptions import *
app = Flask(__name__)
CORS(app)

node_info = TwoPhaseCommitNode(ServerIds.C, ServerName.C)
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
'''
@app.route('/notfinished')
def not_finish():
    rq = request.get_json()
    whoisme = rq['whoIsMe']
    trans_id = rq['id']
    msg = rq['msg']

    #se camila fizer

    return {'id':'transid', msg:'do'},200 if 'algo' else  {'id':'transid', 'msg':'not ok'}, 200


'''

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

def process_client(client: ClientHandler):
    global requests_queue, node_info, tc
    request:Request = client.receive_pkt()
    
    if not request:
        client.conn.close()
        return
    
    try:
        response:Response = Response()
        type = ConstantsManagement(request.rq_type).name

        # Autenticação do token
        if type != "CREATE_USER" and type != "GETTOKEN":
            client.auth_token(node_info.db_handler,request.client_token)
        
        #Seleção do tratamento adequadoda requisição
        match type:
            case "CREATE_USER":
                response.data = client.create_user(request.rq_data, node_info.db_handler) # type: ignore
                if response.data:
                    response.status = ConstantsManagement.OK
                    response.rs_type = ConstantsManagement.TOKEN_TYPE
                else:
                    response.status = ConstantsManagement.OPERATION_FAILED
                    response.rs_type = ConstantsManagement.NO_DATA_TYPE
            case "GETTOKEN":
                print('connected')
                response.data = client.get_token(email=request.rq_data, db_handler=node_info.db_handler) # type: ignore
                response.status = ConstantsManagement.OK
                response.rs_type = ConstantsManagement.TOKEN_TYPE
            
            case "GETROUTES":
                response.data = node_info.graph.search_route(request.rq_data['match'], request.rq_data['destination']) # type: ignore

                if response.data:
                    response.status = ConstantsManagement.OK
                    response.rs_type = ConstantsManagement.ROUTE_TYPE
                else:
                    response.status = ConstantsManagement.NOT_FOUND
                    response.rs_type = ConstantsManagement.NO_DATA_TYPE
            
            case "BUY":
                transaction = tc.setup_transaction(request.rq_data, str(client.addr[0]))
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
                    response.data = Ticket(request.client_token, request.rq_data).to_json()

                    node_info.db_handler.insert_data(response.data, CollectionsName.TICKET.value)

                else:
                    response.status = ConstantsManagement.OPERATION_FAILED
                    response.rs_type = ConstantsManagement.NO_DATA_TYPE
                    response.data = None
            
            case "GETTICKETS":
                response.data = client.get_tickets(request.client_token, node_info.db_handler)

                if response.data:
                    response.status = ConstantsManagement.OK
                    response.rs_type = ConstantsManagement.TICKET_TYPE
                else:
                    response.status = ConstantsManagement.NOT_FOUND
                    response.rs_type = ConstantsManagement.NO_DATA_TYPE
            
            case _: #tipo de requisição inválido
                raise ValueError(f"[SERVER] {client.addr} No request type named {request.rq_type}")

    
    except InvalidTokenException: #autenticação do token falhou
        response.status = ConstantsManagement.INVALID_TOKEN
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    except (KeyError, ValueError) as err: #parâmetros inválidos
        response.status = ConstantsManagement.NOT_FOUND if err == KeyError else ConstantsManagement.OPERATION_FAILED
        response.data = None
        response.rs_type = ConstantsManagement.NO_DATA_TYPE

    response.status = response.status.value
    response.rs_type = response.rs_type.value
    
    #Envio da resposta
    client.send_pkt(response)
    client.conn.close()
    
    return





def socket_client_handler():
    global node_info
    socket_manager = SocketManager(SERVERIP[node_info.host_name.value])
    socket_manager.init_socket()

    with ThreadPoolExecutor(max_workers=10) as exec:
        while True:
            try:
                (conn, client) = socket_manager.server_socket.accept()
                new_client = ClientHandler(conn, client)
                print('connected')
                exec.submit(process_client, new_client)
            except socket.error as er:
                node_info.logger.error(f"Error accepting new connection. Error: {er} Retrying...\n")
            except KeyboardInterrupt:
                return -1

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


'''
def batch_executor():
    global node_info, tc, requests_queue, batch_execution_results


    
    with ThreadPoolExecutor(max_workers=5) as exec:    
        while True:
            with queue_lock:

                if len(requests_queue) == 0:
                    sleep(1)
                    continue
            
                heapify(requests_queue)
                batch = []
                routes_in_batch = set()

                for i in range(len(requests_queue)):
                    
                    keys, data = heappop(requests_queue)
                    transaction, server, timestamp = keys
                    event, task = data
                
                    if transaction.__class__ == Transaction and routes_in_batch.isdisjoint(transaction.intentions):
                        routes_in_batch = routes_in_batch.union(set(transaction.intentions))
                        batch.append((transaction, task, event))

                    elif transaction.__class__ == TransactionProtocolState:
                        if routes_in_batch.isdisjoint(transaction.intentions[node_info.host_name.value]):
                            routes_in_batch = routes_in_batch.union(set(transaction.intentions[node_info.host_name.value]))
                            batch.append((transaction, task, event))
                        else:
                            heappush(requests_queue, (keys, data))

                    else:
                        heappush(requests_queue, (keys, data))
                    
                    if len(batch) == 5:
                        break
            
            print('loop')
            futures = {}
            for request in batch:
                futures[exec.submit(request[1], request[0])] = (request[0].transaction_id, request[2])

            for future in as_completed(futures):
                result = future.result()
                with results_lock:
                    batch_execution_results[futures[future][0]] = result
                futures.get(future)[1].set()            

'''
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
        socket_listener_thread = Thread(target=socket_client_handler)
        batch_executor_thread = Thread(target=batch_executor)
        new_server_connections = Thread(target=new_server_pool, daemon=True)
        

        socket_listener_thread.start()
        batch_executor_thread.start()
        new_server_connections.start()

        
        app.run(host=SERVERIP[ServerName.C.value],  port=SERVERPORT[ServerName.C.value])
        exit(-1)
    finally:
        socket_listener_thread.join()
        batch_executor_thread.join()
        pass


<<<<<<< Updated upstream
'''

a = Transaction(transaction_id='000', intentions=[('A','C'),('A','B')],timestamp=[0,0,0])
b = Transaction(transaction_id='001', intentions=[('A','D'), ('A','E')],timestamp=[0,0,1])
c = Transaction(transaction_id='002', intentions= [('A','D')],timestamp=[0,1,1])
a2 = Transaction(transaction_id='003', intentions=[('B','C'),('A','D1')],timestamp=[1,1,1])
b2 = Transaction(transaction_id='004', intentions=[('A','0D'), ('A','E1')],timestamp=[2,1,1])
c2 = Transaction(transaction_id='005', intentions= [('A','D2')],timestamp=[2,2,1])
a3 = Transaction(transaction_id='006', intentions=[('A','C5'),('A6','D')],timestamp=[3,3,3])
b3 = Transaction(transaction_id='007', intentions=[('A','D8'), ('A','E0')],timestamp=[4,4,4])
c3 = Transaction(transaction_id='008', intentions= [('A9','D')],timestamp=[4,5,5])


def test(trans):
    return f'{TransactionStatus.DONE.value} - {trans.transaction_id}'

def aaaa(transaction):
    finish_event = Event()

    heap_entry = ((transaction, datetime.now()) , (finish_event,test))
    
    with queue_lock:
        heappush(requests_queue, heap_entry)
        
    finish_event.wait()

    with results_lock:
        result = batch_execution_results.pop(transaction.transaction_id)
    
    print(result)

q = Thread(target=aaaa, args=[a])
w = Thread(target=aaaa, args=[b])
e = Thread(target=aaaa, args=[c])
q2 = Thread(target=aaaa, args=[a2])
w2 = Thread(target=aaaa, args=[b2])
e2 = Thread(target=aaaa, args=[c2])
q3 = Thread(target=aaaa, args=[a3])
w3 = Thread(target=aaaa, args=[b3])
e3 = Thread(target=aaaa, args=[c3])

q.start()
w.start()
e.start()
q2.start()
w2.start()
e2.start()
q3.start()
w3.start()
e3.start()

batch_executor_thread = Thread(target=batch_executor)
batch_executor_thread.start()

q.join()
w.join()
e.join()
q2.join()
w2.join()
e2.join()
q3.join()
w3.join()
e3.join()

'''

=======
>>>>>>> Stashed changes
