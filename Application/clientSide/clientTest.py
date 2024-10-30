import socket
from Client.ClientSockClass import *
from Client.requests import Request, ConstantsManagement

def request_func(type, data, token=None):
    sock = ClientSocket(socket.gethostbyname(socket.gethostname()))
    sock.connect()
    request = Request(rq_type=type, rq_data=data, client_token=token)
    rq_json = request.to_json()

    msg_len = len(rq_json)
    send_len = str(msg_len).encode(ConstantsManagement.FORMAT.value)
    send_len += b' ' * (ConstantsManagement.MAX_PKT_SIZE.value - len(send_len))
    sock.client_socket.send(send_len)
    sock.client_socket.send(rq_json.encode(ConstantsManagement.FORMAT.value))
    msg_len =sock.client_socket.recv(ConstantsManagement.MAX_PKT_SIZE.value).decode(ConstantsManagement.FORMAT.value)
    msg = sock.client_socket.recv(int(msg_len)).decode(ConstantsManagement.FORMAT.value)

    sock.end()
    print(msg)
    print()


token = "6602ca3b2497a1abb32ea93f0d02eff123dad0e6b31f9835a0d32b0604d03a64"
#no clients
request_func(ConstantsManagement.GETTOKEN.value, "panda@example.com")

#create clients
request_func(ConstantsManagement.CREATE_USER.value, "pandicorn@example.com")
request_func(ConstantsManagement.CREATE_USER.value, "panda@example.com")
request_func(ConstantsManagement.CREATE_USER.value, "pandicorn@example.com")

#get token
request_func(ConstantsManagement.GETTOKEN.value, "pandas@example.com") #not registered
request_func(ConstantsManagement.GETTOKEN.value, "pandicorn@example.com")

#no tickets
request_func(ConstantsManagement.GETTICKETS.value, None, token) #good token
request_func(ConstantsManagement.GETTICKETS.value, None, token+'ead') #bad token

#check routes
request_func(ConstantsManagement.GETROUTES.value, {"match": "D", "destination": "A"}, token) #good

request_func(ConstantsManagement.GETROUTES.value, {"match": "D", "destination": "A"}, "a7cdf5d0586b392473dd0cd08c9ba833240006a8a7310bf9bc8aefdfaeadb") #bad token

request_func(ConstantsManagement.GETROUTES.value, {"match": "D", "destination": "D"}, token) #bad route

request_func(ConstantsManagement.GETROUTES.value, {"match": "E", "destination": "A"}, token) #bad node

request_func(ConstantsManagement.GETROUTES.value, {"match": "A", "destination": "B"}, token) #no sits left

#buying
request_func(ConstantsManagement.BUY.value, [ ("D","C")], token) #1 sit left


request_func(ConstantsManagement.BUY.value, [ ("D","C")], token) #no sits

request_func(ConstantsManagement.BUY.value, [ ("A","C")], token) #with multiple sits

request_func(ConstantsManagement.BUY.value, [ ("B","D")], token) #bad route

request_func(ConstantsManagement.BUY.value, [ ("A","E")], token) #bad node

request_func(ConstantsManagement.BUY.value, [ ("A","B")], "a7cdf5d0586b392473dd0cd08c9ba833240006a8a73109bc8bf1aefdfaeadb") #bad token

#tickets

request_func(ConstantsManagement.GETTICKETS.value, None, token) #good



