from enum import Enum

NODES_COUNT = 3

class TransactionStatus(Enum):
    PREPARE = "PREPARE"
    READY = "READY"
    COMMIT = "COMMITED"
    ABORTED = "ABORTED"
    DONE = "DONE"

class ServerIds(Enum):
    A = 0
    B = 1
    C = 2

class ServerName(Enum):
    A = 'Server-A'
    B = 'Server-B'
    C = 'Server-C'

SERVERIP:dict = {ServerName.A.value: '127.0.0.1', ServerName.B.value: '127.0.0.2', ServerName.C.value: '127.0.0.3'}

SERVERPORT:dict = {ServerName.A.value: '5000', ServerName.B.value: '5001', ServerName.C.value: '5002'}
