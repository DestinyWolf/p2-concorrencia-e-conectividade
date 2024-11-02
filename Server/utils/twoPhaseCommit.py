from enum import Enum

NODES_COUNT = 3

class TransactionStatus(Enum):
    PREPARE = "PREPARE"
    READY = "READY"
    COMMIT = "COMMITED"
    ABORTED = "ABORTED"
    ILLEGAL = "ILLEGAL"

class ServerIds(Enum):
    A = 0
    B = 1
    C = 2

class ServerName(Enum):
    A = 'Server-A'
    B = 'Server-B'
    C = 'Server-C'

class ServerIP(Enum):
    A = 'localhost1'
    B = 'localhost2'
    C = 'localhost3'