from enum import Enum

NODES_COUNT = 3

class TransactionStates(Enum):
    PREPARE = "prepare"
    READY = "ready"
    COMMIT = "commited"
    ABORTED = "aborted"

class ServerIds(Enum):
    A = 0
    B = 1
    C = 2