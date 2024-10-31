from utils.twoPhaseCommit import *
from threading import Lock

class VectorClock():
    def __init__(self, node_id) -> None:
        self.lock = Lock()
        self.clock = [0 for i in range(NODES_COUNT)]
        self.node_id = node_id

    def reset_clock(self) -> None:
        with self.lock:
            self.clock = [0 for i in range(NODES_COUNT)]
        
    def update_clock(self, peers_clock:list[int, int, int]) -> list[int, int, int]:
        with self.lock:
            self.clock = [max(self.clock[i], peers_clock[i]) for i in range(NODES_COUNT)]
        
        return self.clock

    def increment_clock(self) -> list[int, int, int]:
        with self.lock:
            self.clock[self.node_id] += 1

        return self.clock

    def compare_clock(self, peers_clock:list[int, int, int]) -> list[int, int, int]:
        self_bigger = False
        peers_bigger = False

        with self.lock:
            for t_self, t_peer in zip(self.clock, peers_clock):
                if t_self < t_peer:
                    peers_bigger = True
                elif t_self > t_peer:
                    self_bigger = True
        
        if self_bigger and not peers_bigger:
            return 1 #self is bigger
        elif peers_bigger and not self_bigger:
            return 0 #peer is bigger
        else:
            return -1 #concurrent

