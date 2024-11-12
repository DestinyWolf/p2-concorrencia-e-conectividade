from utils.twoPhaseCommit import *
from threading import Lock


##
#   @brief: classe utilizada para o gerenciamento de vetores de relógios lógicos
##
class VectorClock():
    def __init__(self) -> None:
        self.lock = Lock()
        self.clock = [0 for i in range(NODES_COUNT)]

    ##
    #   @brief: método utilizado para resetar o vetor de relógios. Seta todos os valores para 0
    ##
    def reset_clock(self) -> None:
        with self.lock:
            self.clock = [0 for i in range(NODES_COUNT)]

    ##
    #   @brief: método utilizado para atualizar o vetor de relógios. Seta cada valor do relógio para o maior
    #   entre self e peers_clock
    #   @param: peers_clock: vetor relógio a ser utilizado como parâmetro
    #   @return: vetor relógio atualizado
    ##   
    def update_clock(self, peers_clock:list[int, int, int]) -> list[int, int, int]:
        with self.lock:
            self.clock = [max(self.clock[i], peers_clock[i]) for i in range(NODES_COUNT)]
        
        return self.clock

##
#   @brief: método utilizado para incremetar o vetor relógio. Incrementa apenas a posição respectiva ao servidor
#   @brief: index: id do servidor
#   @return: vetor relógio atualizado
##
    def increment_clock(self, index:int) -> list[int, int, int]:
        with self.lock:
            self.clock[index] += 1

        return self.clock

##
#   @brief: método utlizado para comparar dois self a outro relógio
#   @param: peers_clock: vetor relógio a ser usado na comparação
#   @return: 1 se self é maior, 0 se o parâmetro é maior ou -1 se os vetores são concorrentes
##
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

