from utils.twoPhaseCommit import *
from TwoPhaseCommitNode import *
from Transaction import *
import requests


##
#   @brief: classe utilizada para gerenciar as requisições de compra recebidas de outros servidores
##
class TransactionManager(TwoPhaseCommitNode):

    def __init__(self, host_id, host_name, host_port=8000):
        super().__init__(host_id, host_name, host_port)
        self.logger.name = f"{type(self).__name__} - {self.host_name.value}"
            
    ##
    #   @brief: método utilizado adquirir os locks dos trechos de interesse da transação e verificar se 
    #   a disponibilidade de assentos
    #   @param: transaction: transação a ser executada
    #   @return: decisão do servidor
    ##
    def handle_prepare_RPC(self, transaction:Transaction) -> str:
        #Verificando se a transação já foi executada
        recovered_transaction = self.db_handler.get_data_by_filter({'_id': transaction.transaction_id}, CollectionsName.LOG.value)
        
        if recovered_transaction: #transação já executada anteriormente
            self.logger.warning(f"Transaction {transaction.transaction_id} already exists.")
            return recovered_transaction[0]['status']
        else: #nova transação
            #Write transaction to log
            transaction.status = TransactionStatus.PREPARE
            self.db_handler.insert_data(transaction.to_db_entry(), CollectionsName.LOG.value)
            self.logger.info(f"Transaction {transaction.transaction_id} accepted")
        
        #Adquirindo locks dos trechos de interesse
        for route in transaction.intentions: #locking routes
            self.graph.path_locks.get(route).acquire()
        
        # Verificando disponibilidade dos trechos
        for route in transaction.intentions: #checking if there are sits available
            if self.graph.graph[route[0]][route[1]]['sits'] == 0:
                transaction.status = TransactionStatus.ABORTED
                break

        if transaction.status == TransactionStatus.PREPARE:
            transaction.status = TransactionStatus.READY 
        else:       
            for route in transaction.intentions: #liberando locks caso a operação seja abortada
                self.graph.path_locks.get(route).release()
                    
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id':transaction.transaction_id}, transaction.to_db_entry())
        self.logger.info(f"{transaction.transaction_id} -> {transaction.status.value}")
        return transaction.status.value

    ##
    #   @brief: método utilizado para executar as alterações de commit
    #   @param: recovered_transaction: transação a ser executada carregada do banco de dados
    #   @return: retorna o status final da transação
    ##
    def handle_commit_RPC(self, recovered_transaction:Transaction) -> str:
        
        
        if recovered_transaction.status == TransactionStatus.READY:                        
            #Atualizando status da transação
            recovered_transaction.status = TransactionStatus.COMMIT
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': recovered_transaction.transaction_id}, recovered_transaction.to_db_entry())
            self.logger.info(f'Transaction {recovered_transaction.transaction_id} {recovered_transaction.status.value}')

            new_values = []
            #Atualizando o numero de assentos das rotas de interesse
            for route in recovered_transaction.intentions:
                u , v = route
                self.graph.graph[u][v]['sits'] -= 1
                
                # Atualizando peso local
                if self.graph.graph[u][v]['sits'] == 0:
                    self.graph.graph[u][v]['weight'] = 999
                    self.graph.graph[u][v]['company'][self.host_name.value] = 999

                    #Atualizando peso global
                    self.graph.update_global_edge_weight((u,v))
                    
                    #Notificando peers sobre a indisponibilidade do assento
                    for peer, ip in SERVERIP.items():
                        if peer != self.host_name.value:
                            try:
                                response = requests.post(f'http://{ip}:{SERVERPORT[peer]}/updateroute', json={'whoIsMe': self.host_name.value, 'routeToUpdate': route, 'msg':999}, headers={"Content-Type": "application/json"})
                            except Exception:
                                continue
                
                attrs = self.graph.graph[u][v].copy()
                del attrs['company']
                del attrs['globalWeight']
                new_values.append(({'_id':f'{u}|{v}'}, {'_id': f'{u}|{v}', u:{v:attrs}}))
                self.graph.path_locks[(u, v)].release()
            
            self.db_handler.update_many(CollectionsName.GRAPH.value, new_values)
        else: #transação não está no status de READY
            self.logger.info(f"Illegal commit for {recovered_transaction.transaction_id}. Current state is {recovered_transaction.status}")

        recovered_transaction.status = TransactionStatus.DONE
        self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': recovered_transaction.transaction_id}, recovered_transaction.to_db_entry())
        self.logger.info(f'Transaction {recovered_transaction.transaction_id} {recovered_transaction.status.value}')
        
        return recovered_transaction.status.value
    

     ##
    #   @brief: método utilizado para executar as alterações de abort
    #   @param: recovered_transaction: transação a ser executada carregada do banco de dados
    #   @return: retorna o status final da transação
    ##
    def handle_abort_RPC(self, recovered_transaction: Transaction) -> str:

        if recovered_transaction.status == TransactionStatus.READY:
        
            recovered_transaction.status = TransactionStatus.ABORTED
            self.db_handler.update_data_by_filter(CollectionsName.LOG.value, {'_id': recovered_transaction.transaction_id}, recovered_transaction.to_db_entry())
            self.logger.warning(f'{recovered_transaction.transaction_id} -> ABORTED')

            for route in recovered_transaction.intentions:
                self.graph.path_locks[route].release()

        else:
            self.logger.warning(f"Illegal abort for {recovered_transaction.transaction_id}. Current state is {recovered_transaction.status.value}") 


        return recovered_transaction.status.value

