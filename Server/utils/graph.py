from utils.database import *
from threading import Lock
import networkx as nx
from database.mongoHandler import *


class RoutesGraph:

    graph_lock = Lock() #mutex para o acesso ao grafo

    def __init__(self, server_id):
        self.path_locks:dict[str:Lock] = {}
        self.graph:nx.DiGraph = self.__init_graph(server_id)
        self.destinations:list[str] = list(self.graph)
    
##
#   @brief: Método utilizado inicializar o grafo de rotas
#   @return new_grah - DiGraph carregado do arquivo de grafos do servidor.
#   Caso contrário, retorna um DiGraph vazio
##
    def __init_graph(self, server_name):
        db_handler = MongoHandler(connect_string= CollectionsName.CONNECT_STRING.value, companhia=server_name)
        new_graph = nx.DiGraph()
        
        adjacency_dict:dict = db_handler.get_all_itens_in_group(CollectionsName.GRAPH.value)[0]
        
        del adjacency_dict['_id']

        if adjacency_dict:
            for node, edges in adjacency_dict.items():
                for neighbor, attrs in edges.items():
                    self.path_locks[(node, neighbor)] = Lock()
                    attrs["company"] = {server_name: attrs['globalWeight']}
                    new_graph.add_edge(node, neighbor, **attrs)

        return new_graph

    def merge_graph(self, peers_adjacency:dict, peer_id:str):
        with RoutesGraph.graph_lock:
            for edge, weight in peers_adjacency.items():
                edge = eval(edge) #convertendo str->tuple

                if self.graph.has_edge(edge[0], edge[1]): 

                    with self.path_locks[edge]: #bloqueando mutex do trecho
                        self.graph[edge[0]][edge[1]]["company"].update([(peer_id, weight)])
                
                else:
                    self.graph.add_edge(edge[0],edge[1], globalWeight= weight, company={peer_id: weight})
                
                self.__update_global_edge_weight(edge)

    def __update_global_edge_weight(self, edge:tuple):
        #Atualizando peso global 
        weight_values = set(self.graph[edge[0]][edge[1]]["company"].values())

        if weight_values.pop() == 999 and len(weight_values) == 0:
            self.graph[edge[0]][edge[1]]["globalWeight"] = 999
        else:
            self.graph[edge[0]][edge[1]]["globalWeight"] = 1

    ##
#   @brief: Método busca os três menores caminhos disponíveis entre a origem e o destino.
#
#   @param: match - nó de origem
#   @param: destination - nó de destino
#   @return: Lista com as informações dos voos nos caminhos. Retorna None caso: a
#   origem seja igual ao destino, a origem ou o destino não sejam nós do grafo ou
#   nenhum caminho disponível seja encontrado
## 
    def search_route(self, match:str, destination:str):
        try:
            if match == destination:
                raise ValueError()
            
            with RoutesGraph.graph_lock:
                shortest_paths:list = list(nx.shortest_simple_paths(self.graph, source=match, target=destination,weight="globalWeight"))

            for path in shortest_paths:
                weight = sum(self.graph[u][v]['globalWeight'] for u, v in zip(path[:-1], path[1:]))
                if weight >= 999:
                    shortest_paths.remove(path)
                    
            return self.match_route_to_company(shortest_paths[:3])
        except (nx.NetworkXNoPath, nx.NetworkXError, ValueError) as err:
            return None

    def match_route_to_company(self, routes:list):
        matched_routes = []

        for route in routes:
            temp = []
            for i in range(len(route) - 1):
                segment = self.graph[route[i]][route[i+1]]
                company = next((comp for comp, weight in segment["company"].items() if weight == 1), None)
                if company:
                    temp.append([route[i], route[i+1], company])
            matched_routes.append(temp)

        return matched_routes

from utils.twoPhaseCommit import ServerName
a = RoutesGraph(ServerName.A.value)
print(a.search_route('A','B'))