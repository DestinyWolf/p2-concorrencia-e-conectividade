from Server.utils.database import *
from threading import Lock
import networkx as nx
from Server.database.mongoHandler import *


class RoutesGraph:

    graph_lock = Lock() #mutex para o acesso ao grafo

    def __init__(self, server_name:str):
        self.path_locks:dict[str:Lock] = {}
        self.graph:nx.DiGraph = self.__init_graph(server_name)
        self.destinations = set(self.graph)
    
##
#   @brief: Método utilizado inicializar o grafo de rotas
#   @return new_grah - DiGraph carregado do arquivo de grafos do servidor.
#   Caso contrário, retorna um DiGraph vazio
##
    def __init_graph(self, server_name:str):
        db_handler = MongoHandler(connect_string= CollectionsName.CONNECT_STRING.value, companhia=server_name)
        new_graph = nx.DiGraph()
        
        adjacency_dict = db_handler.get_all_itens_in_group(CollectionsName.GRAPH.value)
        
        if adjacency_dict:

            for edge in adjacency_dict:
                del edge['_id']
                u , data = edge.popitem()
                v, attrs = data.popitem()
                self.path_locks[(u,v)] = Lock()
                attrs['weight'] = attrs['globalWeight']
                attrs["company"] = {server_name: attrs['weight']}
                new_graph.add_edge(u,v,**attrs)

        return new_graph

    def unmerge_graph(self, peers_id):
        with RoutesGraph.graph_lock:
            remove_edges = []
            possible_remove_nodes = set()
            for edge in self.graph.edges:
                u,v = edge
                self.graph[u][v]['company'].pop(peers_id, None)
                if self.graph[u][v]['company']:
                    self.update_global_edge_weight((u,v))
                else:
                    if edge not in self.path_locks:
                        remove_edges.append(edge)
                        possible_remove_nodes.add(u)
                        possible_remove_nodes.add(v)
            
            
            self.graph.remove_edges_from(remove_edges)
            
            for node in possible_remove_nodes:
                if not self.graph[node] and (node not in self.destinations):
                    self.graph.remove_node(node)
          
            
                    

         

    def merge_graph(self, peers_adjacency:list, peer_id:str):
        with RoutesGraph.graph_lock:
            for edge in peers_adjacency:
                del edge['_id']
                u , data = edge.popitem()
                v, attrs = data.popitem()
                
                if self.graph.has_edge(u,v): 
                    self.graph[u][v]["company"].update([(peer_id, attrs['weight'])])
                
                else:
                    self.graph.add_edge(u,v, globalWeight= attrs['weight'], company={peer_id: attrs['weight']})
                
                self.update_global_edge_weight((u,v))


    def update_global_edge_weight(self, edge:tuple):
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

    def match_route_to_company(self, routes:list): #bug -> provavelmente o break
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