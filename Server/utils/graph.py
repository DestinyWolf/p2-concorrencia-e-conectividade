from utils.database import *
from threading import Lock
import networkx as nx
from database.mongoHandler import *

##
#   @brief: Classe utilizada para o gerenciamento do grafo que armazena as informações dos trechos
##

class RoutesGraph:

    graph_lock = Lock() #mutex para o acesso ao grafo completo

    def __init__(self, server_name:str):
        self.path_locks:dict[str:Lock] = {} #mutexes para os trechos deste servidor
        self.graph:nx.DiGraph = self.__init_graph(server_name) #grafo de rotas
        self.destinations = set(self.graph) #destinos disponibilizados apenas por este servidor
    
##
#   @brief: Método utilizado inicializar o grafo de rotas
#   @param: server_name nome do servidor sendo inicializado
#   @return new_grah - DiGraph carregado do banco de dados
#   Caso contrário, retorna um DiGraph vazio
##
    def __init_graph(self, server_name:str):
        db_handler = MongoHandler(connect_string= CollectionsName.CONNECT_STRING.value, companhia=server_name)
        new_graph = nx.DiGraph()
        
        adjacency_dict = db_handler.get_all_itens_in_group(CollectionsName.GRAPH.value)
        
        if adjacency_dict:
            # Adicionando vertices e arestas ao grafo
            for edge in adjacency_dict:
                del edge['_id']
                u , data = edge.popitem()
                v, attrs = data.popitem()
                self.path_locks[(u,v)] = Lock()
                attrs['globalWeight'] = attrs['weight'] #globalWeight -> peso da aresta considerando os pesos individuais de cada companhia
                attrs["company"] = {server_name: attrs['weight']}
                new_graph.add_edge(u,v,**attrs)

        return new_graph
##
#   @brief: Método utilizado remover as rotas do servidor passado como parametro do grafo de rotas deste servidor
##
    def unmerge_graph(self, peers_id):
        with RoutesGraph.graph_lock:
            remove_edges = [] #arestas que pertencem apenas ao servidor peer_id
            possible_remove_nodes = set() #nos que não pertencem a este servidor e não possuem arestas conectadas
            
            #Identificando arestas a e vertices a serem removidos
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
            
            #Removendo arestas
            self.graph.remove_edges_from(remove_edges)
            
            #Removendo vertices
            for node in possible_remove_nodes:
                if not self.graph[node] and (node not in self.destinations):
                    self.graph.remove_node(node)
          
            
                    
##
#   @brief: Método utilizado incluir as rotas passadas como parametro ao grafo de trechos deste servidor. 
#   Preserva as arestas de ambos os grafos
##
    def merge_graph(self, peers_adjacency:list, peer_id:str):
        with RoutesGraph.graph_lock:
        
            for edge in peers_adjacency:
                del edge['_id']
                u , data = edge.popitem()
                v, attrs = data.popitem()
                
                if self.graph.has_edge(u,v): #aresta já existe
                    self.graph[u][v]["company"].update([(peer_id, attrs['weight'])])
                
                else: #aresta não existe
                    self.graph.add_edge(u,v, globalWeight= attrs['weight'], company={peer_id: attrs['weight']})
                
                self.update_global_edge_weight((u,v)) #atualiza peso globa da aresta

##
#   @brief: Método utilizado incluir atualizar peso global da aresta passada
#   @param: edge: aresta ser atualizada
##
    def update_global_edge_weight(self, edge:tuple):
        
        weight_values = set(self.graph[edge[0]][edge[1]]["company"].values())

        if weight_values.pop() == 999 and len(weight_values) == 0: #verificando se existe pelo menos 1 servidr com disponibilidade
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

##
#   @brief: Método relaciona cada trecho passad a umm servidor que possua disponibilidade para aquela rota
#   @param: routes - lista de trechos no formato [[node1,...,nodeN],..., [node1,...,nodeN]]
#   @return: lista de rotas incluindo o nome da companhia para cada trecho no formato [[match1, destnation1, Server1], ..., [matchN, destinationN, ServerN]]
##
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
