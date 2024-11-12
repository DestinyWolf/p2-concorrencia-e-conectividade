from database.mongoHandler import *
from json import load
from utils.database import CollectionsName
from utils.twoPhaseCommit import ServerName


##
# Arquivo utilizado para carregar as informações dos grafos de cada servidor no banco de dados
##


handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.A.value)

with open("graph1.json", "r") as file:
    graph = load(file)
    graph_formatted = []
    for key, values in graph.items():
        for u ,v in values.items():
            graph_formatted.append({'_id':f'{key}|{u}',key: {u: v}})
    handler.insert_many_data(graph_formatted, CollectionsName.GRAPH.value)


handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.B.value)

with open("graph2.json", "r") as file:
    graph = load(file)
    graph_formatted = []
    for key, values in graph.items():
        for u ,v in values.items():
            graph_formatted.append({'_id':f'{key}|{u}',key: {u: v}})
    handler.insert_many_data(graph_formatted, CollectionsName.GRAPH.value)



handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.C.value)

with open("graph3.json", "r") as file:
    graph = load(file)
    graph_formatted = []
    for key, values in graph.items():
        for u ,v in values.items():
            graph_formatted.append({'_id':f'{key}|{u}',key: {u: v}})
    handler.insert_many_data(graph_formatted, CollectionsName.GRAPH.value)

