from database.mongoHandler import *
from json import load
from utils.database import CollectionsName
from utils.twoPhaseCommit import ServerName

handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.A.value)

with open("graph1.json", "r") as file:
    graph = load(file)
    graph = [{'_id': key, key: value} for key, value in graph.items()]
    handler.insert_many_data(graph, CollectionsName.GRAPH.value)

handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.B.value)

with open("graph2.json", "r") as file:
    graph = load(file)
    graph = [{'_id': key, key: value} for key, value in graph.items()]
    handler.insert_many_data(graph, CollectionsName.GRAPH.value)


handler = MongoHandler(CollectionsName.CONNECT_STRING.value, ServerName.C.value)

with open("graph3.json", "r") as file:
    graph = load(file)
    graph = [{'_id': key, key: value} for key, value in graph.items()]
    handler.insert_many_data(graph, CollectionsName.GRAPH.value)
