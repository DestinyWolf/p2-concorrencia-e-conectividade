from pymongo import MongoClient

class MongoHandler:
    def __init__(self, connect_string:str, companhia:str):
        self.connect_string = connect_string
        self.companhia = companhia
        self.client = MongoClient(connect_string).get_database(companhia)

    def  __get_collection(self, collection_name:str):
        return self.client.get_collection(collection_name)
    
    def insert_data(self, data:dict, group:str):
        self.__get_collection(group).insert_one(data)

    def insert_many_data(self, list_of_data:list, group:str):
        self.__get_collection(group).insert_many(list_of_data)

    def  get_data_by_filter(self,filter:dict, group:str):
        response = self.__get_collection(group).find(filter)

        data = [item for item in response]
        
        None if len(data)  == 0 else data
        
    def get_all_itens_in_group(self,  group:str):
        response = self.__get_collection(group).find()
        data = [item for  item in response]

        None if len(data)  == 0 else data
        
    def  delete_data_by_filter(self, filter:dict, group:str):
        response = self.__get_collection(group).delete_one(filter)

        True if response.deleted_count > 0 else False
