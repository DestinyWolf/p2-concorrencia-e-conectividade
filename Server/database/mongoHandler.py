from pymongo import MongoClient, PyMongoError

class MongoHandler:
    def __init__(self, connect_string:str, companhia:str):
        self.connect_string = connect_string
        self.companhia = companhia
        self.client = MongoClient(connect_string)
        self.db = self.client.get_database(companhia)

    def  __get_collection(self, collection_name:str):
        return self.db.get_collection(collection_name)
    
    def insert_data(self, data:dict, group:str):
        try:
            self.__get_collection(group).insert_one(data)
        except  Exception as e:
            print(f'[DATABASE] Fail to insert data, raise: {str(e)}')
            raise


    def insert_many_data(self, list_of_data:list, group:str):
        try:
            self.__get_collection(group).insert_many(list_of_data)
        except Exception as e:
            print(f'[DATABASE] Fail to insert data, raise: {str(e)}')
            raise
        
    def get_data_by_filter(self,filter:dict, group:str):
        response = self.__get_collection(group).find(filter)

        data = [item for item in response]
        
        return None if len(data)  == 0 else data
        
    def get_all_itens_in_group(self,  group:str):
        response = self.__get_collection(group).find()
        data = [item for item in response]

        return None if len(data)  == 0 else data
        
    def  delete_data_by_filter(self, filter:dict, group:str):
        response = self.__get_collection(group).delete_one(filter)

        return True if response.deleted_count > 0 else False

    def update_data_by_filter(self, group, filter, data):
        response = self.__get_collection(group).replace_one(filter, data)

        return True if response.matched_count > 0 else False
    
    def update_many(self, group, data_list):
        try:
            with self.client.start_session() as session:
                for data in data_list:
                    self.__get_collection(group).replace_one(data[0], data[1], session=session)


            return True
        except PyMongoError as e:
            return False

