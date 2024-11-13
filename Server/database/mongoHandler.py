from pymongo import MongoClient

## 
#   @brief: classe responsavel por gerenciar o mongo DB e seus atributos
##
class MongoHandler:

    ##
    # @brief: construtor da classe
    # @param: connect_string - endereco do servidor do mongoDB
    # @param: companhia - nome da companhia que deseja ser acessada
    ##
    def __init__(self, connect_string:str, companhia:str):
        self.connect_string = connect_string
        self.companhia = companhia
        self.client = MongoClient(connect_string)
        self.db = self.client.get_database(companhia)

    ##
    # @brief: metodo responsavel por retornar uma coleção do banco de dados
    # @param: collection_name - nome da coleção que deseja ser acessada
    # @return: coleção do banco de dados do Mongo DB
    ##
    def  __get_collection(self, collection_name:str):
        return self.db.get_collection(collection_name)
    
    ##
    # @brief: metodo responsavel por inserir dados em uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @param: data - dados que deseja ser inserido na coleção
    ##
    def insert_data(self, data:dict, group:str):
        try:
            self.__get_collection(group).insert_one(data)
        except  Exception as e:
            print(f'[DATABASE] Fail to insert data, raise: {str(e)}')
            raise

    ##
    # @brief: metodo responsavel por inserir mais de um dado em uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @param: list_of_data - dados que deseja ser inserido na coleção
    ##
    def insert_many_data(self, list_of_data:list, group:str):
        try:
            self.__get_collection(group).insert_many(list_of_data)
        except Exception as e:
            print(f'[DATABASE] Fail to insert data, raise: {str(e)}')
            raise
        
    ##
    # @brief: metodo responsavel por buscar dados em uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @param: filter - filtro que deseja ser aplicado na busca
    # @return: list - dados da coleção do banco de dados do Mongo DB
    ##
    def get_data_by_filter(self,filter:dict, group:str):
        response = self.__get_collection(group).find(filter)

        data = [item for item in response]
        
        return None if len(data)  == 0 else data
        
    ##
    # @brief: metodo responsavel por buscar todos os dados de uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @return: list - dados da coleção do banco de dados do Mongo DB
    def get_all_itens_in_group(self,  group:str):
        response = self.__get_collection(group).find()
        data = [item for item in response]

        return None if len(data)  == 0 else data
    
    ##
    # @brief: função responsavel por excluir um dado do banco de dados
    # @param: group - nome da coleção que deseja ser acessada
    # @param: filter - filtro que deseja ser aplicado na exclusão
    # return: bool - True se o dado foi excluido com sucesso, False caso contrario
    def  delete_data_by_filter(self, filter:dict, group:str):
        response = self.__get_collection(group).delete_one(filter)

        return True if response.deleted_count > 0 else False

    ##
    # @brief: metodo responsavel por atualizar um dado em uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @param: filter - filtro que deseja ser aplicado na atualização
    # @param: data - dados que deseja ser atualizado na coleção
    # @return: bool - True se o dado foi atualizado com sucesso, False caso contrario
    def update_data_by_filter(self, group, filter, data):
        response = self.__get_collection(group).replace_one(filter, data)

        return True if response.matched_count > 0 else False
    
    ##
    # @brief: metodo responsavel por atualizar muitos dados em uma coleção
    # @param: group - nome da coleção que deseja ser acessada
    # @param: filter - filtro que deseja ser aplicado na atualização
    # @param: data - dados que deseja ser atualizado na coleção
    # @return: bool - True se os dados foram atualizados com sucesso, False caso contrario
    def update_many(self, group, data_list):
        try:
            with self.client.start_session() as session:
                for data in data_list:
                    response = self.__get_collection(group).replace_one(data[0], data[1], session=session)
                    print(response.matched_count)

            return True
        except Exception as e:
            return False

