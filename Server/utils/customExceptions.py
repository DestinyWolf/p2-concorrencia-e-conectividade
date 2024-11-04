##
#   @brief Subclasse da classe Exception. Gerada quando a autenticação de um token falha
##
class InvalidTokenException(Exception):
    def __init__(self, *args: object, msg:str = ''):
        super().__init__(*args)
        self.msg = msg
