import os
from time import sleep

##@brief: gera um meno com duas opções, sim ou não, recebe o input do usuario e retorna
# @param: text - str,  texto a ser mostrado
# @param: clear_str - str, comando de limpar o terminal do SO 
# @return: int, resposta do usuario decrementado de 1
def ysno_menu(text: str, clear_str: str):
    opc = 0
    color_list = ['\033[47;30m', '\033[49;0m']
    while True:
        os.system(clear_str)
        print(text)
        print(f'{color_list[opc*1]}\t1 <- Sim\033[49;0m')
        print(f'{color_list[1-opc]}\t2 <- Nao\033[49;0m')
        str_opc = input('opção: ')
        if str_opc.isnumeric():
            opc = int(str_opc) - 1
            if opc == 0 or opc == 1:
                return opc
        print('opção invalida!')
        sleep(2)

##@brief: gera um meno com n opções, recebe o input do usuario e retorna
# @param: text - str,  texto a ser mostrado
# @param: text_opc - list[str], lista de opções a serem mostradas para o usuario
# @param: clear_str - str, comando de limpar o terminal do SO 
# @return: int, resposta do usuario decrementado de 1
def enumerate_menu(text_opc: list, text: str, clear_str: str):
    color_list = ['\033[47;30m', '\033[49;0m']


    while True:
        opc = 0
        os.system(clear_str)
        print(text)
        for item in text_opc:
            print(f'{text_opc.index(item)+1} <- {color_list[0] if opc == text_opc.index(item) else color_list[1]}\t{item}\033[49;0m')

        str_opc = input('opção: ')
        if str_opc.isnumeric():
            opc = int(str_opc) - 1
            if 0 <= opc < len(text_opc):
                return opc
        print('opção invalida!')
        sleep(2)


