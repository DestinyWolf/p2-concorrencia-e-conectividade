from time import sleep
from Client import requests
from Client import utils
from clientSide import menus
from Client import controller
import os
from Client.ClientSockClass import ClientSocket
from sys import platform



#checar o sistema operaciona em que o codigo esta sendo rodado
if platform == 'linux' or platform == 'linux2' or platform == 'darwin':
    __CLEAR = 'clear'
else:
    __CLEAR =  'cls'

##@brief: Função responsavel pelas operações relacionadas a compra e tratamento de erros no momento da compra
#  @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def buy_route(client: ClientSocket):
    os.system(__CLEAR)
    match = input('digite de onde voce esta saindo: ')
    while match == '':
        match = input('digite de onde voce esta saindo: ')
    
    destination = input('digite para onde voce deseja ir: ')
    while destination == '':
        destination = input('digite para onde voce deseja ir: ')

    (status, data) = controller.search_routes(match=match, destination=destination, client=client)

    color_list = ['\033[47;30m', '\033[49;0m']

    if  status == requests.ConstantsManagement.OK.value:

        i = 0

        opc = 0
        while True:
            os.system(__CLEAR)
            i = 0
            print('selecione uma das rotas: ')
            for flight in data:
                j = 1
                color = color_list[0] if opc == i else color_list[1]
                print(f'\t{color}rota {i + 1}:\033[49;0m')
                for route in flight:
                    path = utils.Route()
                    path.from_string(route)
                    print(f'\t\tconexao {j}: {path.match} a {path.destination}')
                    print(f'\t\tcompania: {path.company}')
                    j += 1

                i += 1
            opc =  int(input('opção: ')) -1

            if 0 <= opc < len(data):
                break
            else:
                print('opção invalida')
                sleep(2)
        old_data = data

        (status, data) = controller.buying(routes=data[opc], client=client)

        if status == requests.ConstantsManagement.OK.value:
            os.system(__CLEAR)
            ticket = requests.Ticket()
            ticket.from_json(data)
            print("Compra efetuada com sucesso!")

            print(f"dados da compra:\n\temail: {ticket.email}\n\tdata: {ticket.timestamp}")


            i = 1
            for item in ticket.routes:
                print(f"\tConexão {i}:")  
                print(f"\t\tDe: {item[0]}\tPara: {item[1]}")
                print(f"\t\tCompanhia: {item[2]}")

                i+=1

            print('pressione enter para retornar ao menu...')
            input()
            submenu_status_ok(client=client)

        elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
            print('falha ao compra passagens, vaga indisponivel\nTente selecionar outra passagem')
            sleep(2)
            buy_route(client=client)

        else:
            while True:
                print('falha na conexao, selecione novamente a rota: ')
                sleep(2)

                os.system(__CLEAR)
                i = 0
                print('selecione uma das rotas: ')
                for flight in old_data:
                    j = 1
                    color = color_list[0] if opc == i else color_list[1]
                    print(f'\t{color}rota {i + 1}:\033[49;0m')
                    for route in flight:
                        path = utils.Route()
                        path.from_string(route)
                        print(f'\t\tconexao {j}: {path.match} a {path.destination}')
                        print(f'\t\tcompania: {path.company}')
                        j += 1

                    i += 1
                opc = int(input('opção: ')) - 1

                if 0 <= opc < len(old_data):
                    break
                else:
                    print('opção invalida')
                    sleep(2)

                (status, data) = controller.buying(routes=old_data[opc], client=client)

                if status == requests.ConstantsManagement.OK.value:
                    os.system(__CLEAR)
                    ticket = requests.Ticket()
                    ticket.from_json(data)
                    print("Compra efetuada com sucesso!")

                    print(f"dados da compra:\n\temail: {ticket.email}\n\tdata: {ticket.timestamp}")
                    i = 1
                    for item in ticket.routes:
                        print(f"\tConexão {i}:")  
                        print(f"\t\tDe: {item[0]}\tPara: {item[1]}")
                        print(f"\t\tCompanhia: {item[2]}")
                        

                        i+=1

                    print('pressione enter para retornar ao menu...')
                    input()
                    submenu_status_ok(client=client)

                elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
                    print('falha ao compra passagens, vaga indisponivel\nTente selecionar outra passagem')
                    sleep(2)
                    buy_route(client=client)
                else:
                    pass


    elif status == requests.ConstantsManagement.NOT_FOUND.value:
        print('Rotas escolhidas nao existem ou nao estao disponivel')
        submenu_status_ok(client)
    elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
        opc = menus.ysno_menu('falha ao buscar rotas, deseja tentar novamente?', __CLEAR)

        if opc == 0:
            buy_route(client=client)
        else:
            submenu_status_ok(client)
    elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
        print('erro de token, por favor tentar novamente mais tarde')
        sleep(2)
        main_loop()
    else:
        print(f"erro de conexão, por favor realizar comprar novamente")
        sleep(2)
        buy_route(client)

##@brief: Função responsavel pelas operações relacionadas a busca de compras de um usuario e tratamento de erros
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def seek_bougths(client: ClientSocket):
    color_list = ['\033[47;30m', '\033[49;0m']
    os.system(__CLEAR)
    (status, data) = controller.search_bougths(client=client)

    if status == requests.ConstantsManagement.OK.value:
        i = 0
        
        print('suas compras:')
        for buy in data:
            ts = buy.get('timestamp')
            route = buy.get('routes')
            print(f'compra {i+1}:\n\tdata: {ts}')
            j = 1
            for item in route:
                print(f"\tConexão {j}:")  
                print(f"\t\tDe: {item[0]}\tPara: {item[1]}")
                print(f'\t\tcompania: {item[2]}')

                j+=1
            i += 1

        print('pressione enter para retornar ao menu...')
        input()
        submenu_status_ok(client=client)
    elif status == requests.ConstantsManagement.NOT_FOUND.value:
        print('Nao existem comprar associadas a essa conta')
        print('\npressione enter para retornar ao menu...')
        input()
        submenu_status_ok(client=client)
    elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
        print('falha ao buscar compras, deseja tentar novamente? ')
        opc = menus.ysno_menu('falha ao buscar compras, deseja tentar novamente? ', __CLEAR)

        if opc == 0:
            seek_bougths(client=client)
        else:
            menu(client)
    else:
        print('falha na conexao')
        sleep(2)
        main_loop()

##@brief: Função responsavel pelas operações relacionadas ao status ok no menu
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def submenu_status_ok(client: ClientSocket):
    opc = menus.enumerate_menu(['Comprar Passagem', 'Consultar Compras', 'Voltar'], 'selecione uma das opcoes abaixo:', __CLEAR)
    if opc == 0:
        buy_route(client)
    elif opc == 1:
        seek_bougths(client)
    else:
        menu(client)

##@brief: Função responsavel pelas operações relacionadas ao status de token invalido no menu
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
# @param: old_opc - int, ultima opção selecionada pelo usuario
def submenu_status_token(client: ClientSocket, old_opc):
    color_list = ['\033[47;30m', '\033[49;0m']
    while True:
        opc = 0
        print('Usuario nao encontrado')
        if old_opc == 1:
            opc = menus.ysno_menu('deseja criar uma conta? ', __CLEAR)

            if opc == 0:
                email = input('digite seu email: ')

                while email.find('@') == -1 or len(email) < 5:
                    email = input('por favor informe um email valido: ')

                (status, data) = controller.create_account(email, client)

                if status == requests.ConstantsManagement.OK.value:
                    client.token = data
                    submenu_status_ok(client)
                elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
                    pass
                else:
                    print("não foi possivel criar conta, por favor tentar novamente mais tarde")
                    sleep(2)
                    main_loop()
            else:
                while True:
                    opc = menus.enumerate_menu(['Tentar novamente', 'Sair'], 'Escolha uma opcao:', __CLEAR)

                    if opc == 0:
                        email = input('digite seu email: ')

                        while email.find('@') == -1 or len(email) < 5:
                            email = input('por favor informe um email valido: ')

                        (status, data) = controller.connect(email, client)

                        if status == requests.ConstantsManagement.OK.value:
                            client.token = data
                            submenu_status_ok(client)
                        elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
                            pass
                        elif status == requests.ConstantsManagement.NOT_FOUND.value:
                            print('usuario nao encontrado')
                            sleep(2)
                            submenu_create_account(client)
                        elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
                            print('conta ja existente\nPor favor fazer login')
                            sleep(2)
                            submenu_login(client)
                        else:
                            print('falha na conexao, por favor tente novamente mais tarde')
                            sleep(2)
                            main_loop()
                    else:
                        exit(1)
        else:
            while True:
                opc = menus.enumerate_menu(['Tentar novamente', 'Sair'], 'Escolha uma opcao:', __CLEAR)

                if opc == 0:
                    email = input('digite seu email: ')

                    while email.find('@') == -1 or len(email) < 5:
                        email = input('por favor informe um email valido: ')

                    (status, data) = controller.connect(email, client)

                    if status == requests.ConstantsManagement.OK.value:
                        client.token = data
                        submenu_status_ok(client)
                    elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
                        pass
                    elif status == requests.ConstantsManagement.NOT_FOUND.value:
                        print('usuario nao encontrado')
                        sleep(2)
                        submenu_create_account(client)
                    elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
                        print('conta ja existente\nPor favor fazer login')
                        sleep(2)
                        submenu_login(client)
                    else:
                        print('falha na conexao, por favor tente novamente mais tarde')
                        sleep(2)
                        main_loop()
                else:
                    exit(1)

##@brief: Função responsavel pelas operações relacionadas a erros na momento de login no menu principal
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def submenu_login(client: ClientSocket):
    opc = menus.enumerate_menu(['Fazer Login', 'Sair'], 'Selecione uma opcao:', __CLEAR)

    if opc == 0:
        email = input('digite seu email: ')

        while email.find('@') == -1 or len(email) < 5:
            email = input('por favor informe um email valido: ')

        (status, data) = controller.connect(email, client)

        if status == requests.ConstantsManagement.OK.value:
            client.token = data
            submenu_status_ok(client)
        elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
            submenu_status_token(client, 2)
        elif status == requests.ConstantsManagement.NOT_FOUND.value:
            print('usuario nao encontrado')
            sleep(2)
            submenu_create_account(client)
        elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
            print('conta ja existente\nPor favor fazer login')
            sleep(2)
            submenu_login(client)
        else:
            print('falha na conexao, por favor tente novamente mais tarde')
            sleep(2)
            main_loop()
    else:
        exit(1)

##@brief: Função responsavel pelas operações relacionadas a erros na momento de criar conta no menu principal
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def submenu_create_account(client: ClientSocket):
    opc = menus.ysno_menu('Deseja criar uma conta?', __CLEAR)
    if opc == 0:
        email = input('digite seu email: ')

        while email.find('@') == -1 or len(email) < 5:
            email = input('por favor informe um email valido: ')

        (status, data) = controller.create_account(email, client)

        if status == requests.ConstantsManagement.OK.value:
            client.token = data
            submenu_status_ok(client)
        elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
            submenu_status_token(client, 1)
        elif status == requests.ConstantsManagement.NOT_FOUND.value:
            print('usuario nao encontrado')
            sleep(2)
            submenu_create_account(client)
        elif status == requests.ConstantsManagement.OPERATION_FAILED.value:
            print('conta ja existente\nPor favor fazer login')
            sleep(2)
            submenu_login(client)
        else:
            print('falha na conexao, por favor tente novamente mais tarde')
            sleep(2)
            main_loop()
    else:
        main_loop()

##@brief: Função responsavel pelas operações relacionadas ao menu principal e tratamento de seus erros
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def menu(client: ClientSocket):
    os.system(__CLEAR)
    while True:
        opc = menus.enumerate_menu(['Criar conta', 'Entrar', 'Sair'], 'Selecione uma das opcoes abaixo:', __CLEAR)

        email: str = ''

        if opc != 2:
            os.system(__CLEAR)
            email = input('digite seu email: ')

            while email.find('@') == -1 or len(email) < 5:
                email = input('por favor informe um email valido: ')

        if opc == 0:
            (status, data) = controller.create_account(email, client)
        elif opc == 1:
            (status, data) = controller.connect(email, client)
        else:
            exit(1)

        if status == requests.ConstantsManagement.OK.value:
            client.token = data
            submenu_status_ok(client=client)
        elif status == requests.ConstantsManagement.INVALID_TOKEN.value:
            submenu_status_token(client, opc)
        elif status == requests.ConstantsManagement.NOT_FOUND.value:
            print('Nao foi possivel encontrar uma conta')
            sleep(2)
            submenu_create_account(client)
        elif status == requests.ConstantsManagement.OPERATION_FAILED.value and opc == 0:
            print('conta ja existe\npor favor fazer login')
            sleep(2)
            submenu_login(client)
        elif  status == requests.ConstantsManagement.OPERATION_FAILED.value and opc == 1:
            print('conta nao existe\npor favor fazer criar conta')
            sleep(2)
            submenu_status_token(client, 1)
        else:
            print('falha na conexao, por favor tente novamente mais tarde')
            sleep(2)
            main_loop()

##@brief: Função principal do programa, ao ser iniciado verifica se é possivel conexão com servidor, caso contrario encerra o programa
# @param: client - ClientSocket, objeto do cliente que será usado para as operações de conexão com o servidor
def main_loop():
    ip = input("Digite o IP do servidor: ")

    new_client = ClientSocket(ip=ip)

    if new_client.connect():
        os.system(__CLEAR)
        print('conexao estabelecida')
        sleep(2)
        new_client.end()
    else:
        os.system(__CLEAR)
        print('nao foi possivel conectar')
        exit(1)

    menu(new_client)

main_loop()


