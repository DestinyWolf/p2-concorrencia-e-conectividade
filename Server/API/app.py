#code for API
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from database import mongoHandler
from utils.utils import CollectionsName

serveriplist = []
hasnewserver = False

app = Flask(__name__)
CORS(app)

db = mongoHandler.MongoHandler('connectstring', 'companhia')

#funcoes da api

@app.route('/')
def home():
    return {'msg':'working'}

@app.route('/newserver')
def new_server():
    global hasnewserver
    params = request.get_json()
    serveriplist.append((params['name'],params['ip']))
    hasnewserver = True
    
    return {'msg':'success'}, 200

#endpoint para a lista de adjacencias, retorna toda a lista de adjacencias de uma companhia

@app.route('/getgraph')
def get_graph():
    global db
    return jsonify(db.get_all_itens_in_group(CollectionsName.GRAPH.value)), 200

@app.route('/firstphase')
def  first_phase():
    '''1 fase do 2pc'''
    rq = request.get_json()

    trans_id = rq['id']
    data = rq['data']
    time = rq['time']

    '''thread pool'''

    return {'id':'transid','msg':'accept'}, 200 if 'algo' else {'id':'transid','msg':'denied'}, 200
    
@app.route('/secondphase')
def  second_phase():
    '''2 fase do 2pc'''
    rq = request.get_json()
    trans_id = rq['id']
    msg = rq['msg']

    '''thread pool'''
    return {'msg':'ok'}, 200 if 'algo' else {'msg':'not ok'}, 200

@app.route('/notfinish')
def not_finish():
    rq = request.get_json()
    whoisme = rq['whoIsMe']
    trans_id = rq['id']
    msg = rq['msg']

    '''se camila fizer'''

    return {'id':'transid', msg:'do'},200 if 'algo' else  {'id':'transid', 'msg':'not ok'}, 200

@app.route('/updateroute')
def update_route():
    rq = request.get_json()
    whoisme = rq['whoIsMe']
    route = rq['routeToUpdate']
    msg = rq['msg']

    '''camila se vira pra implementar'''

    return {'msg':'success'}, 200

# funcao de inicializacao do servidor que faz o envio dos rq informando um novo servidor
payload = {'name':'nome da companhia',  'ip':cm.HOST}
for (name, ip) in serveriplist:
    rs = requests.post(f'http://{ip}/newserver', data=payload).json()

    #mudar o tratamento depois apenas para testes
    if rs['msg'] == 'fail':
        print(f'falha ao se comunicar com o servidor:{name}')


def main_api():
    global  app, serveriplist, db, cm
    app.run(host='0.0.0.0',  port=5000, debug=True)