
from Server.TransactionCoordinatorNode import TransationCoordinator
from Server.utils.twoPhaseCommit import *

tc = TransationCoordinator(ServerIds.A, ServerName.A)
t = tc.setup_transaction([['A','B', 'Server-A'], ['E','F','Server-B']],'1234')
print(tc.prepare_transaction(t))

'''
# coordinator = rq.pop('coordinator')
#     id = rq.pop('transaction_id')
#     timestamp = rq.pop('timestamp')
#     participants = rq.pop('participants')
#     intentions = rq.pop('intentions')
response = requests.post('http://'+SERVERIP['Server-B']+':5001/newtransaction', json=t.to_request_msg('Server-B'), headers={"Content-Type": "application/json"})
response = requests.post('http://'+SERVERIP['Server-B']+':5001/committransaction', json={'transaction_id': response.json().get('id'), 'decision': TransactionStatus.ABORTED.value}, headers={"Content-Type": "application/json"})
#response = requests.get('http://'+SERVERIP['Server-B']+':5000/serverstatus')
'''