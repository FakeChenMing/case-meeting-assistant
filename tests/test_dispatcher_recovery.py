import unittest,tempfile,queue,json
from dispatcher import Dispatcher

class FakeRPC:
 def __init__(self):self.events=queue.Queue();self.calls=[];self.sent=[]
 def call(self,method,p,**kw):
  self.calls.append((method,p))
  if method=='turn/start':
   self.events.put({'method':'item/completed','params':{'turnId':'T-new','item':{'id':'answer','type':'agentMessage','text':'{}'}}})
   self.events.put({'method':'turn/completed','params':{'turn':{'id':'T-new','status':'completed'}}})
   return {'turn':{'id':'T-new'}}
 def send(self,x):self.sent.append(x)
 def close(self):pass

class RecoveryTests(unittest.TestCase):
 def setUp(self):self.d=Dispatcher(tempfile.mkdtemp(prefix='case-recovery-'),'T-fake');self.rpc=FakeRPC();self.d.rpc=self.rpc
 def tearDown(self):self.d.close()
 def test_user_priority_and_single_turn(self):
  self.d.enqueue('auto','自动转写',10);self.d.enqueue('user1','提问一',0);self.d.enqueue('user2','提问二',0)
  self.assertEqual(self.d.next(),'user1');self.assertEqual(self.d.next(),'user2');self.assertEqual(self.d.next(),'auto')
  self.assertEqual(len(self.rpc.calls),3)
  self.d.busy.acquire()
  try:
   with self.assertRaises(RuntimeError):self.d.next()
  finally:self.d.busy.release()
 def test_unknown_submission_never_resent(self):
  self.d.enqueue('unknown','已提交但结果不明');self.d.update('unknown',state='submitting');self.d.history=lambda:[]
  self.d.reconcile();self.assertEqual(self.d.rows()[0]['state'],'uncertain')
  with self.assertRaises(RuntimeError):self.d.next()
  self.assertFalse(self.rpc.calls)
 def test_completion_before_local_commit_recovered_once(self):
  self.d.enqueue('recover','原输入');self.d.update('recover',state='inflight',turn_id='T-old')
  t={'id':'T-old','status':'completed','items':[{'id':'answer','type':'agentMessage','text':'保留的已完成回答'}]}
  self.d.history=lambda:[t];self.d.reconcile();self.d.reconcile()
  row=self.d.rows()[0];self.assertEqual(row['state'],'completed');self.assertEqual(json.loads(row['result'])['items'][0]['text'],'保留的已完成回答')
  self.assertIsNone(self.d.next());self.assertFalse(self.rpc.calls)
 def test_partial_history_merged_without_losing_tools(self):
  self.d.enqueue('partial','输入');self.d.update('partial',state='inflight',turn_id='T-live')
  t={'id':'T-live','status':'inProgress','items':[{'id':'tool1','type':'commandExecution','status':'completed'}]}
  self.d.history=lambda:[t];self.d.reconcile()
  self.rpc.events.put({'method':'item/completed','params':{'turnId':'T-live','item':{'id':'answer','type':'agentMessage','text':'{}'}}})
  self.rpc.events.put({'method':'turn/completed','params':{'turn':{'id':'T-live','status':'completed'}}})
  self.d.next();items=json.loads(self.d.rows()[0]['result'])['items']
  self.assertEqual({i['id'] for i in items},{'tool1','answer'});self.assertFalse(self.rpc.calls)
 def test_permission_request_blocked_and_visible(self):
  self.d.enqueue('permission','输入');self.d.update('permission',state='inflight',turn_id='T-live')
  self.rpc.events.put({'id':'request-1','method':'item/commandExecution/requestApproval','params':{'turnId':'T-live','command':'fictional approval fixture'}})
  self.rpc.events.put({'method':'turn/completed','params':{'turn':{'id':'T-live','status':'failed','error':{'message':'permission not approved'}}}})
  self.d.next();record=json.loads((self.d.root/'approval-blocked.json').read_text())
  self.assertIn('未批准',record['state']);self.assertIn('核对',record['next']);self.assertIn('error',self.rpc.sent[0]);self.assertEqual(self.d.rows()[0]['state'],'failed')

if __name__=='__main__':unittest.main(verbosity=2)
