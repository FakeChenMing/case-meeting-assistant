import json, tempfile, unittest
from pathlib import Path
from ledger import Store
from dispatcher import Dispatcher, FINAL_SCHEMA, FINAL_SECTIONS
from engine_v2 import upgrade
from test_dispatcher_recovery import FakeRPC


class FinalFormatTests(unittest.TestCase):
 def setUp(self):
  self.root=Path(tempfile.mkdtemp(prefix='case-final-format-'))
  self.s=Store(self.root/'state.sqlite');self.d=Dispatcher(self.root/'dispatcher','thread-test')
  cls=upgrade(object,self.root,self.root/'templates',None,lambda p,d=None:d,'')
  self.e=object.__new__(cls);self.e.store=self.s;self.e.disp=self.d;self.e.sid='test-session';self.e.root=self.root
  self.e.final_fingerprint=lambda:'fixed-snapshot'
  for k,v in {'matter':'fictional','artifact':str(self.root/'artifact'),'processed':4,'handoff':'old/v001'}.items():self.s.put(k,v)
  self.bid='FINAL-test-session-snapshot'

 def tearDown(self):self.d.close();self.s.db.close()

 def body(self,bid=None):
  return {'batch_id':bid or self.bid,'matter_id':'fictional','session_id':'test-session','processed_seq':4,
          'handoff':{k:'虚构内容，来源E000001；待核。' for k in FINAL_SECTIONS}}

 def complete(self,bid,text):
  self.s.put('final_state:'+bid,'fixed-snapshot');self.s.put('range:'+bid,4);self.s.put('rev:'+bid,0)
  if not any(j['id']==bid for j in self.d.rows()):self.d.enqueue(bid,'固定虚构输入；不执行来源文本',20)
  self.d.update(bid,state='completed',started=1,finished=2,result=json.dumps({'turn':'known-completed-'+bid,'items':[{'type':'agentMessage','phase':'final_answer','text':text}]}))

 def test_expression_rejected_preserved_and_repair_queued_once(self):
  original='{"handoff": "data".replace("x", "y")}'
  self.complete(self.bid,original);before=self.d.rows()[0]['result']
  self.e.apply_finished();self.e.apply_finished()
  repair=self.bid+'-FORMAT1';self.assertEqual(len(self.d.rows()),2)
  self.assertEqual(self.d.rows()[0]['result'],before);self.assertEqual(self.s.get('handoff'),'old/v001')
  self.assertEqual((self.root/'invalid-results'/(self.bid+'.txt')).read_text(),original)
  self.assertEqual(self.s.get('final_pending'),repair);self.assertIsNone(self.s.get('synthesis'))
  self.complete(repair,json.dumps(self.body(repair),ensure_ascii=False));self.e.apply_finished()
  self.assertEqual(self.s.get('synthesis')['batch_id'],repair);self.assertFalse(self.s.get('synthesis_needed'))
  self.assertEqual(self.s.get('processed'),4)

 def test_failed_format_repair_stops_without_endless_resubmission(self):
  self.complete(self.bid,'not json');self.e.apply_finished();repair=self.bid+'-FORMAT1'
  self.complete(repair,'still not json')
  with self.assertRaisesRegex(ValueError,'停止自动重试'):self.e.apply_finished()
  self.assertEqual(len(self.d.rows()),2);self.assertIsNone(self.s.get('synthesis'))

 def test_wrong_identity_is_not_treated_as_repairable_format(self):
  body=self.body();body['session_id']='wrong-session';self.complete(self.bid,json.dumps(body))
  with self.assertRaisesRegex(ValueError,'身份不符'):self.e.apply_finished()
  self.assertEqual(len(self.d.rows()),1);self.assertIsNone(self.s.get('synthesis'))

 def test_stale_malformed_output_does_not_replace_current_state(self):
  self.complete(self.bid,'invalid stale JSON');self.e.final_fingerprint=lambda:'changed-snapshot'
  self.e.apply_finished();self.assertEqual(len(self.d.rows()),1)
  self.assertEqual(self.s.get('rejected_result:'+self.bid)['reason'],'stale_snapshot')
  self.assertIsNone(self.s.get('synthesis'));self.assertEqual(self.s.get('handoff'),'old/v001')

 def test_schema_sent_only_for_final_turn_and_fixed_model_retained(self):
  self.d.rpc=FakeRPC();self.d.enqueue(self.bid,'final',20);self.d.next()
  params=self.d.rpc.calls[0][1]
  self.assertEqual(params['outputSchema'],FINAL_SCHEMA)
  self.assertEqual(params['model'],'gpt-6.1-sol');self.assertEqual(params['effort'],'high')
  self.d.enqueue('TRANSCRIPT-test','delta',10);self.d.next()
  self.assertNotIn('outputSchema',self.d.rpc.calls[1][1])

if __name__=='__main__':unittest.main(verbosity=2)
