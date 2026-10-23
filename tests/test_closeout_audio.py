import unittest,tempfile,json,hashlib
from pathlib import Path
from types import SimpleNamespace
from ledger import Store
from engine_v2 import upgrade

class CloseoutAudioTests(unittest.TestCase):
 def setUp(self):
  self.root=Path(tempfile.mkdtemp(prefix='case-closeout-'));self.case=self.root/'case';self.case.mkdir()
  self.sid='test-session';self.rid=self.sid+'-R01';self.capture=self.root/'capture'/self.rid;self.capture.mkdir(parents=True)
  self.closed=self.capture/'audio-00001.wav';self.closed.write_bytes(b'closed-original-test-fixture')
  self.open=self.capture/'audio-00002.wav';self.open.write_bytes(b'unclosed-original-test-fixture')
  self.s=Store(self.root/'state.sqlite')
  for k,v in {'case':str(self.case),'artifact':str(self.case/'沟通记录-GPT'/self.sid),'recordings':[self.rid],'session':self.sid,'matter':'case','pack':'pack.md'}.items():self.s.put(k,v)
  e={'event_type':'audio_closed','recording_id':self.rid,'file':self.closed.name,'audio_start_ms':0,'audio_end_ms':10000}
  self.s.db.execute('INSERT INTO events(recording,source_seq,data) VALUES(?,?,?)',(self.rid,1,json.dumps(e)));self.s.db.commit()
  stopped={'event_type':'stopped','recording_id':self.rid,'capture_stopped':True,'tail_drained':True,'observed_at':42}
  self.s.db.execute('INSERT INTO events(recording,source_seq,data) VALUES(?,?,?)',(self.rid,2,json.dumps(stopped)));self.s.db.commit()
  cls=upgrade(object,self.root,self.root/'templates',None,lambda p,d=None:d,'')
  self.engine=object.__new__(cls);self.engine.store=self.s;self.engine.sid=self.sid
  self.engine.make_prompt=lambda bid,end,text:text
 def tearDown(self):self.s.db.close()
 def test_files_and_snapshot_exist_before_model_is_queued(self):
  observed=[]
  def enqueue(bid,prompt,priority):
   snapshot=Path(self.s.get('final_snapshot:'+bid));packet=json.loads((snapshot/'整场整理输入.json').read_text())
   inv=packet['audio_inventory'];self.assertEqual(len(inv['closed']),1);self.assertEqual(len(inv['unclosed']),1)
   self.assertEqual(packet['capture_final_events'][0]['event_type'],'stopped')
   self.assertTrue(packet['capture_final_events'][0]['tail_drained'])
   self.assertIn('不得写成用户口头确认',prompt);self.assertIn('旧陈述→后续陈述',prompt)
   for row in inv['closed']+inv['unclosed']:
    path=self.case/row['path'];self.assertTrue(path.is_file());self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),row['sha256'])
   self.assertTrue((snapshot/'音频归档核对.json').exists());observed.append(bid)
  self.engine.disp=SimpleNamespace(enqueue=enqueue)
  self.assertTrue(self.engine.enqueue_final());self.assertEqual(len(observed),1)
  self.assertEqual(self.open.read_bytes(),b'unclosed-original-test-fixture')
 def test_mismatch_is_not_overwritten_and_missing_closed_file_stops(self):
  inv=self.engine.archive_audio();target=self.case/inv['closed'][0]['path'];target.write_bytes(b'different')
  with self.assertRaises(ValueError):self.engine.archive_audio()
  self.assertEqual(target.read_bytes(),b'different');self.closed.unlink()
  with self.assertRaises(ValueError):self.engine.archive_audio()
 def test_queued_question_can_use_committed_context_but_never_skip_speech(self):
  self.s.put('processed',5)
  def job(identifier,reported):
   self.s.add_question(identifier,'虚构排队问题');self.s.put('range:'+identifier,2)
   body={'batch_id':identifier,'matter_id':'case','session_id':self.sid,'processed_seq':reported,'questions':[],'answer':'已保存回答','answer_sources':['材料.txt']}
   return {'id':identifier,'state':'completed','started':1,'finished':2,'result':json.dumps({'items':[{'type':'agentMessage','phase':'final_answer','text':json.dumps(body)}]})}
  first=job('USER-valid',5);self.engine.disp=SimpleNamespace(rows=lambda:[first]);self.engine.apply_finished()
  self.assertEqual(self.s.get('processed'),5);self.assertEqual(self.s.qa()[0]['answer'],'已保存回答')
  second=job('USER-unprocessed',6);self.engine.disp=SimpleNamespace(rows=lambda:[first,second])
  with self.assertRaises(ValueError):self.engine.apply_finished()
  self.assertEqual(self.s.get('processed'),5);self.assertEqual(self.s.qa()[1]['answer'],'')
 def test_question_after_end_requests_a_new_handoff_without_overwriting_old(self):
  self.s.put('thread','dedicated-test-thread');self.s.put('phase','ended');self.s.put('handoff','existing/v001')
  queued=[];self.engine.disp=SimpleNamespace(enqueue=lambda *args,**kwargs:queued.append(args))
  bid=self.engine._question('虚构会后补充问题')
  self.assertEqual(self.s.get('phase'),'ending');self.assertTrue(self.s.get('synthesis_needed'))
  self.assertEqual(self.s.get('handoff'),'existing/v001');self.assertEqual(self.s.qa()[0]['id'],bid)
  self.assertEqual(queued[0][2],0)

if __name__=='__main__':unittest.main(verbosity=2)
