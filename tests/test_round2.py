import unittest,tempfile,json,hashlib,time,sys
from pathlib import Path
from ledger import Store,recover_ranges

class LedgerTests(unittest.TestCase):
 def setUp(self):
  self.dir=Path(tempfile.mkdtemp(prefix='case-v02-'));self.path=self.dir/'state.sqlite';self.s=Store(self.path)
 def tearDown(self):self.s.db.close()
 def q(self,i,**kw):
  return dict(issue_key='issue-'+str(i),fact_slot='gap.'+str(i),question='核对独立事实缺口'+str(i),reason='测试原因',impact='待核事实',source='materials/同一材料.txt',**kw)
 def merge(self,bid,qs,**fields):
  self.s.put('rev:'+bid,self.s.revision());self.s.merge(bid,dict(batch_id=bid,questions=qs,**fields),0)
 def test_important_sixth_stable_and_reboot(self):
  # Distinct wording avoids intentionally conservative fuzzy matching.
  texts=['付款主体是谁','合同日期是什么','货物数量是多少','运输地点在哪里','联系人的电话如何核对','刚才否定交付是否更正了旧说法']
  qs=[self.q(i,urgency=0) for i in range(6)]
  for q,t in zip(qs,texts):q['question']=t
  self.merge('first',qs[:5]);old=[q['id'] for q in self.s.visible()];self.assertEqual(len(old),5)
  qs[5]['urgency']=3;qs[5]['key_change']=True;self.merge('important',[qs[5]])
  ids=[q['id'] for q in self.s.visible()];self.assertIn('Q006',ids);self.assertEqual(ids[:4],old[:4]);self.assertEqual(len(self.s.questions()),6)
  self.merge('ordinary',[dict(qs[1],reason='普通说明更新')]);self.assertEqual(ids,[q['id'] for q in self.s.visible()])
  self.s.db.close();self.s=Store(self.path);self.assertEqual(ids,[q['id'] for q in self.s.visible()])
 def test_same_material_different_gap_and_synonym_exclusion(self):
  first=self.q(1);first['question']='这笔款用于哪项约定';self.merge('a',[first]);self.s.action('Q001','本次不问','exclude')
  other=self.q(2,distinct_from={'Q001':'付款用途与合同签署日期是不同缺口'});other['question']='合同哪天签署';self.merge('b',[other]);self.assertEqual(len(self.s.questions()),2)
  alias=dict(first,issue_key='paraphrased',question='请解释本笔付款的目的');self.merge('c',[alias]);self.assertEqual(len(self.s.questions()),2);self.assertEqual(self.s.questions()[0]['state'],'本次不问')
  self.merge('late',[first]);self.assertEqual([q['id'] for q in self.s.visible()],['Q002'])
  self.merge('evidence',[dict(alias,recovery_reason='新增凭证区分了原款项',recovery_evidence='E000009')]);self.assertEqual(len(self.s.proposal_rows()),1);self.assertEqual(self.s.questions()[0]['state'],'本次不问')
  pid=self.s.proposal_rows()[0]['id'];self.s.resolve_proposal(pid,True);self.assertEqual(self.s.questions()[0]['state'],'候选')
 def event(self,n=1,text='金额125000，尚未交付',segment='s1'):
  return dict(event_seq=n,recording_id='R1',session_id='R1',matter_id='fake',event_type='segment',is_final=True,revision=1,segment_id=segment,text=text,audio_start_ms=0,audio_end_ms=1000)
 def ingest(self,events):
  path=self.dir/'events.jsonl'
  with path.open('a') as f:
   for e in events:f.write(json.dumps(e,ensure_ascii=False)+'\n')
  self.s.ingest(path,'R1','fake','meeting')
 def test_correction_equal_length_early_and_stale_result(self):
  self.ingest([self.event(),self.event(2,'日期三月十二日','s2')]);raw=json.dumps(self.s.events(),ensure_ascii=False)
  self.merge('before',[self.q(1)],fact_changes=[{'fact_key':'delivery','新说法':'未交付'}])
  self.s.put('rev:late',0)
  c=self.s.correct('R1','s1','金额120000，尚未交付',0,'op1');self.assertEqual(c,1)
  self.assertEqual(raw,json.dumps(self.s.events(),ensure_ascii=False));self.assertEqual(self.s.current_segments()[0]['text'],'金额120000，尚未交付')
  self.assertFalse(self.s.visible());self.assertEqual(self.s.revision(),1)
  self.assertEqual(self.s.correct('R1','s1','金额120000，尚未交付',0,'op1'),1)
  with self.assertRaises(ValueError):self.s.correct('R1','s1','金额三万元',0,'bad-old-version')
  self.s.merge('late',{'batch_id':'late','questions':[self.q(1)]},2);self.assertFalse(self.s.visible())
  bid='CORRECTION-meeting-2-C1';self.merge(bid,[self.q(1)],fact_changes=[{'fact_key':'delivery','新说法':'待核'}]);self.assertEqual(self.s.get('correction_processed'),1);self.assertEqual(len(self.s.visible()),1)
  self.s.correct('R1','s2','日期三月二十一日',0,'op2');self.assertEqual(self.s.revision(),2)
  self.s.export(self.dir/'case/沟通记录-GPT/meeting/会中记录');self.assertTrue((self.dir/'case/沟通记录-GPT/meeting/会中记录/转写人工纠错.jsonl').exists())
  self.s.db.close();self.s=Store(self.path);self.assertEqual(self.s.current_segments()[1]['text'],'日期三月二十一日')
 def test_qa_never_overwritten_and_decision_explicit(self):
  for i in [1,2]:
   bid='USER-'+str(i);self.s.add_question(bid,'用户提问'+str(i));self.merge(bid,[],answer='回答'+str(i),answer_sources=['E000001'])
  for i in range(4):self.merge('auto'+str(i),[],answer='')
  self.s.add_question('USER-d','我决定下周核对原件','decision')
  self.assertEqual([q['answer'] for q in self.s.qa()][:2],['回答1','回答2']);self.assertEqual(self.s.qa()[2]['intent'],'decision')
  self.s.db.close();self.s=Store(self.path);self.assertEqual(len(self.s.qa()),3);self.assertEqual(self.s.qa()[0]['sources'],['E000001'])
 def test_corrected_answer_versions_and_reviewed_questions(self):
  self.ingest([self.event()]);self.s.add_question('USER-1','金额是多少');self.merge('USER-1',[self.q(1)],answer='旧答案',answer_sources=['E000001'])
  self.s.correct('R1','s1','金额120000，尚未交付',0,'change')
  self.assertIn('待复核',self.s.qa()[0]['state'])
  self.merge('CORRECTION-meeting-1-C1',[],reviewed_questions=[{'id':'Q001','still_needed':True,'question':'更正后的十二万元是合同金额吗','reason':'金额经纠错仍需原件核对','impact':'确认金额与合同的对应','source':'C000001'}],answer_updates=[{'id':'USER-1','answer':'纠错后金额120000；仅为陈述','sources':['C000001']}])
  self.assertEqual(self.s.qa()[0]['answer'],'纠错后金额120000；仅为陈述')
  self.assertEqual(self.s.db.execute('SELECT answer FROM qa_versions').fetchone()[0],'旧答案')
  self.assertEqual(len(self.s.visible()),1)
  self.assertEqual(self.s.visible()[0]['body']['reason'],'金额经纠错仍需原件核对')
  self.s.correct('R1','s1','金额待核，尚未交付',1,'second-correction')
  self.assertIn('待复核',self.s.qa()[0]['state'])
  self.merge('CORRECTION-meeting-1-C2',[],reviewed_questions=[{'id':'Q001','still_needed':True,'reason':'只有复核标签不足','source':'C000002'}])
  self.assertEqual(self.s.visible(),[])
 def test_similar_words_distinct_fact_slots(self):
  a=self.q('signature');a['question']='请核对这份材料中的签署日期是什么时候'
  b=self.q('acceptance');b['question']='请核对这份材料中的验收日期是什么时候'
  self.merge('separate',[a,b]);self.assertEqual(len(self.s.questions()),2)
 def test_new_urgent_question_not_starved_by_five_urgent_ones(self):
  texts=['付款主体是谁','合同日期是什么','货物数量是多少','运输地点在哪里','联系人的电话如何核对','新的否定说法是否改变了交付事实']
  qs=[dict(self.q(i,urgency=3),question=t) for i,t in enumerate(texts)]
  self.merge('five',qs[:5]);prior=[q['id'] for q in self.s.visible()]
  self.merge('new-urgent',[qs[5]]);current=[q['id'] for q in self.s.visible()]
  self.assertIn('Q006',current);self.assertEqual(len(current),5)
  self.assertEqual(current[:-1],[q for q in prior if q in current])
  self.merge('ordinary-update',[dict(qs[1],reason='普通文字变化')]);self.assertEqual(current,[q['id'] for q in self.s.visible()])
 def test_unclear_identity_held_not_source_wide_block(self):
  self.merge('a',[self.q(1)]);self.s.action('Q001','已问','ask')
  q=self.q(2);q['question']='另一个完全不同的提问';self.merge('b',[q]);self.assertEqual(len(self.s.questions()),1);self.assertEqual(len(self.s.proposal_rows()),1)
  self.s.resolve_proposal(self.s.proposal_rows()[0]['id'],True);self.assertEqual(len(self.s.questions()),2)
 def test_queue_checkpoint_recovery_with_correction_revision(self):
  self.ingest([self.event()]);self.s.correct('R1','s1','金额120000，尚未交付',0,'op')
  bid='CORRECTION-meeting-1-C1';recover_ranges(self.s,'meeting',[{'id':bid}]);self.assertEqual(self.s.get('range:'+bid),1);self.assertEqual(self.s.get('rev:'+bid),1)
  with self.assertRaises(ValueError):recover_ranges(self.s,'meeting',[{'id':'TRANSCRIPT-meeting-999-C1'}])
 def test_duplicate_half_line_and_asr_revision_preserves_manual(self):
  e=self.event();self.ingest([e,e]);self.assertEqual(len(self.s.events()),1)
  self.s.correct('R1','s1','金额待回听',0,'correct')
  self.ingest([dict(e,event_seq=2,revision=2,text='金额130000')]);self.assertEqual(self.s.current_segments()[0]['text'],'金额待回听');self.assertEqual(self.s.current_segments()[0]['raw_text'],'金额130000')
 def test_incomplete_utf8_event_resumes_and_conflicting_duplicate_stops(self):
  p=self.dir/'partial.jsonl';data=(json.dumps(self.event(),ensure_ascii=False)+'\n').encode();cut=data.index('金额'.encode())+1
  p.write_bytes(data[:cut]);self.s.ingest(p,'R1','fake','meeting');self.assertFalse(self.s.events())
  with p.open('ab') as f:f.write(data[cut:])
  self.s.ingest(p,'R1','fake','meeting');self.assertEqual(len(self.s.events()),1)
  with p.open('ab') as f:f.write((json.dumps(self.event(text='冲突的重复序号'),ensure_ascii=False)+'\n').encode())
  with self.assertRaises(ValueError):self.s.ingest(p,'R1','fake','meeting')
  self.assertEqual(self.s.events()[0]['text'],'金额125000，尚未交付')

if __name__=='__main__':unittest.main(verbosity=2)
