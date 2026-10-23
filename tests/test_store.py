from store import Store
from pathlib import Path
import tempfile,json
p=Path(tempfile.mkdtemp(prefix='case-store-test-'));s=Store(p/'state.sqlite')
qs=[{'issue_key':'issue-'+str(i),'question':'问题'+str(i),'reason':'原因','impact':'影响','source':'E'+str(i).zfill(6)} for i in range(1,7)]
s.merge('b1',{'batch_id':'b1','questions':qs},0)
assert len([q for q in s.questions() if q['state']=='候选'][:5])==5
for q,st in zip(s.questions(),['已问','已解决','稍后再问','本次不问']):s.action(q['id'],st,'op-'+q['id'])
s.merge('stale',{'batch_id':'stale','questions':qs},0)
assert [q['state'] for q in s.questions()][:4]==['已问','已解决','稍后再问','本次不问']
s.merge('paraphrase',{'batch_id':'paraphrase','questions':[dict(qs[3],issue_key='new-wording',question='同义的新问法')]},0)
assert len(s.questions())==6
assert s.db.execute('select count(*) from deferred').fetchone()[0]==1
s.db.close();s=Store(p/'state.sqlite');assert s.questions()[3]['state']=='本次不问'
s.action('Q004','候选','restore-explicit');assert s.questions()[3]['state']=='候选'
e={'event_seq':1,'recording_id':'r1','session_id':'r1','matter_id':'fake','event_type':'segment','is_final':True,'revision':1,'segment_id':'p1','text':'金额125000','audio_start_ms':0,'audio_end_ms':1000}
f=p/'events.jsonl';line=json.dumps(e,ensure_ascii=False).encode();f.write_bytes(line);assert s.ingest(f,'r1','fake','s1')==0
f.write_bytes(line+b'\n');assert s.ingest(f,'r1','fake','s1')==1
with f.open('ab') as a:a.write(line+b'\n')
s.ingest(f,'r1','fake','s1');assert len(s.events())==1
r=dict(e,event_seq=2,revision=2,event_type='revision',text='金额120000')
with f.open('ab') as a:a.write(json.dumps(r,ensure_ascii=False).encode()+b'\n')
s.ingest(f,'r1','fake','s1');assert len(s.events())==2 and s.events()[-1]['revision']==2
bad=dict(e,event_seq=3,matter_id='wrong')
with f.open('ab') as a:a.write(json.dumps(bad).encode()+b'\n')
try:s.ingest(f,'r1','fake','s1');raise AssertionError('wrong case accepted')
except ValueError:pass
assert len(s.events())==2
s.export(p/'case/沟通记录-GPT/s1/会中记录')
assert (p/'case/沟通记录-GPT/s1/会中记录/实时转写.md').exists()
print(json.dumps({'passed':['五条上限','四种点击状态','旧结果不能恢复','同来源同义新key保守隔离','重启状态保留','显式恢复','半行等待','重复事件幂等','等长早段更正','串案拒绝','案件内相对布局'],'directory':str(p)},ensure_ascii=False))

# Final timing may shift; superseded partial remains in raw events, not duplicate prose.
f2=p/'timing.jsonl'
a=dict(e,recording_id='r2',session_id='r2',event_seq=1,is_final=False,segment_id='MIC-0',audio_start_ms=0,audio_end_ms=1500)
b=dict(a,event_seq=2,is_final=True,segment_id='MIC-500',audio_start_ms=500,audio_end_ms=1600)
f2.write_text(''.join(json.dumps(x)+'\n' for x in [a,b]))
s.ingest(f2,'r2','fake','s1')
assert len([x for x in s.current_segments() if x['recording_id']=='r2'])==1
assert len([x for x in s.events() if x['recording_id']=='r2'])==2
print('shifted_final_keeps_raw_hides_duplicate: passed')
from store import recover_ranges
# Job commit survives while the bookkeeping update was lost.
s.put('submitted',0);recover_ranges(s,'s1',[{'id':'TRANSCRIPT-s1-2'}]);assert s.get('submitted')==2 and s.get('range:TRANSCRIPT-s1-2')==2 and s.get('processed',0)==0
try:recover_ranges(s,'s1',[{'id':'TRANSCRIPT-s1-999'}]);raise AssertionError('skipped missing events')
except ValueError:pass
print('crash gap bookkeeping recovery + impossible cursor refusal: passed')
