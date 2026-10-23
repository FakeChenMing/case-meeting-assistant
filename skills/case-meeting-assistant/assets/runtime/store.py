import sqlite3,json,time,threading,re,hashlib,os
from pathlib import Path

def atomic(path,value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 data=value if isinstance(value,str) else json.dumps(value,ensure_ascii=False,indent=2)
 tmp=path.with_name(path.name+'.tmp');tmp.write_text(data);os.chmod(tmp,0o600);os.replace(tmp,path)
def recover_ranges(store,session,jobs):
 prefix='TRANSCRIPT-'+session+'-';events=store.events();received=events[-1]['seq'] if events else 0
 for job in jobs:
  if job['id'].startswith(prefix) and job['id'][len(prefix):].isdigit():
   end=int(job['id'][len(prefix):])
   if end>received:raise ValueError('队列位置超过已保存采集事件；停止防止跳过内容')
   store.put('range:'+job['id'],end);store.put('submitted',max(store.get('submitted',0),end))
class Store:
 def __init__(self,path):
  self.lock=threading.RLock();self.db=sqlite3.connect(str(path),check_same_thread=False);self.db.row_factory=sqlite3.Row
  self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
  CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT);
  CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,recording TEXT,source_seq INTEGER,data TEXT,UNIQUE(recording,source_seq));
  CREATE TABLE IF NOT EXISTS offsets(recording TEXT PRIMARY KEY,position INTEGER);
  CREATE TABLE IF NOT EXISTS questions(id TEXT PRIMARY KEY,issue_key TEXT UNIQUE,body TEXT,state TEXT,version INTEGER,created REAL);
  CREATE TABLE IF NOT EXISTS actions(op TEXT PRIMARY KEY,qid TEXT,state TEXT,at REAL,reason TEXT);
  CREATE TABLE IF NOT EXISTS results(batch TEXT PRIMARY KEY,body TEXT,at REAL);
  CREATE TABLE IF NOT EXISTS deferred(id INTEGER PRIMARY KEY,body TEXT,reason TEXT,at REAL);
  ''');self.db.commit()
 def get(self,k,default=None):
  with self.lock:
   r=self.db.execute('select v from meta where k=?',(k,)).fetchone();return json.loads(r[0]) if r else default
 def put(self,k,v):
  with self.lock:self.db.execute('insert or replace into meta values(?,?)',(k,json.dumps(v,ensure_ascii=False)));self.db.commit()
 def questions(self):
  with self.lock:return [{**dict(r),'body':json.loads(r['body'])} for r in self.db.execute('select * from questions order by created,id')]
 def action(self,qid,state,op,reason='用户点击'):
  if state not in ['已问','已解决','稍后再问','本次不问','候选']:raise ValueError('无效问题状态')
  with self.lock,self.db:
   if self.db.execute('select 1 from actions where op=?',(op,)).fetchone():return
   if not self.db.execute('select 1 from questions where id=?',(qid,)).fetchone():raise ValueError('问题不存在')
   self.db.execute('update questions set state=?,version=version+1 where id=?',(state,qid));self.db.execute('insert into actions values(?,?,?,?,?)',(op,qid,state,time.time(),reason))
 def merge(self,batch,result,processed):
  if result.get('batch_id')!=batch:raise ValueError('分析批次ID不一致')
  qs=result.get('questions',[])
  if not isinstance(qs,list) or len(qs)>30:raise ValueError('问题输出格式无效')
  # User statuses are authoritative; candidates can update text, never restore a handled issue.
  with self.lock,self.db:
   if self.db.execute('select 1 from results where batch=?',(batch,)).fetchone():return
   excluded=[q for q in self.questions() if q['state']=='本次不问']
   for q in qs:
    for k in ['issue_key','question','reason','impact','source']:
     if not isinstance(q.get(k),str) or not q[k].strip():raise ValueError('问题字段缺失 '+k)
    old=self.db.execute('select * from questions where issue_key=?',(q['issue_key'],)).fetchone()
    if old:
     q['id']=old['id'];self.db.execute('update questions set body=? where id=?',(json.dumps(q,ensure_ascii=False),old['id']));continue
    # Conservative source-overlap quarantine for reworded/new-key exclusions. Never auto restore.
    overlap=next((e for e in excluded if set(re.findall(r'E\d+|M\d+|[^\s；;]+\.(?:md|txt|pdf)',q['source'])) & set(re.findall(r'E\d+|M\d+|[^\s；;]+\.(?:md|txt|pdf)',e['body']['source']))),None)
    if overlap:
     self.db.execute('insert into deferred(body,reason,at) values(?,?,?)',(json.dumps(q,ensure_ascii=False),'与本次不问 '+overlap['id']+' 的来源重叠，保守隔离；需用户复核后恢复。',time.time()));continue
    num=self.db.execute('select count(*) from questions').fetchone()[0]+1;qid=f'Q{num:03d}';q['id']=qid
    self.db.execute('insert into questions values(?,?,?,?,?,?)',(qid,q['issue_key'],json.dumps(q,ensure_ascii=False),'候选',1,time.time()))
   self.db.execute('insert into results values(?,?,?)',(batch,json.dumps(result,ensure_ascii=False),time.time()))
   self.db.execute('insert or replace into meta values(?,?)',('processed',json.dumps(processed)))
   self.db.execute('insert or replace into meta values(?,?)',('latest_result',json.dumps(result,ensure_ascii=False)))
 def ingest(self,path,recording,matter,session):
  path=Path(path)
  if not path.exists():return 0
  with self.lock:
   row=self.db.execute('select position from offsets where recording=?',(recording,)).fetchone();pos=row[0] if row else 0
   if path.stat().st_size<pos:raise ValueError('采集日志被截短，停止读取')
   count=0
   with path.open('rb') as f:
    f.seek(pos)
    for line in f:
     if not line.endswith(b'\n'):break
     d=json.loads(line)
     if d.get('session_id')!=recording or d.get('recording_id')!=recording or d.get('matter_id')!=matter:raise ValueError('采集事件身份不符，停止防止串案')
     n=d.get('event_seq')
     if not isinstance(n,int) or n<1:raise ValueError('无效事件序号')
     expected=self.db.execute('select max(source_seq) from events where recording=?',(recording,)).fetchone()[0] or 0
     if n>expected+1:raise ValueError('采集事件序号缺口')
     existing=self.db.execute('select data from events where recording=? and source_seq=?',(recording,n)).fetchone()
     d['source_session_id']=d['session_id'];d['session_id']=session
     value=json.dumps(d,ensure_ascii=False)
     if existing and existing[0]!=value:raise ValueError('重复序号内容冲突')
     with self.db:
      self.db.execute('insert or ignore into events(recording,source_seq,data) values(?,?,?)',(recording,n,value));pos+=len(line)
      self.db.execute('insert or replace into offsets values(?,?)',(recording,pos))
     count+=1
   return count
 def events(self,after=0):
  with self.lock:return [{'seq':r['seq'],**json.loads(r['data'])} for r in self.db.execute('select * from events where seq>? order by seq',(after,))]
 def current_segments(self):
  # Apple may move a segment's start time when finalizing it. Keep every raw
  # revision, but hide superseded volatile windows from the readable view.
  latest={}
  for e in self.events():
   if e.get('event_type') in ['segment','revision']:latest[(e['recording_id'],e['segment_id'])]=e
  finals=[e for e in latest.values() if e.get('is_final')]
  rows=[]
  for e in latest.values():
   superseded=not e.get('is_final') and any(f['recording_id']==e['recording_id'] and f['seq']>e['seq'] and max(f['audio_start_ms'],e['audio_start_ms'])<min(f['audio_end_ms'],e['audio_end_ms']) for f in finals)
   if not superseded:rows.append(e)
  return sorted(rows,key=lambda e:(e['recording_id'],e['audio_start_ms']))
 def export(self,directory):
  with self.lock:self._export(directory)
 def _export(self,directory):
  dest=Path(directory);events=self.events();qs=self.questions()
  atomic(dest/'转写事件.jsonl',''.join(json.dumps(e,ensure_ascii=False)+'\n' for e in events))
  rows=self.current_segments()
  text='# 完整有效转写\n\n人工纠错另存于转写人工纠错.jsonl，原始ASR在转写事件.jsonl。后文改变说法保留为独立段落。\n\n转写是识别结果，说话人身份未知；不等于已经证明的事实。被后续稳定时间窗替代的临时片段不重复展示，全部原始修订仍保存在转写事件.jsonl。\n\n'
  for e in rows:text+=f"## E{e['seq']:06d} · {e['recording_id']} · {e['segment_id']} r{e['revision']} · 人工纠错C{e.get('correction_id',0)} · {e['audio_start_ms']/1000:.2f}–{e['audio_end_ms']/1000:.2f}秒 · {'稳定' if e['is_final'] else '临时'}\n\n{e['text']}\n\n"
  atomic(dest/'实时转写.md',text)
  with self.lock:
   actions=[dict(r) for r in self.db.execute('select * from actions order by at')]
  atomic(dest/'问题状态.json',{'questions':qs,'history':actions,'boundary':'已问不等于解决；已解决不等于事实证明。'})
  atomic(dest/'提示与处理记录.jsonl',''.join(json.dumps(a,ensure_ascii=False)+'\n' for a in actions))
  from model_settings import selection
  choice=selection(self)
  usage=[json.loads(r[0]) for r in self.db.execute("select v from meta where k like 'model_usage:%' order by k")]
  atomic(dest/'模型使用记录.json',{'current_for_new_jobs':choice,'changes':self.get('model_changes',[]),'turns':usage,'note':'配置记录表示实际提交参数；旧版结果按原固定6.1-sol/high，未重标历史结果。'})
  atomic(dest/'分析检查点.json',{'session_id':self.get('session'),'thread_id':self.get('thread'),'received':events[-1]['seq'] if events else 0,'processed':self.get('processed',0),'model':choice['model'],'effort':choice['effort'],'settings_revision':choice['revision'],'error':self.get('error'),'recordings':self.get('recordings',[])})
