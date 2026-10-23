"""One writer, durable jobs, explicit recovery of uncertain submissions. No API billing fallback."""
import sqlite3,json,time,queue,uuid,fcntl,threading
from pathlib import Path
from rpc import RPC,clean
from store import atomic
from model_settings import MODEL,EFFORT,pinned
FINAL_SECTIONS=['purpose_and_outcome','new_statements','asr_corrections','speaker_changes','contradictions','prior_analysis_adjustments','questions_closure','evidence_and_actions','user_decisions','ai_suggestions','verification','gaps','mac_mini_steps']
FINAL_SCHEMA={
 'type':'object','additionalProperties':False,
 'properties':{
  'batch_id':{'type':'string'},'matter_id':{'type':'string'},'session_id':{'type':'string'},'processed_seq':{'type':'integer'},
  'handoff':{'type':'object','additionalProperties':False,'properties':{k:{'type':'string'} for k in FINAL_SECTIONS},'required':FINAL_SECTIONS}},
 'required':['batch_id','matter_id','session_id','processed_seq','handoff']}
class Dispatcher:
 def __init__(self,root,tid,settings_provider=None):
  self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.tid=tid
  self.lockfile=(self.root/'dispatcher.lock').open('a');fcntl.flock(self.lockfile,fcntl.LOCK_EX|fcntl.LOCK_NB)
  self.db=sqlite3.connect(str(self.root/'queue.sqlite'),check_same_thread=False);self.db.row_factory=sqlite3.Row
  self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
  self.db.execute('CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,prompt TEXT,priority INTEGER,state TEXT,turn_id TEXT,result TEXT,created REAL,started REAL,finished REAL,error TEXT)');self.db.commit()
  if 'attempt' not in [r[1] for r in self.db.execute('PRAGMA table_info(jobs)')]:
   self.db.execute('ALTER TABLE jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0');self.db.commit()
  # Jobs made before this feature all used 6.1-sol/high. Preserve that fact.
  for name,sql in [('model',"TEXT NOT NULL DEFAULT 'gpt-6.1-sol'"),('effort',"TEXT NOT NULL DEFAULT 'high'"),('settings_revision','INTEGER NOT NULL DEFAULT 0')]:
   if name not in [r[1] for r in self.db.execute('PRAGMA table_info(jobs)')]:
    self.db.execute('ALTER TABLE jobs ADD COLUMN '+name+' '+sql)
  self.db.commit();self.settings_provider=settings_provider or (lambda:pinned(None))
  self.mutex=threading.Lock();self.busy=threading.Lock();self.rpc=None;self.events=[]
 def connect(self):
  self.rpc=RPC(self.root)
  a=self.rpc.call('account/read',{'refreshToken':False})
  if (a.get('account') or {}).get('type')!='chatgpt':raise RuntimeError('需要现有 ChatGPT 登录，不自动使用 API key')
  x=self.rpc.call('thread/resume',{'threadId':self.tid,'excludeTurns':True,'approvalPolicy':'on-request','approvalsReviewer':'auto_review','sandbox':'workspace-write'},timeout=90)
  self.reconcile();return x
 def history(self):
  rows=[];cursor=None
  while True:
   p={'threadId':self.tid,'itemsView':'full','limit':50}
   if cursor:p['cursor']=cursor
   x=self.rpc.call('thread/turns/list',p);rows.extend(x['data']);cursor=x.get('nextCursor')
   if not cursor:return rows
 def audit(self,kind,**data):
  e={'at':time.time(),'kind':kind,**data};self.events.append(e)
  with (self.root/'scheduler.jsonl').open('a') as f:f.write(json.dumps(e,ensure_ascii=False)+'\n')
 def enqueue(self,bid,prompt,priority=10,settings=None):
  with self.mutex:
   old=self.db.execute('select * from jobs where id=?',(bid,)).fetchone()
   if old:
    if old['prompt']!=prompt:raise ValueError('batch ID collision')
    return False
   choice=pinned(settings if settings is not None else self.settings_provider())
   self.db.execute('insert into jobs(id,prompt,priority,state,created,model,effort,settings_revision) values(?,?,?,?,?,?,?,?)',(bid,prompt,priority,'queued',time.time(),choice['model'],choice['effort'],choice['revision']));self.db.commit()
  self.audit('enqueue',batch=bid,priority=priority,settings=choice);return True
 def rows(self):
  with self.mutex:return [dict(r) for r in self.db.execute('select * from jobs order by created')]
 def update(self,bid,**fields):
  with self.mutex:
   self.db.execute('update jobs set '+','.join(k+'=?' for k in fields)+' where id=?',(*fields.values(),bid));self.db.commit()
 def reconcile(self):
  history=self.history()
  for j in self.rows():
   if j['state'] not in ['submitting','inflight','uncertain']:continue
   matches=[]
   for t in history:
    prefix='BATCH_ID='+j['id']+'\n'+(f"ATTEMPT={j['attempt']}\n" if j['attempt'] else '')
    if t['id']==j['turn_id'] or (not j['turn_id'] and any(i.get('type')=='userMessage' and any(c.get('text','').startswith(prefix) for c in i.get('content',[])) for i in t['items'])):matches.append(t)
   if len(matches)==1:
    t=matches[0]
    if t['status']=='completed':self.complete(j,t,t['items'],recovered=True)
    elif t['status']=='inProgress':self.update(j['id'],state='inflight',turn_id=t['id'],result=json.dumps({'recovered_partial_items':t['items']},ensure_ascii=False))
    else:self.update(j['id'],state='failed',turn_id=t['id'],error='服务端状态 '+t['status']+' '+json.dumps(t.get('error'),ensure_ascii=False)+'；保留输入，需核对重试')
   else:self.update(j['id'],state='uncertain',error='未确认提交结果；暂停以防重复，需核对历史')
 def retry_confirmed_failed(self):
  """Only a user recovery action may retry a server-confirmed terminal failure.
  Completed/unknown submissions are never blindly sent again. Attempts remain audited.
  """
  history={t['id']:t for t in self.history()}
  for j in self.rows():
   if j['state']!='failed':continue
   t=history.get(j['turn_id'])
   if not t:raise RuntimeError('未能核实失败回合，保留输入并停止重发')
   if t['status']=='completed':self.complete(j,t,t['items'],recovered=True);continue
   if t['status'] not in ['failed','interrupted']:raise RuntimeError('服务端回合未终止，不另开回合')
   if j['attempt']>=3:raise RuntimeError('已达三次恢复尝试上限，保留输入并报告故障')
   self.audit('explicit-retry',batch=j['id'],previous_turn=j['turn_id'],previous_error=j['error'],attempt=j['attempt']+1)
   self.update(j['id'],state='queued',turn_id=None,result=None,error=None,finished=None,attempt=j['attempt']+1)
 def complete(self,j,t,items,recovered=False):
  unique={i.get('id',str(n)):i for n,i in enumerate(items)}
  result={'turn':t['id'],'status':t['status'],'items':list(unique.values()),'recovered':recovered,'requested_settings':pinned(j)}
  self.update(j['id'],state='completed' if t['status']=='completed' else 'failed',result=json.dumps(result,ensure_ascii=False),finished=time.time(),error=json.dumps(t.get('error'),ensure_ascii=False) if t.get('error') else None)
  self.audit('complete',batch=j['id'],turn=t['id'],recovered=recovered,status=t['status'],settings=pinned(j))
 def next(self,timeout=360,crash_after_completion=False):
  if not self.busy.acquire(False):raise RuntimeError('已有受控回合，拒绝并发启动')
  try:
   rows=self.rows();active=[j for j in rows if j['state'] in ['submitting','inflight','uncertain']]
   if active:
    if len(active)!=1 or active[0]['state']!='inflight':raise RuntimeError('有未确认回合，暂停自动提交')
    j=active[0];tid=j['turn_id']
   else:
    ready=sorted((j for j in rows if j['state']=='queued'),key=lambda j:(j['priority'],j['created']))
    if not ready:return None
    j=ready[0];self.update(j['id'],state='submitting',started=time.time())
    self.audit('start',batch=j['id'],settings=pinned(j))
    choice_note=f"本回合由用户本地设置指定模型{j['model']}、推理{j['effort']}。旧文件中的固定6.1-sol/high仅为旧默认，此处为用户显式选择；其他技能、法源核验和权限规则不变。不得自行换模型、调用子代理或付费API。\n"
    params={'threadId':self.tid,'clientUserMessageId':str(uuid.uuid5(uuid.NAMESPACE_URL,j['id']+'/'+str(j['attempt']))),'input':[{'type':'text','text':'BATCH_ID='+j['id']+'\n'+(f"ATTEMPT={j['attempt']}\n" if j['attempt'] else '')+choice_note+j['prompt']}],'model':j['model'],'effort':j['effort']}
    # This supported per-turn constraint does not change authentication,
    # permissions, reasoning effort, or later transcript turns.
    if j['id'].startswith('FINAL-'):params['outputSchema']=FINAL_SCHEMA
    x=self.rpc.call('turn/start',params,timeout=60)
    tid=x['turn']['id'];self.update(j['id'],state='inflight',turn_id=tid)
   deadline=time.monotonic()+timeout;items=json.loads(j.get('result') or '{}').get('recovered_partial_items',[])
   while time.monotonic()<deadline:
    try:e=self.rpc.events.get(timeout=1)
    except queue.Empty:continue
    m=e.get('method');p=e.get('params',{})
    if 'id' in e and m:
     self.audit('approval-required',request=e)
     atomic(self.root/'approval-blocked.json',{'at':time.time(),'batch':j['id'],'method':m,'request':clean(p),'state':'未批准，权限请求已拒绝','next':'请在系统维护聊天中核对这项具体权限；处理后回到此页检查并恢复连接。不会自动批准。'})
     self.rpc.send({'id':e['id'],'error':{'code':-32000,'message':'本地验收不代替用户批准额外权限。'}})
    if m=='item/completed' and p.get('turnId')==tid:items.append(p['item'])
    if m=='turn/completed' and p.get('turn',{}).get('id')==tid:
     if crash_after_completion:raise ConnectionError('FAULT: received completion before local commit')
     self.complete(j,p['turn'],items);return j['id']
    if m=='transport/closed':raise ConnectionError('App Server transport closed; input retained')
    if m=='error':self.audit('server-error',detail=p)
   raise TimeoutError('分析超时，保留正在处理批次，不直接重发')
  finally:self.busy.release()
 def close(self):
  if self.rpc:self.rpc.close()
  self.db.close();fcntl.flock(self.lockfile,fcntl.LOCK_UN);self.lockfile.close()
