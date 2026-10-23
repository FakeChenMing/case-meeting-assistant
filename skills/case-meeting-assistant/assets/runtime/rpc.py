import json,os,subprocess,threading,queue,time,re
from pathlib import Path
from runtime_config import codex_cli
def clean(x):
 if isinstance(x,dict):return {k:('[REDACTED]' if re.search(r'(^email$|token$|api.?key|authorization|cookie|secret|password)',k,re.I) else clean(v)) for k,v in x.items()}
 if isinstance(x,list):return [clean(v) for v in x]
 if isinstance(x,str):return re.sub(r'(?i)(Bearer\s+)[\w.\-]+',r'\1[REDACTED]',x)
 return x
class RPC:
 def __init__(self,root):
  self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);os.chmod(self.root,0o700)
  self.pending={};self.events=queue.Queue();self.counter=0;self.lock=threading.Lock();self.started=time.monotonic()
  env=os.environ.copy()
  proxy=subprocess.run(['/usr/sbin/scutil','--proxy'],capture_output=True,text=True).stdout
  for proto in ['HTTP','HTTPS']:
   enabled=re.search(r'\b'+proto+r'Enable : 1',proxy);host=re.search(r'\b'+proto+r'Proxy : (\S+)',proxy);port=re.search(r'\b'+proto+r'Port : (\d+)',proxy)
   if enabled and host and port and host.group(1) in ['127.0.0.1','localhost']:
    env[proto+'_PROXY']='http://'+host.group(1)+':'+port.group(1)
  test_proxy=self.root.parent/'test-proxy.json'
  if test_proxy.exists():
   spec=json.loads(test_proxy.read_text())
   if spec.get('test_case')!='demo-round2' or spec.get('port')!=34962:raise ValueError('测试网络代理配置无效')
   env['HTTP_PROXY']=env['HTTPS_PROXY']='http://127.0.0.1:34962'
  env['NO_PROXY']='127.0.0.1,localhost'
  self.p=subprocess.Popen([codex_cli(),'-c','mcp_servers.lawmcp.enabled=false','-c','mcp_servers.qcc-history.enabled=false','-c','model_reasoning_effort="high"','-c','approval_policy="on-request"','-c','approvals_reviewer="auto_review"','-c','sandbox_mode="workspace-write"','app-server','--listen','stdio://'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1,env=env)
  threading.Thread(target=self.reader,daemon=True).start();threading.Thread(target=self.errreader,daemon=True).start()
  self.call('initialize',{'clientInfo':{'name':'case_meeting_assistant','title':'案件沟通专用分析会话','version':'0.3.0-preview'},'capabilities':{'experimentalApi':True}})
  self.send({'method':'initialized'})
 def record(self,kind,x):
  with self.lock:
   with (self.root/'rpc-events.jsonl').open('a') as f:f.write(json.dumps({'at':time.time(),'kind':kind,'data':clean(x)},ensure_ascii=False)+'\n')
 def reader(self):
  for line in self.p.stdout:
   try:x=json.loads(line)
   except ValueError:continue
   self.record('receive',x)
   if 'method' not in x and x.get('id') in self.pending:self.pending[x['id']].put(x)
   else:self.events.put(x)
  self.events.put({'method':'transport/closed','params':{'exit':self.p.poll()}})
 def errreader(self):
  for line in self.p.stderr:self.record('stderr',line.strip())
 def send(self,x):
  self.record('send',x);self.p.stdin.write(json.dumps(x,ensure_ascii=False)+'\n');self.p.stdin.flush()
 def call(self,method,params=None,timeout=45):
  self.counter+=1;mid=self.counter;q=queue.Queue();self.pending[mid]=q
  x={'id':mid,'method':method}
  if params is not None:x['params']=params
  self.send(x)
  try:r=q.get(timeout=timeout)
  finally:self.pending.pop(mid,None)
  if 'error' in r:raise RuntimeError(json.dumps(r['error'],ensure_ascii=False))
  return r.get('result')
 def close(self):
  if self.p.poll() is not None:return
  self.p.terminate()
  try:self.p.wait(timeout=4)
  except subprocess.TimeoutExpired:self.p.kill()
