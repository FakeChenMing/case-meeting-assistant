#!/usr/bin/env python3
"""Fictional interactive UI tour. No microphone, network model, or real case reads."""
import argparse, json, secrets, sys, tempfile, time, uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlparse,parse_qs

SKILL=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SKILL/'assets/runtime'))
from ledger import Store

class Demo:
    def __init__(self):
        self.root=Path(tempfile.mkdtemp(prefix='case-assistant-demo-'))
        self.s=Store(self.root/'demo.sqlite');self.token=secrets.token_urlsafe(24)
        self.view='home';self.phase='ready';self.paused=False;self.error=None
        self.sid='DEMO-20261023-01';self.model={'model':'gpt-6.1-sol','effort':'high','revision':0}
        self.recording=False;self.history=False
        self.questions=[
            ('payment.purpose','已付的三万元用于哪一项约定？','付款用途尚未明确。','核对已付金额与约定项目的对应。','E000001 · 00:04；材料/虚构会前说明.md'),
            ('contract.signature_date','请核对合同签署日期，并指出合同上的位置。','识别日期可能有误，需回听并核对原件。','影响事实时间线，法律影响待核。','E000002 · 00:09'),
            ('acceptance.record','验收记录由谁保存，现在能否提供？','目前尚未取得该材料。','核对验收记录是否存在及具体内容。','E000004 · 00:22'),
            ('invoice.copy','能否提供与这笔款项对应的票据？','付款凭证尚缺。','核对材料与陈述的对应关系。','材料/虚构会前说明.md 第3段'),
            ('original.custodian','合同原件目前由谁保管？','需要确定可核验原件的位置。','安排后续材料核对。','材料/虚构会前说明.md 第4段'),
            ('delivery.correction','“还没有交付”是在更正先前“已经交付”吗？','后一句否定了先前的交付说法。','需要调整交付事实记录，不能直接确定法律责任。','E000003 · 00:15'),
        ]
        self.merge('DEMO-FIRST',[self.q(i) for i in range(5)])
        events=[]
        for i,text in enumerate(['已经支付三万元，付款用途还没有确认。','合同日期是三月十二日。','更正一下，货物还没有交付，不是已经交付。','验收记录目前没有收到。'],1):
            events.append(dict(event_seq=i,recording_id='DEMO-R01',session_id='DEMO-R01',matter_id='虚构沟通示范',segment_id='S'+str(i),revision=1,event_type='segment',is_final=True,text=text,audio_start_ms=[4000,9000,15000,22000][i-1],audio_end_ms=[8000,14000,21000,26000][i-1]))
        p=self.root/'events.jsonl';p.write_text(''.join(json.dumps(e,ensure_ascii=False)+'\n' for e in events))
        self.s.ingest(p,'DEMO-R01','虚构沟通示范',self.sid)
    def q(self,i):
        slot,q,reason,impact,source=self.questions[i]
        return dict(issue_key=slot,fact_slot=slot,question=q,reason=reason,impact=impact,source=source,urgency=3 if i==5 else 0,key_change=i==5,new_source=i==5)
    def merge(self,bid,qs,**kw):
        self.s.put('rev:'+bid,self.s.revision());self.s.merge(bid,dict(batch_id=bid,questions=qs,summary='【脚本示例】已付三万元；付款用途、日期和交付情况待核。法律判断未作出。',answer='',**kw),4)
    def state(self):
        home=self.view=='home'
        d=dict(version='0.3.0-preview · 演示',view=self.view,session=None if home else self.sid,matter='虚构沟通示范',thread='DEMO-ONLY · 未连接真实Codex',case='示例案件',pack='会前交接/v001',artifact='沟通记录-GPT/DEMO-20261023-01',phase='home' if home else self.phase,paused=self.paused,
            model=self.model['model'],effort=self.model['effort'],model_selection=self.model,model_defaults=self.model,
            model_catalog={'fresh':True,'fetched_at':1,'source':'示例列表，非本机权限验证','models':[{'model':'gpt-6.1-sol','label':'6.1 Sol（示例）','efforts':['medium','high'],'default_effort':'high'},{'model':'gpt-6-astra','label':'Astra（示例）','efforts':['medium','high'],'default_effort':'high'}]},
            capture_connected=not home,capture_for_session=not home,capture={'running':self.recording,'input_confirmed':self.recording,'microphone':'演示设备（未启用麦克风）','actual_device_id':'DEMO','audio_samples':0,'engine':'演示状态，未运行识别','source_channel':'demo_only','audio_closed':self.phase=='ended'},auth={'type':'演示：未登录/未消耗额度'},questions=[] if home else self.s.questions(),visible=[] if home else self.s.visible(),proposals=[] if home else self.s.proposal_rows(),qa=[] if home else self.s.qa(),jobs=[],transcript=[] if home else self.s.current_segments(),audio=[],recordings=[] if home else ['DEMO-R01'],received=4,processed=4,backlog=0,correction_revision=self.s.revision(),correction_processed=self.s.get('correction_processed',0),latest_result=self.s.get('latest_result'),latest_result_stale=self.s.get('latest_result_stale'),priority_notice=self.s.get('priority_notice'),blocked_permission=None,error=self.error,
            recent_sessions=[{'session':self.sid,'matter':'虚构沟通示范','handoff_exists':True}] if self.history else [],handoff='沟通记录-GPT/DEMO-20261023-01/会后交接/v001' if self.history else None)
        return d
    def act(self,d):
        op=d.get('op')
        if op=='load':self.view='active';self.phase='ready'
        elif op=='choose':raise ValueError('演示不读取您的文件，请点“载入虚构验收材料”')
        elif op=='start':self.recording=True;self.phase='recording'
        elif op=='pause':self.paused=True
        elif op=='resume':self.paused=False
        elif op=='end':self.recording=False;self.phase='ended';self.view='home';self.history=True
        elif op=='home':self.view='home'
        elif op=='open_history':self.view='history';self.phase='ended'
        elif op=='resume_history':self.view='active';self.phase='ready'
        elif op=='action':self.s.action(d['id'],d['state'],d['op_id'])
        elif op=='proposal':self.s.resolve_proposal(d['id'],d['allow'])
        elif op=='correct':self.s.correct(d['recording'],d['segment'],d['text'],d['expected_revision'],d['op_id'])
        elif op=='question':
            bid='USER-'+uuid.uuid4().hex;self.s.add_question(bid,d['text'],d.get('intent','question'))
            self.s.put('rev:'+bid,self.s.revision());self.s.merge(bid,{'batch_id':bid,'questions':[],'summary':'脚本示例','answer':'【预置演示回答】先核对付款凭证、合同日期和验收记录。该内容不是实际GPT分析，不作法律结论。','answer_sources':['E000001','E000004']},4)
        elif op=='change_model':self.model={k:d[k] for k in ['model','effort']};self.model['revision']=d.get('expected_revision',0)+1
        elif op=='demo_important':self.merge('DEMO-IMPORTANT',[self.q(5)])
        elif op=='demo_fault':self.error='【故障示意】未确认提交结果；已保留输入，请先核对历史，不盲目重发。'
        elif op=='retry_connection':self.error=None
        elif op in ['paint','refresh_models','recheck_materials']:pass
        else:raise ValueError('演示未开放此操作；没有录音或模型连接')
        return {'notice':'演示状态已保存；未调用麦克风或GPT。'}

def main():
    p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=34972);a=p.parse_args();d=Demo()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):pass
        def response(self,x,html=False,status=200):
            data=(x if html else json.dumps(x,ensure_ascii=False)).encode();self.send_response(status);self.send_header('Content-Type','text/html;charset=utf-8' if html else 'application/json');self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(data)
        def do_GET(self):
            if self.headers.get('Host')!=f'127.0.0.1:{a.port}':return self.response({'error':'local only'},status=403)
            url=urlparse(self.path)
            if url.path=='/api/state':return self.response(d.state())
            if url.path!='/':return self.response({'error':'演示未提供原音频'},status=404)
            html=(SKILL/'assets/runtime/index.html').read_text().replace('{{TOKEN}}',d.token).replace('{{DEMO_CASE}}','示例案件')
            banner='<div style="background:#fff1d9;padding:14px 24px;border-bottom:2px solid #d4a947"><b>功能演示 · 全部内容虚构</b>　未录音、未调用GPT，文字与状态均为脚本示例。<button onclick="act(\'demo_important\')">演示重要第六条</button> <button onclick="act(\'demo_fault\')">演示故障提示</button></div>'
            html=html.replace('<header>',banner+'<header>')
            if parse_qs(url.query).get('overview')==['1']:html=html.replace('</style>','body{zoom:.48} .scroll{max-height:none}</style>')
            self.response(html,html=True)
        def do_POST(self):
            if self.headers.get('Host')!=f'127.0.0.1:{a.port}' or self.headers.get('X-CSRF')!=d.token or self.headers.get('Origin',f'http://127.0.0.1:{a.port}')!=f'http://127.0.0.1:{a.port}':return self.response({'error':'local only'},status=403)
            try:
                n=int(self.headers.get('Content-Length',0))
                if not 0<n<200000:raise ValueError('invalid size')
                self.response({'ok':True,'result':d.act(json.loads(self.rfile.read(n)))})
            except Exception as e:self.response({'error':str(e)},status=400)
    print(f'演示页面 http://127.0.0.1:{a.port}/；未录音/未调用GPT；临时演示状态 {d.root}',flush=True)
    ThreadingHTTPServer(('127.0.0.1',a.port),Handler).serve_forever()
if __name__=='__main__':main()
