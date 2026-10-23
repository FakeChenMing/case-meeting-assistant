import ast, hashlib, json, re, shutil, tempfile, threading, time, unittest, uuid
from pathlib import Path
from ledger import Store, atomic
from engine_v2 import upgrade
from dispatcher import Dispatcher, FINAL_SECTIONS
from model_settings import defaults, save_defaults


class SessionLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='case-home-test-');self.base=Path(self.tmp.name)
        self.templates=self.base/'templates';self.templates.mkdir()
        for n in range(7):(self.templates/f'{n:02d}-fixture.md').write_text('虚构模板测试')
        self.case=self.base/'fictional-case';self.case.mkdir();(self.case/'pack.md').write_text('虚构会前材料')
        # Use the actual base implementation without launching its threads or
        # reading/changing the installed runtime directory.
        tree=ast.parse((Path(__file__).resolve().parents[1]/'skills/case-meeting-assistant/assets/runtime/server.py').read_text())
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Engine')
        scope=dict(Path=Path,BASE=self.base,TEMPLATES=self.templates,Store=Store,atomic=atomic,
                   time=time,uuid=uuid,hashlib=hashlib,json=json,shutil=shutil,re=re,defaults=defaults,
                   MODEL='gpt-6.1-sol',EFFORT='high')
        exec(compile(ast.Module(body=[cls],type_ignores=[]),'actual_base','exec'),scope)
        def read(path,default=None):
            try:return json.loads(Path(path).read_text())
            except (OSError,ValueError):return default
        scope['read_json']=read
        self.calls=[];scope['native']=lambda *args,**kw:self.calls.append(args)
        self.klass=upgrade(scope['Engine'],self.base,self.templates,scope['native'],read,'')
        e=object.__new__(self.klass);e.operation_lock=threading.RLock();e.connect_lock=threading.Lock();e.lock=threading.RLock()
        e.store=None;e.disp=None;e.sid=None;e.root=None;e.init_lifecycle(self.base);self.e=e
        self.sid=e.create(str(self.case),str(self.case/'pack.md'));self.s=e.store
        self.s.put('phase','ended');self.s.put('thread','fictional-dedicated-thread')
        self.post=Path(self.s.get('artifact'))/'会后交接/v001';self.post.mkdir(parents=True)
        (self.post/'01-会后交接.md').write_text('虚构已保存交接')
        atomic(self.post/'完整性清单.json',{'state':'draft'})
        self.s.put('handoff',str(self.post));self.s.put('error',None)
        self.s.add_question('USER-old','虚构旧问题','question');self.s.qa_status('USER-old','已回答')
        self.s.put('marker','旧案件材料标记')
        save_defaults(self.base,{'model':'gpt-6-astra','effort':'medium'})

    def tearDown(self):
        if self.e.disp:self.e.disp.close()
        if self.e.store:self.e.store.db.close()
        self.tmp.cleanup()

    def test_return_home_retains_data_and_persists_empty_active_pointer(self):
        old=(self.post/'01-会后交接.md').read_bytes();path=self.e.root/'state.sqlite'
        self.e.return_home(self.sid)
        self.assertIsNone(self.e.store);self.assertIsNone(self.e.sid);self.assertEqual(self.e.view,'home')
        active=json.loads((self.base/'active.json').read_text());self.assertIsNone(active['session_id'])
        self.assertEqual(active['last_completed']['session'],self.sid)
        s=Store(path)
        try:self.assertEqual(s.get('marker'),'旧案件材料标记');self.assertEqual(s.qa()[0]['text'],'虚构旧问题')
        finally:s.db.close()
        self.assertEqual((self.post/'01-会后交接.md').read_bytes(),old)
        self.assertEqual(defaults(self.base)['model'],'gpt-6-astra')

    def test_restart_restores_home_and_history_is_read_only(self):
        self.e.return_home(self.sid);self.e.init_lifecycle(self.base)
        self.assertEqual(self.e.view,'home');self.assertEqual(self.e.recent_sessions()[0]['session'],self.sid)
        self.e.open_history(self.sid)
        self.assertEqual(self.e.view,'history');self.assertIsNone(self.e.disp)
        self.e.ensure_connected=lambda: self.fail('history must not contact a model')
        self.e._cycle()
        for op in ('start','question','action','correct','change_model','end'):
            with self.assertRaisesRegex(ValueError,'历史查看'):self.e.guard_action(op,self.sid)
        self.e.guard_action('resume_history',self.sid);self.e.resume_history()
        self.assertEqual(self.e.view,'active');self.assertFalse(self.e.store.get('return_home_after_finish'))
        self.assertEqual(self.calls,[],'resume history must not open the microphone')

    def test_new_case_has_empty_questions_transcript_and_independent_identity(self):
        self.e.return_home(self.sid)
        case=self.base/'different-fictional-case';case.mkdir();(case/'pack.md').write_text('另案虚构材料')
        sid=self.e.create(str(case),str(case/'pack.md'))
        self.assertNotEqual(sid,self.sid);self.assertIsNone(self.e.store.get('thread'))
        self.assertEqual(self.e.store.qa(),[]);self.assertEqual(self.e.store.questions(),[])
        self.assertEqual(self.e.store.events(),[]);self.assertIsNone(self.e.store.get('marker'))
        self.assertEqual(self.e.store.get('model_selection')['model'],'gpt-6-astra')

    def test_capture_tail_and_unknown_job_prevent_return_home(self):
        atomic(self.base/'capture-status.json',{'stopping':True})
        with self.assertRaisesRegex(ValueError,'尾段'):self.e.return_home(self.sid)
        atomic(self.base/'capture-status.json',{})
        self.e.disp=Dispatcher(self.e.root/'dispatcher','fictional')
        self.e.disp.enqueue('UNKNOWN','fictional input');self.e.disp.update('UNKNOWN',state='uncertain')
        with self.assertRaisesRegex(ValueError,'结果未知'):self.e.return_home(self.sid)
        self.assertEqual(self.e.sid,self.sid);self.assertEqual(self.e.view,'active')

    def test_failed_or_missing_handoff_never_clears_screen(self):
        self.s.put('error','fictional model failure')
        with self.assertRaisesRegex(ValueError,'故障'):self.e.return_home(self.sid)
        self.s.put('error',None);(self.post/'01-会后交接.md').unlink()
        with self.assertRaisesRegex(ValueError,'交接文件'):self.e.return_home(self.sid)
        self.assertEqual(self.e.sid,self.sid)

    def test_stale_tab_and_path_traversal_rejected(self):
        self.e.return_home(self.sid)
        with self.assertRaisesRegex(ValueError,'不一致'):self.e.guard_action('action',self.sid)
        with self.assertRaisesRegex(ValueError,'不一致'):self.e.guard_action('load',self.sid)
        with self.assertRaisesRegex(ValueError,'无效'):self.e.open_history('../outside')

    def test_history_with_missing_attachment_can_still_return_home(self):
        self.e.return_home(self.sid);(self.post/'01-会后交接.md').unlink()
        self.e.open_history(self.sid);self.e.store.put('error','historical error')
        self.e.return_home(self.sid)
        self.assertEqual(self.e.view,'home');self.assertIsNone(self.e.store)

    def test_active_worker_cannot_be_detached(self):
        acquired=threading.Event();release=threading.Event()
        def worker():
            with self.e.session_work_lock:acquired.set();release.wait(2)
        t=threading.Thread(target=worker);t.start();acquired.wait(1)
        try:
            with self.assertRaisesRegex(ValueError,'仍在处理'):self.e.return_home(self.sid)
            self.assertEqual(self.e.sid,self.sid)
        finally:release.set();t.join()

    def test_finish_existing_completed_record_returns_home_without_new_inference(self):
        self.e.finish();self.assertEqual(self.e.view,'home');self.assertEqual(self.calls,[])

    def test_waiting_closeout_only_returns_home_after_files_are_written(self):
        self.s.put('phase','ready');self.s.put('handoff',None);self.e.finish()
        self.assertEqual(self.e.view,'active');self.assertEqual(self.s.get('phase'),'ending')
        self.e.auto_return_home();self.assertEqual(self.e.view,'active')
        bid='FINAL-fictional';body={'batch_id':bid,'handoff':{k:'虚构单元测试，非实际模型输出。' for k in FINAL_SECTIONS}}
        self.s.put('synthesis',body);self.s.put('synthesis_needed',False)
        self.s.put('final_state:'+bid,self.e.final_fingerprint())
        self.e.handoff()
        self.assertEqual(self.e.view,'home');self.assertIsNone(self.e.sid)
        post=Path(self.e.last_completed['handoff']);self.assertTrue((post/'01-会后交接.md').is_file())
        self.assertEqual(json.loads((post/'完整性清单.json').read_text())['state'],'draft')


if __name__=='__main__':unittest.main(verbosity=2)
