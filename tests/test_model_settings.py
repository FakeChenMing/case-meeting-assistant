import json, sqlite3, tempfile, unittest, time, threading
from pathlib import Path
from model_settings import ModelCatalog, DEFAULT, selection, save_selection, defaults, save_defaults
from dispatcher import Dispatcher
from ledger import Store
from engine_v2 import upgrade
from test_dispatcher_recovery import FakeRPC


def model(name, efforts=('high', 'low', 'ultra')):
    return {'model': name, 'displayName': name.upper(), 'inputModalities': ['text'],
            'supportedReasoningEfforts': [{'reasoningEffort': e} for e in efforts],
            'defaultReasoningEffort': 'low'}


class CatalogRPC:
    def __init__(self, root): self.closed = False; self.calls = []
    def close(self): self.closed = True
    def call(self, method, params):
        self.calls.append((method, params))
        if method == 'account/read': return {'account': {'type': 'chatgpt'}}
        if not params.get('cursor'): return {'data': [model('first')], 'nextCursor': 'page2'}
        return {'data': [model('second', ('high',))], 'nextCursor': None}


class ModelSettingsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='case-model-test-')
        self.root = Path(self.temp.name); self.s = Store(self.root/'state.sqlite')
        self.d = Dispatcher(self.root/'queue', 'fake-thread', lambda: selection(self.s))
    def tearDown(self): self.d.close(); self.s.db.close(); self.temp.cleanup()
    def change(self, model='alternate', effort='high', revision=0, op='choice-operation-1'):
        return save_selection(self.s, {'model': model, 'effort': effort}, revision, op)

    def test_future_jobs_switch_but_queued_and_inflight_are_pinned(self):
        self.d.rpc = FakeRPC()
        self.d.enqueue('old', 'old prompt')
        self.change()
        self.d.enqueue('new', 'new prompt')
        self.d.next(); self.d.next()
        calls = [p for m,p in self.d.rpc.calls if m == 'turn/start']
        self.assertEqual([p['model'] for p in calls], ['gpt-6.1-sol', 'alternate'])
        self.assertEqual([p['effort'] for p in calls], ['high', 'high'])
        self.assertEqual([p['threadId'] for p in calls], ['fake-thread']*2)
        self.assertIn('本回合由用户本地设置指定模型alternate', calls[1]['input'][0]['text'])
        self.d.enqueue('inflight', 'input'); self.d.update('inflight', state='inflight', turn_id='active')
        self.change('third', 'low', 1, 'choice-operation-2')
        self.assertEqual(self.d.rows()[-1]['model'], 'alternate')

    def test_selection_history_and_queue_survive_restart(self):
        self.change(); self.d.enqueue('saved', 'input')
        self.d.close(); self.s.db.close()
        self.s = Store(self.root/'state.sqlite')
        self.d = Dispatcher(self.root/'queue', 'fake-thread', lambda: selection(self.s))
        self.assertEqual(selection(self.s)['model'], 'alternate')
        self.assertEqual(self.d.rows()[0]['model'], 'alternate')
        self.assertEqual(len(self.s.get('model_changes')), 1)
        self.s.export(self.root/'export')
        export=json.loads((self.root/'export/模型使用记录.json').read_text())
        self.assertEqual(export['current_for_new_jobs']['model'], 'alternate')

    def test_duplicate_choice_idempotent_stale_tab_rejected(self):
        self.change(); self.change()
        self.assertEqual(len(self.s.get('model_changes')), 1)
        with self.assertRaisesRegex(ValueError, '其他页面'):
            self.change('third', op='different-operation')
        with self.assertRaisesRegex(ValueError, '不一致'):
            self.change('third')

    def test_old_session_does_not_inherit_new_default(self):
        save_defaults(self.root, {'model':'alternate', 'effort':'low'})
        self.assertEqual(defaults(self.root)['model'], 'alternate')
        self.assertEqual(selection(self.s), DEFAULT)

    def test_migrate_old_jobs_does_not_relabel_history(self):
        old=self.root/'old'; old.mkdir(); db=sqlite3.connect(old/'queue.sqlite')
        db.execute('CREATE TABLE jobs(id TEXT PRIMARY KEY,prompt TEXT,priority INTEGER,state TEXT,turn_id TEXT,result TEXT,created REAL,started REAL,finished REAL,error TEXT)')
        db.execute("INSERT INTO jobs(id,prompt,state) VALUES('legacy','input','completed')"); db.commit(); db.close()
        d=Dispatcher(old,'thread',lambda:{'model':'other','effort':'low'})
        try:
            self.assertEqual(d.rows()[0]['model'], 'gpt-6.1-sol')
            self.assertEqual(d.rows()[0]['effort'], 'high')
            self.assertEqual(d.rows()[0]['state'], 'completed')
        finally: d.close()

    def test_catalog_pagination_validation_and_no_inference(self):
        instances=[]
        def factory(root): r=CatalogRPC(root);instances.append(r);return r
        c=ModelCatalog(self.root/'catalog',factory); c.refresh(); c.worker.join(2)
        self.assertTrue(c.snapshot()['fresh']); self.assertTrue(instances[0].closed)
        self.assertEqual([m['model'] for m in c.snapshot()['models']], ['first','second'])
        self.assertEqual(c.validate('second','high'), {'model':'second','effort':'high'})
        for name,effort in [('unknown','high'),('second','low'),('first','ultra')]:
            with self.assertRaises(ValueError): c.validate(name,effort)
        self.assertEqual([m for m,p in instances[0].calls], ['account/read','model/list','model/list'])

    def test_catalog_failure_and_api_key_do_not_fallback(self):
        class KeyRPC(CatalogRPC):
            def call(self,m,p):
                if m=='account/read':return {'account': {'type':'apiKey'}}
                raise AssertionError('must not read models with API key')
        c=ModelCatalog(self.root/'catalog',KeyRPC);c.refresh();c.worker.join(2)
        self.assertFalse(c.snapshot()['fresh']);self.assertIn('不使用 API',c.snapshot()['error'])
        with self.assertRaises(ValueError):c.validate('first','high')
        self.assertEqual(selection(self.s),DEFAULT)

    def test_expired_catalog_and_repeated_cursor_rejected(self):
        c=ModelCatalog(self.root/'catalog',CatalogRPC);c.refresh();c.worker.join(2)
        c.state['fetched_at']=time.time()-901
        with self.assertRaises(ValueError):c.validate('first','high')
        class RepeatRPC(CatalogRPC):
            def call(self,m,p):
                if m=='account/read':return super().call(m,p)
                return {'data':[model('first')],'nextCursor':'same'}
        c=ModelCatalog(self.root/'catalog2',RepeatRPC);c.refresh();c.worker.join(2)
        self.assertFalse(c.snapshot()['fresh']);self.assertIn('分页异常',c.snapshot()['error'])

    def test_failed_inference_is_explicit_and_keeps_selected_model(self):
        class FailingRPC(FakeRPC):
            def call(self,m,p,**kw):
                self.calls.append((m,p));raise RuntimeError('model unavailable')
        self.change(); self.d.rpc=FailingRPC();self.d.enqueue('fail','input')
        with self.assertRaisesRegex(RuntimeError,'unavailable'):self.d.next()
        self.assertEqual(len(self.d.rpc.calls),1)
        self.assertEqual(selection(self.s)['model'],'alternate')
        with self.assertRaisesRegex(RuntimeError,'未确认'):self.d.next()
        self.assertEqual(len(self.d.rpc.calls),1)

    def test_engine_change_is_explicit_and_does_not_start_job(self):
        cls=upgrade(object,self.root,self.root,None,lambda p,d=None:d,'')
        e=object.__new__(cls);e.operation_lock=threading.RLock();e.lock=threading.RLock()
        e.sid='fictional';e.store=self.s;e.disp=self.d;e.model_catalog=ModelCatalog(self.root/'catalog',CatalogRPC)
        e.model_catalog.refresh();e.model_catalog.worker.join(2)
        d={'session_id':'fictional','model':'second','effort':'high','expected_revision':0,'op_id':'apply-once-123','remember':True}
        result=e.change_model(d)
        self.assertEqual(result['selection']['model'],'second');self.assertEqual(defaults(self.root)['model'],'second')
        self.assertEqual(self.d.rows(),[])
        with self.assertRaisesRegex(ValueError,'案件已切换'):e.change_model({**d,'session_id':'wrong'})

    def test_orphan_question_recovery_keeps_original_selection(self):
        class Base:
            def ensure_connected(self):pass
        cls=upgrade(Base,self.root,self.root,None,lambda p,d=None:d,'')
        e=object.__new__(cls);e.store=self.s;e.disp=self.d
        self.s.put('user_settings:USER-test', {'model':'old-choice','effort':'low','revision':4})
        self.s.put('user_prompt:USER-test', 'original input');self.s.add_question('USER-test','fictional question','question')
        self.change();e._ensure_connected()
        self.assertEqual(self.d.rows()[0]['model'],'old-choice')
        self.assertEqual(self.d.rows()[0]['effort'],'low')


if __name__ == '__main__': unittest.main(verbosity=2)
