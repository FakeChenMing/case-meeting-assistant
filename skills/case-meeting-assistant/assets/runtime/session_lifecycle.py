"""Separate the empty home screen from active work and read-only history."""
import json, re, sqlite3, threading, time
from pathlib import Path
from contextlib import contextmanager
from ledger import Store, atomic


def read_json(path, default=None):
    try: return json.loads(Path(path).read_text())
    except (OSError, ValueError): return default


class SessionLifecycle:
    def init_lifecycle(self, base):
        self.lifecycle_base = Path(base)
        self.session_work_lock = threading.RLock()
        active = read_json(self.lifecycle_base/'active.json', {})
        self.view = active.get('view', 'active') if active.get('session_id') else 'home'
        self.last_completed = active.get('last_completed')
        self.history_cache = None

    @contextmanager
    def session_switch(self, history_exit=False):
        # A model turn can run for minutes. Never wait behind it while hiding
        # the current screen or moving its Store to a different case.
        with self.operation_lock:
            if not self.session_work_lock.acquire(False):
                raise ValueError('当前分析或收尾仍在处理，请等待完成后再返回首页')
            try:
                with self.lock:
                    self.assert_session_idle(history_exit)
                    yield
            finally:
                self.session_work_lock.release()

    def assert_session_idle(self, history_exit=False):
        capture = read_json(self.lifecycle_base/'capture-status.json', {})
        if any(capture.get(k) for k in ('running', 'stopping', 'busy')):
            raise ValueError('录音尚未停止或尾段仍在保存，请先结束并生成交接')
        if not self.store or (history_exit and self.view=='history'):
            return
        if self.store.get('phase') != 'ended' or self.store.get('error'):
            raise ValueError('当前沟通尚未完成，或仍有故障；请先完成交接，页面和记录均保留')
        if self.disp:
            if self.disp.busy.locked(): raise ValueError('当前模型回合仍在处理')
            jobs = self.disp.rows()
        else:
            path = self.root/'dispatcher/queue.sqlite'
            jobs = []
            if path.exists():
                with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as db:
                    jobs = [{'state': r[0]} for r in db.execute('SELECT state FROM jobs')]
        if any(j['state'] in ('queued', 'inflight', 'submitting', 'uncertain', 'blocked') for j in jobs):
            raise ValueError('还有未完成或结果未知的分析批次，暂不清空当前页面')
        if any(q['state']=='排队中' for q in self.store.qa()):
            raise ValueError('仍有已保存但未处理的问题，请先完成回答和交接')

    def saved_handoff(self):
        if not self.store: return None
        path = self.store.get('handoff')
        if not path: return None
        root = Path(path)
        manifest = read_json(root/'完整性清单.json', {})
        if not (root/'01-会后交接.md').is_file() or manifest.get('state') not in ('draft', 'ready'):
            return None
        return {'session': self.sid, 'matter': self.store.get('matter'), 'handoff': path,
                'state': manifest['state'], 'saved_at': time.time(),
                'has_gaps': bool(manifest.get('capture_gaps') or manifest.get('capture_error')
                                 or manifest.get('unclosed_audio_preserved') or manifest.get('unfinalized_segments'))}

    def return_home(self, expected_session):
        with self.session_switch(history_exit=True):
            if expected_session != self.sid: raise ValueError('页面会话已变化，请刷新后操作')
            completed = self.saved_handoff() if self.store else self.last_completed
            if self.store and not completed and self.view!='history':
                raise ValueError('尚未确认会后交接文件已保存，当前页面保留，请先检查交接')
            if not completed: completed=self.last_completed
            # Persist the empty landing page before releasing database handles.
            # A crash/restart then cannot restore the old case as active work.
            atomic(self.lifecycle_base/'active.json', {'session_id': None, 'view': 'home',
                                                       'last_completed': completed})
            if self.disp: self.disp.close(); self.disp = None
            if self.store: self.store.db.close()
            self.store = None; self.sid = None; self.root = None
            self.view = 'home'; self.last_completed = completed; self.history_cache = None
            return {'notice': '已返回首页。录音、问答、问题处理和交接仍保存在历史沟通记录中。'}

    def finish(self):
        with self.operation_lock:
            if not self.store or self.view != 'active': raise ValueError('请先进入要结束的沟通')
            self.store.put('return_home_after_finish', True)
            if self.store.get('phase') == 'ended':
                return self.return_home(self.sid)
            super().finish()
            return {'notice': '正在停止录音、保存尾段和整理交接；成功保存后自动返回首页。'}

    def auto_return_home(self):
        if self.store and self.store.get('phase') == 'ended' and self.store.get('return_home_after_finish'):
            return self.return_home(self.sid)

    def recent_sessions(self):
        # Read metadata only. Merely opening the home screen never connects to
        # Codex, starts inference, or changes a historical session database.
        now = time.monotonic()
        if self.history_cache and now-self.history_cache[0]<3: return self.history_cache[1]
        rows = []
        for root in sorted((self.lifecycle_base/'sessions').glob('*'), reverse=True):
            path = root/'state.sqlite'
            if not path.is_file() or not re.fullmatch(r'[\w-]{1,100}', root.name): continue
            try:
                with sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=.2) as db:
                    meta = {k: json.loads(v) for k,v in db.execute(
                        "SELECT k,v FROM meta WHERE k IN ('matter','created','phase','handoff','case')")}
                if meta.get('phase')!='ended': continue
                handoff = meta.get('handoff')
                rows.append({'session':root.name,'matter':meta.get('matter',root.name),
                             'created':meta.get('created'),'handoff':handoff,
                             'handoff_exists':bool(handoff and (Path(handoff)/'01-会后交接.md').is_file())})
                if len(rows)>=30: break
            except (sqlite3.Error, ValueError, OSError): continue
        self.history_cache=(now,rows)
        return rows

    def open_history(self, sid):
        with self.session_switch():
            if self.store: raise ValueError('请先返回首页，再选择历史沟通')
            if not isinstance(sid,str) or not re.fullmatch(r'[\w-]{1,100}',sid): raise ValueError('历史会话标识无效')
            root=(self.lifecycle_base/'sessions'/sid).resolve()
            if root.parent!=(self.lifecycle_base/'sessions').resolve(): raise ValueError('历史会话路径无效')
            if not (root/'state.sqlite').is_file(): raise ValueError('历史会话不存在')
            store=Store(root/'state.sqlite')
            if store.get('phase')!='ended':
                store.db.close();raise ValueError('该会话尚未结束，不能作为已完成历史记录打开')
            try:
                atomic(self.lifecycle_base/'active.json',{'session_id':sid,'view':'history','last_completed':self.last_completed})
            except Exception:
                store.db.close();raise
            self.store=store;self.sid=sid;self.root=root;self.view='history'
            return {'notice':'正在查看历史记录；不会自动开始录音或分析。'}

    def resume_history(self):
        with self.session_switch():
            if self.view!='history' or not self.store: raise ValueError('请先打开历史沟通记录')
            self.store.put('return_home_after_finish',False)
            atomic(self.lifecycle_base/'active.json',{'session_id':self.sid,'view':'active','last_completed':self.last_completed})
            self.view='active'
            return {'notice':'已进入原沟通。需要续录时，再点“继续本次会话录音”。'}

    def guard_action(self, op, requested_session):
        if op!='choose' and requested_session!=self.sid:
            raise ValueError('页面与当前案件会话不一致，操作未执行；请刷新核对身份')
        allowed={'home':('choose','load','refresh_models','change_model','open_history','home'),
                 'history':('home','resume_history','refresh_models')}
        if self.view in allowed and op not in allowed[self.view]:
            raise ValueError('当前是首页或历史查看页，请先新建沟通或明确继续原沟通')
