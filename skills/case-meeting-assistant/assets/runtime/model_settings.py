"""Local model choices; catalog discovery never starts an inference turn."""
import json, re, threading, time
from pathlib import Path
from rpc import RPC, clean
from store import atomic

MODEL = 'gpt-6.1-sol'
EFFORT = 'high'
DEFAULT = {'model': MODEL, 'effort': EFFORT, 'revision': 0}


def selection(store):
    # Pre-selector sessions retain their original configuration, independently
    # of any subsequently chosen default for new sessions.
    return dict(store.get('model_selection', DEFAULT) if store else DEFAULT)


def pinned(value):
    value = value or DEFAULT
    return {'model': value.get('model', MODEL), 'effort': value.get('effort', EFFORT),
            'revision': value.get('revision', value.get('settings_revision', 0))}


def save_selection(store, value, expected_revision, op_id):
    if not isinstance(op_id, str) or not re.fullmatch(r'[\w-]{8,100}', op_id):
        raise ValueError('切换请求标识无效，请刷新后重试')
    with store.lock:
        current = selection(store)
        history = store.get('model_changes', [])
        for old in history:
            if old['op_id'] == op_id:
                if pinned(old['after'])['model'] != value['model'] or old['after']['effort'] != value['effort']:
                    raise ValueError('同一次切换请求的内容不一致')
                return current
        if type(expected_revision) is not int or current['revision'] != expected_revision:
            raise ValueError('模型设置已在其他页面更新，请刷新核对后再应用')
        if all(current[k] == value[k] for k in ('model', 'effort')):
            return current
        updated = {**pinned(value), 'revision': current['revision'] + 1, 'at': time.time()}
        history.append({'at': updated['at'], 'op_id': op_id, 'source': 'local_page_explicit_choice',
                        'before': current, 'after': updated})
        for key, data in [('model_selection', updated), ('model_changes', history)]:
            store.db.execute('INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)',
                             (key, json.dumps(data, ensure_ascii=False)))
        store.db.commit()
        return updated


class ModelCatalog:
    """A separate read-only transport avoids racing the analysis RPC stream."""
    max_age = 900

    def __init__(self, root, rpc_factory=RPC):
        self.root = Path(root)
        self.rpc_factory = rpc_factory
        self.lock = threading.RLock()
        self.state = {'models': [], 'loading': False, 'error': None, 'fetched_at': None}
        self.worker = None

    def snapshot(self):
        with self.lock:
            data = json.loads(json.dumps(self.state))
        data['fresh'] = bool(data['fetched_at'] and time.time() - data['fetched_at'] < self.max_age
                             and not data['error'] and not data['loading'])
        return data

    def refresh(self):
        with self.lock:
            if self.state['loading']:
                return False
            self.state.update(loading=True, error=None)
            self.worker = threading.Thread(target=self._load, daemon=True)
            self.worker.start()
        return True

    def _load(self):
        rpc = None
        try:
            rpc = self.rpc_factory(self.root / ('read-' + str(time.time_ns())))
            account = rpc.call('account/read', {'refreshToken': False}).get('account') or {}
            if account.get('type') != 'chatgpt':
                raise ValueError('需要本机现有 ChatGPT 登录；不使用 API Key 或另行计费接口')
            models, cursor, seen = {}, None, set()
            for _ in range(20):
                params = {'limit': 100, 'includeHidden': False}
                if cursor:
                    params['cursor'] = cursor
                page = rpc.call('model/list', params)
                for row in page.get('data', []):
                    model = row.get('model')
                    if row.get('hidden') or not isinstance(model, str) or not model:
                        continue
                    if row.get('inputModalities') and 'text' not in row['inputModalities']:
                        continue
                    efforts = [x['reasoningEffort'] for x in row.get('supportedReasoningEfforts', [])
                               if isinstance(x.get('reasoningEffort'), str) and x['reasoningEffort'] != 'ultra']
                    # Ultra advertises automatic delegation. This system retains
                    # the user's explicit no-subagent workflow boundary.
                    if not efforts:
                        continue
                    models[model] = {'model': model, 'label': row.get('displayName') or model,
                                     'efforts': efforts, 'default_effort': row.get('defaultReasoningEffort'),
                                     'verification': 'listed_not_inference_verified'}
                cursor = page.get('nextCursor')
                if not cursor:
                    break
                if cursor in seen:
                    raise ValueError('模型目录分页异常，未采用不完整目录')
                seen.add(cursor)
            else:
                raise ValueError('模型目录未完整取得，请重试')
            if not models:
                raise ValueError('当前登录未返回可用的文字分析模型')
            with self.lock:
                self.state = {'models': list(models.values()), 'loading': False, 'error': None,
                              'fetched_at': time.time(), 'auth_type': 'chatgpt',
                              'source': 'local_codex_app_server_model/list'}
        except Exception as exc:
            with self.lock:
                self.state.update(loading=False, error=str(clean(str(exc))))
        finally:
            if rpc:
                rpc.close()

    def validate(self, model, effort):
        state = self.snapshot()
        if not state['fresh']:
            raise ValueError('请先刷新模型列表；读取失败或过期时不接受新的切换')
        option = next((m for m in state['models'] if m['model'] == model), None)
        if not option or effort not in option['efforts']:
            raise ValueError('该模型或推理强度不在当前本机 Codex 返回的可选范围内')
        return {'model': model, 'effort': effort}


def defaults(base):
    path = Path(base) / 'model-preferences.json'
    if not path.exists():
        return dict(DEFAULT)
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not all(isinstance(data.get(k), str) for k in ('model', 'effort')):
        raise ValueError('本地模型偏好文件损坏，未自动换回其他模型')
    return data


def save_defaults(base, value):
    atomic(Path(base) / 'model-preferences.json', {**pinned(value), 'at': time.time(),
                                                'source': 'local_page_explicit_choice'})
