"""Versioned local ledgers. Raw capture events and user actions are never rewritten."""
import json, time, re, uuid, difflib
from pathlib import Path
from store import Store as RawStore, atomic


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class Store(RawStore):
    def __init__(self, path):
        super().__init__(path)
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS corrections(
          id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT UNIQUE, recording TEXT,
          segment TEXT, raw_seq INTEGER, revision INTEGER, old_text TEXT,
          text TEXT, at REAL, source TEXT, UNIQUE(recording,segment,revision));
        CREATE TABLE IF NOT EXISTS qa(
          id TEXT PRIMARY KEY, text TEXT, intent TEXT, created REAL,
          state TEXT, answer TEXT, sources TEXT, answered REAL, revision INTEGER);
        CREATE TABLE IF NOT EXISTS qa_versions(
          n INTEGER PRIMARY KEY, qid TEXT, answer TEXT, sources TEXT, answered REAL,
          revision INTEGER, replaced_by TEXT);
        CREATE TABLE IF NOT EXISTS question_versions(
          n INTEGER PRIMARY KEY, qid TEXT, body TEXT, batch TEXT, at REAL);
        CREATE TABLE IF NOT EXISTS aliases(alias TEXT PRIMARY KEY, qid TEXT);
        CREATE TABLE IF NOT EXISTS facts(
          id TEXT PRIMARY KEY, body TEXT, batch TEXT, revision INTEGER, validity TEXT);
        CREATE TABLE IF NOT EXISTS result_status(
          batch TEXT PRIMARY KEY, revision INTEGER, validity TEXT, reason TEXT);
        CREATE TABLE IF NOT EXISTS telemetry(
          key TEXT PRIMARY KEY, kind TEXT, at REAL, body TEXT);
        CREATE TABLE IF NOT EXISTS proposals(
          id TEXT PRIMARY KEY, qid TEXT, body TEXT, reason TEXT, state TEXT, at REAL);
        ''')
        self.db.execute("INSERT OR IGNORE INTO aliases SELECT issue_key,id FROM questions")
        self.db.commit()

    def revisions(self, after=0):
        with self.lock:
            return [dict(r) for r in self.db.execute('SELECT * FROM corrections WHERE id>? ORDER BY id', (after,))]

    def revision(self):
        with self.lock:
            return self.db.execute('SELECT coalesce(max(id),0) FROM corrections').fetchone()[0]

    def raw_segments(self):
        return super().current_segments()

    def current_segments(self):
        corrections = {(r['recording'], r['segment']): r for r in self.revisions()}
        rows = self.raw_segments()
        for e in rows:
            r = corrections.get((e['recording_id'], e['segment_id']))
            e['raw_text'] = e['text']
            e['correction_revision'] = r['revision'] if r else 0
            if r:
                e.update(text=r['text'], correction_id=r['id'], correction_at=r['at'],
                         correction_source=r['source'], correction_pending=r['id'] > self.get('correction_processed', 0))
        return rows

    def correct(self, recording, segment, text, expected_revision, op):
        if not isinstance(text, str) or not text.strip() or len(text) > 12000:
            raise ValueError('更正文字须为1至12000字；不能用空文字删除原话')
        with self.lock, self.db:
            oldop = self.db.execute('SELECT * FROM corrections WHERE op=?', (op,)).fetchone()
            if oldop:
                if (oldop['recording'],oldop['segment'],oldop['text']) != (recording,segment,text):
                    raise ValueError('同一操作编号的内容不一致')
                return oldop['id']
            e = next((e for e in self.current_segments() if e['recording_id']==recording and e['segment_id']==segment), None)
            if not e or not e.get('is_final'):
                raise ValueError('请选择已稳定的转写段；临时识别仍在变化')
            if e['correction_revision'] != expected_revision:
                raise ValueError('该段已被更正，请刷新后核对最新版本再提交')
            if e['text'] == text:
                raise ValueError('文字没有变化')
            row = self.db.execute('INSERT INTO corrections(op,recording,segment,raw_seq,revision,old_text,text,at,source) VALUES(?,?,?,?,?,?,?,?,?)',
                 (op,recording,segment,e['seq'],expected_revision+1,e['text'],text,time.time(),'本地纠错操作；非当事人改变说法。虚构验收操作须结合测试记录识别操作者。'))
            cid = row.lastrowid
            # Missing granular dependencies must never leave an old judgment looking current.
            # Conservatively mark all earlier analysis pending, with this reason visible.
            self.db.execute("UPDATE result_status SET validity='待更新',reason=?", (f'转写纠错C{cid:06d}；保守复核既往分析',))
            self.db.execute("UPDATE facts SET validity='待更新'")
            for q in self.questions():
                q['body']['_validation'] = '转写已更正，分析待更新'
                self.db.execute('UPDATE questions SET body=? WHERE id=?', (encoded(q['body']),q['id']))
            self.db.execute("UPDATE qa SET state='已回答·依据待复核' WHERE state LIKE '已回答%'")
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', ('latest_result_stale', 'true'))
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', ('synthesis_needed', 'true'))
            return cid

    def add_question(self, bid, text, intent='question'):
        if intent not in ('question','decision'):
            raise ValueError('提问类型无效')
        with self.lock,self.db:
            self.db.execute('INSERT INTO qa VALUES(?,?,?,?,?,?,?,?,?)',
                (bid,text,intent,time.time(),'排队中','', '[]',None,self.revision()))

    def qa(self):
        with self.lock:
            return [{**dict(r),'sources':json.loads(r['sources'])} for r in self.db.execute('SELECT * FROM qa ORDER BY created')]

    def qa_status(self, bid, state):
        with self.lock,self.db:
            self.db.execute('UPDATE qa SET state=? WHERE id=? AND answered IS NULL',(state,bid))

    def trace(self, key, kind, **body):
        with self.lock,self.db:
            self.db.execute('INSERT OR IGNORE INTO telemetry VALUES(?,?,?,?)',(key,kind,time.time(),encoded(body)))

    @staticmethod
    def score(q):
        b=q['body']
        return min(3,max(0,int(b.get('urgency',0))))*100 + (30 if b.get('key_change') is True else 0) + (10 if b.get('new_source') is True else 0)

    def visible(self):
        with self.lock:
            qs=[q for q in self.questions() if q['state']=='候选' and not q['body'].get('_validation')]
            prior=self.get('visible_ids',[])
            selected=sorted(qs,key=lambda q:(-self.score(q), -q['body'].get('_priority_at',q['created']) if self.score(q)>=200 else 0, 0 if q['id'] in prior else 1, q['created']))[:5]
            ids={q['id'] for q in selected}
            order=[x for x in prior if x in ids]+[q['id'] for q in selected if q['id'] not in prior]
            if order != prior:
                newcomers=[q for q in selected if q['id'] not in prior and self.score(q)>=200]
                if prior and newcomers:
                    self.put('priority_notice',{'at':time.time(),'text':'重要新问题进入主区：'+','.join(q['id'] for q in newcomers)+'；原问题仍保留在候选池。'})
                self.put('visible_ids',order)
            byid={q['id']:q for q in selected}
            return [byid[i] for i in order]

    def proposal_rows(self):
        with self.lock:
            return [{**dict(r),'body':json.loads(r['body'])} for r in self.db.execute("SELECT * FROM proposals WHERE state='待确认' ORDER BY at")]

    def resolve_proposal(self, pid, allow):
        with self.lock,self.db:
            r=self.db.execute('SELECT * FROM proposals WHERE id=?',(pid,)).fetchone()
            if not r or r['state']!='待确认':return
            self.db.execute('UPDATE proposals SET state=? WHERE id=?',('用户允许' if allow else '用户拒绝',pid))
            if allow:
                if r['qid']:
                    self.action(r['qid'],'候选','proposal-'+pid,'用户审阅具体新证据后明确恢复')
                else:
                    q=json.loads(r['body']);self._insert_question(q,'proposal-'+pid,self.revision())

    def _insert_question(self,q,batch,revision):
        num=self.db.execute('SELECT count(*) FROM questions').fetchone()[0]+1
        qid=f'Q{num:03d}';q['id']=qid;q['_batch']=batch;q['_revision']=revision;q.pop('_validation',None)
        self.db.execute('INSERT INTO questions VALUES(?,?,?,?,?,?)',(qid,q['issue_key'],encoded(q),'候选',1,time.time()))
        self.db.execute('INSERT OR IGNORE INTO aliases VALUES(?,?)',(q['issue_key'],qid))
        return qid

    def _identity(self,q):
        alias=self.db.execute('SELECT qid FROM aliases WHERE alias=?',(q['issue_key'],)).fetchone()
        if alias:return next(x for x in self.questions() if x['id']==alias[0]),None
        match=q.get('same_as')
        if match:
            old=next((x for x in self.questions() if match in (x['id'],x['issue_key'])),None)
            if not old:raise ValueError('问题同一性引用不存在')
            return old,None
        slot=q.get('fact_slot','')
        if slot:
            old=next((x for x in self.questions() if x['body'].get('fact_slot')==slot),None)
            if old:return old,None
        normalized=lambda t:re.sub(r'[^\w]','',t).lower()
        for old in self.questions():
            # Explicitly different fact slots must not collapse merely because
            # their wording differs only in a date, party, or transaction name.
            if slot and old['body'].get('fact_slot') and slot!=old['body']['fact_slot']:
                continue
            if difflib.SequenceMatcher(None,normalized(q['question']),normalized(old['body']['question'])).ratio()>.78:
                return old,None
        handled=[x for x in self.questions() if x['state']!='候选']
        # No file-overlap shortcut: distinct gaps can cite the same material.
        # Missing identity evidence is held outside the main area, not silently allowed.
        distinctions=q.get('distinct_from',{})
        missing=[x['id'] for x in handled if not isinstance(distinctions,dict) or not distinctions.get(x['id'])]
        if missing:return None,'新问题尚未说明与已处理问题 '+','.join(missing)+' 的具体差别；请核对事实缺口。'
        return None,None

    def merge(self,batch,result,processed):
        if result.get('batch_id')!=batch:raise ValueError('分析批次ID不一致')
        qs=result.get('questions',[])
        if not isinstance(qs,list) or len(qs)>30:raise ValueError('问题输出格式无效')
        revision=self.get('rev:'+batch,0)
        stale=revision<self.revision()
        with self.lock,self.db:
            if self.db.execute('SELECT 1 FROM results WHERE batch=?',(batch,)).fetchone():return
            self.db.execute('INSERT INTO results VALUES(?,?,?)',(batch,encoded(result),time.time()))
            self.db.execute('INSERT INTO result_status VALUES(?,?,?,?)',(batch,revision,'待更新' if stale else '当前','返回时已有较新人工纠错' if stale else ''))
            if not stale:
                for q in qs:
                    for k in ('issue_key','question','reason','impact','source'):
                        if not isinstance(q.get(k),str) or not q[k].strip():raise ValueError('问题字段缺失 '+k)
                    # Reject nonnumeric priority, rather than letting page status fail later.
                    q['urgency']=min(3,max(0,int(q.get('urgency',0))))
                    old,reason=self._identity(q)
                    if reason:
                        pid='identity-'+q['issue_key']
                        self.db.execute('INSERT OR IGNORE INTO proposals VALUES(?,?,?,?,?,?)',(pid,None,encoded(q),reason,'待确认',time.time()));continue
                    if old:
                        qid=old['id'];q['id']=qid;q['_batch']=batch;q['_revision']=revision;q.pop('_validation',None)
                        q['_priority_at']=time.time() if (q.get('key_change') or q.get('new_source')) and q['source']!=old['body']['source'] else old['body'].get('_priority_at',old['created'])
                        self.db.execute('INSERT INTO question_versions(qid,body,batch,at) VALUES(?,?,?,?)',(qid,encoded(old['body']),batch,time.time()))
                        self.db.execute('INSERT OR IGNORE INTO aliases VALUES(?,?)',(q['issue_key'],qid))
                        q['issue_key']=old['issue_key']
                        self.db.execute('UPDATE questions SET body=? WHERE id=?',(encoded(q),qid))
                        evidence=q.get('recovery_evidence');why=q.get('recovery_reason')
                        if old['state']!='候选' and evidence and why:
                            pid='recovery-'+qid+'-'+str(evidence)
                            self.db.execute('INSERT OR IGNORE INTO proposals VALUES(?,?,?,?,?,?)',(pid,qid,encoded(q),str(why)+'；新来源：'+str(evidence),'待确认',time.time()))
                    else:self._insert_question(q,batch,revision)
                for review in result.get('reviewed_questions',[]):
                    old=self.db.execute('SELECT * FROM questions WHERE id=?',(review.get('id'),)).fetchone()
                    if not old:raise ValueError('纠错复核引用了不存在的问题')
                    q=json.loads(old['body'])
                    if not review.get('reason') or not review.get('source'):raise ValueError('复核问题缺少原因或来源')
                    if review.get('still_needed') is True:
                        if all(isinstance(review.get(k),str) and review[k].strip() for k in ('question','reason','impact','source')):
                            self.db.execute('INSERT INTO question_versions(qid,body,batch,at) VALUES(?,?,?,?)',(old['id'],encoded(q),batch,time.time()))
                            for k in ('question','reason','impact','source'):q[k]=review[k]
                            q['_batch']=batch
                            q.pop('_validation',None)
                        else:
                            q['_validation']='纠错复核未给出完整更新问法、原因、影响和来源；待补齐'
                    elif review.get('still_needed') is False:q['_validation']='纠错后暂不适用；保留历史，不等于用户已解决'
                    else:raise ValueError('纠错问题复核状态无效')
                    q['_review']=review;q['_revision']=revision
                    self.db.execute('UPDATE questions SET body=? WHERE id=?',(encoded(q),old['id']))
                for update in result.get('answer_updates',[]):
                    old=self.db.execute('SELECT * FROM qa WHERE id=?',(update.get('id'),)).fetchone()
                    if not old or old['answered'] is None:continue
                    if not isinstance(update.get('answer'),str) or not update.get('sources'):raise ValueError('回答复核缺少答案或来源')
                    self.db.execute('INSERT INTO qa_versions(qid,answer,sources,answered,revision,replaced_by) VALUES(?,?,?,?,?,?)',(old['id'],old['answer'],old['sources'],old['answered'],old['revision'],batch))
                    self.db.execute('UPDATE qa SET answer=?,sources=?,answered=?,revision=?,state=? WHERE id=?',(update['answer'],encoded(update['sources']),time.time(),revision,'已回答·纠错后复核',old['id']))
                for i,f in enumerate(result.get('fact_changes',[])):
                    fid=f.get('fact_key') or batch+':'+str(i)
                    self.db.execute('INSERT OR REPLACE INTO facts VALUES(?,?,?,?,?)',(fid,encoded(f),batch,revision,'待核陈述/分析'))
                self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('latest_result',encoded(result)))
                self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('latest_result_stale','false'))
                if batch.startswith('TRANSCRIPT-') or batch.startswith('CORRECTION-'):
                    self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('correction_processed',encoded(revision)))
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('processed',encoded(processed)))
            if batch.startswith('USER-'):
                self.db.execute('UPDATE qa SET answer=?,sources=?,answered=?,state=? WHERE id=?',
                    (result.get('answer',''),encoded(result.get('answer_sources',[])),time.time(),
                     '已回答·依据待复核' if stale else '已回答',batch))

    def export(self,directory):
        with self.lock:
            super().export(directory)
            dest=Path(directory)
            atomic(dest/'转写人工纠错.jsonl',''.join(encoded(r)+'\n' for r in self.revisions()))
            atomic(dest/'问答历史.json',self.qa())
            atomic(dest/'问答旧版本.json',[dict(r) for r in self.db.execute('SELECT * FROM qa_versions ORDER BY n')])
            atomic(dest/'事实台账.json',[{**dict(r),'body':json.loads(r['body'])} for r in self.db.execute('SELECT * FROM facts')])
            atomic(dest/'分析有效性.json',[dict(r) for r in self.db.execute('SELECT * FROM result_status')])
            atomic(dest/'恢复建议.json',self.proposal_rows())
            atomic(dest/'时序记录.jsonl',''.join(encoded({**dict(r),'body':json.loads(r['body'])})+'\n' for r in self.db.execute('SELECT * FROM telemetry ORDER BY at')))


def recover_ranges(store,session,jobs):
    received=max([e['seq'] for e in store.events()]+[0])
    for j in jobs:
        m=re.fullmatch(r'(?:TRANSCRIPT|CORRECTION)-'+re.escape(session)+r'-(\d+)(?:-C(\d+))?',j['id'])
        if not m:continue
        end=int(m[1]);rev=int(m[2] or 0)
        if end>received or rev>store.revision():raise ValueError('队列位置超过已保存材料，拒绝跳过')
        store.put('range:'+j['id'],end);store.put('rev:'+j['id'],rev)
        store.put('submitted',max(store.get('submitted',0),end))
