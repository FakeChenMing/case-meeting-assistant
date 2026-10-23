import unittest,tempfile,ast,json,hashlib
from pathlib import Path
from types import SimpleNamespace
from ledger import Store

class BackgroundTests(unittest.TestCase):
 def test_changed_added_deleted_and_same_length_materials(self):
  # Execute the actual method in isolation, without starting the HTTP/capture service.
  tree=ast.parse((Path(__file__).resolve().parents[1]/'skills/case-meeting-assistant/assets/runtime/server.py').read_text());cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Engine');method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='context_note')
  root=Path(tempfile.mkdtemp(prefix='case-background-'));case=root/'case';case.mkdir();templates=root/'templates';templates.mkdir();s=Store(root/'state.sqlite')
  s.put('case',str(case));s.put('pack','pack.md');(case/'pack.md').write_text('交接v1');(case/'source.txt').write_text('金额125000');(templates/'00-rules.md').write_text('规则')
  scope={'Path':Path,'TEMPLATES':templates,'SKILL_HOME':root/'skills','hashlib':hashlib,'json':json};exec(compile(ast.Module(body=[method],type_ignores=[]),'actual_context_note','exec'),scope)
  obj=SimpleNamespace(store=s,sid='test');fn=scope['context_note']
  first=fn(obj,'first');self.assertIn(str(case/'source.txt'),first);s.put('background_snapshot',s.get('context:first'))
  self.assertIn('哈希检查不等于模型已读',fn(obj,'same'))
  (case/'source.txt').write_text('金额120000');self.assertIn(str(case/'source.txt'),fn(obj,'replace'));s.put('background_snapshot',s.get('context:replace'))
  (case/'new.txt').write_text('新增材料');self.assertIn(str(case/'new.txt'),fn(obj,'added'));s.put('background_snapshot',s.get('context:added'))
  (case/'new.txt').unlink();self.assertIn(str(case/'new.txt'),fn(obj,'deleted'));s.put('background_snapshot',s.get('context:deleted'))
  own=case/'沟通记录-GPT/test';own.mkdir(parents=True);(own/'output.md').write_text('自己的输出不反复触发背景读取')
  self.assertIn('哈希检查不等于模型已读',fn(obj,'own-output'));s.db.close()

if __name__=='__main__':unittest.main(verbosity=2)
