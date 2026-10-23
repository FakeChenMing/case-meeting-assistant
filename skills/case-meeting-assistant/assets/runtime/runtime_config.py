"""Portable paths only. No credentials or global Codex configuration writes."""
import json, os, shutil
from pathlib import Path

CONFIG_FILE=Path(os.environ.get('CASE_ASSISTANT_CONFIG',Path(__file__).with_name('installation.json'))).expanduser()
CONFIG=json.loads(CONFIG_FILE.read_text()) if CONFIG_FILE.is_file() else {}
BASE=Path(CONFIG.get('base',Path.home()/'Library/Application Support/CaseMeetingAssistantCommunity')).expanduser()
TEMPLATES=Path(CONFIG.get('templates',BASE/'templates')).expanduser()
APP=Path(CONFIG.get('capture_app',Path.home()/'Applications/案件沟通采集-公开版.app')).expanduser()
PORT=int(CONFIG.get('port',34971))
if not 1024<=PORT<=65535: raise ValueError('本地端口必须介于1024与65535之间')
SKILL_NAME='case-meeting-assistant'
SKILL_HOME=Path(os.environ.get('CODEX_HOME',Path.home()/'.codex'))/'skills'

def codex_cli():
    explicit=CONFIG.get('codex_cli') or os.environ.get('CASE_ASSISTANT_CODEX')
    candidates=[explicit] if explicit else [
        shutil.which('codex'),
        '/Applications/Codex.app/Contents/Resources/codex',
        '/Applications/Codex.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex',
        '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex',
    ]
    for p in candidates:
        if p and Path(p).is_file() and os.access(p,os.X_OK): return str(Path(p).resolve())
    raise RuntimeError('未找到Codex App Server。请安装官方Codex并登录，再在本安装配置中指定codex_cli；不回退到API Key。')
