"""Launch this installation only. A running server is reused, never replaced."""
from pathlib import Path
import json, os, subprocess, sys, time, urllib.request, webbrowser
from runtime_config import BASE,PORT

url=f'http://127.0.0.1:{PORT}/'
def alive():
    try:
        with urllib.request.urlopen(url,timeout=2) as r:
            return '案件沟通实时辅助' in r.read().decode()
    except OSError:return False

if not alive():
    BASE.mkdir(parents=True,exist_ok=True)
    log=(BASE/'server.log').open('ab')
    subprocess.Popen([sys.executable,str(Path(__file__).with_name('server.py'))],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    for _ in range(60):
        if alive():break
        time.sleep(.2)
    else:raise SystemExit(f'启动失败，请检查 {BASE / "server.log"}。未开始录音。')
if '--no-browser' not in sys.argv:webbrowser.open(url)
print(url)
