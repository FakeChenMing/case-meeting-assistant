#!/usr/bin/env python3
"""Install a separate, credential-free macOS preview from local source."""
import argparse, datetime, hashlib, json, os, platform, plistlib, shlex, shutil, subprocess, sys
from pathlib import Path

SKILL=Path(__file__).resolve().parents[1]
VERSION='0.3.0-preview'
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def validate_templates(path):
    for n in range(7):
        found=list(path.glob(f'{n:02d}-*.md'))
        if len(found)!=1 or not found[0].read_text().strip():raise ValueError(f'模板{n:02d}缺失、不唯一或不可读：{path}')
def install(args):
    base=args.base.expanduser().resolve();app=args.app_path.expanduser().resolve()
    if base==Path.home()/'Library/Application Support/CaseMeetingAssistant':
        raise ValueError('此安装器保留既有私人版；请使用独立目录，不覆盖旧运行程序。')
    if args.mode=='single' and args.role!='all':raise ValueError('单机模式的role应为all')
    if platform.system()!='Darwin' or int(platform.mac_ver()[0].split('.')[0])<26:
        raise ValueError('采集运行版要求macOS 26或更高版本；当前尚未验证Windows/Linux。')
    if platform.machine()!='arm64':raise ValueError('公开采集版当前仅支持已测试的Apple Silicon配置')
    sys.path.insert(0,str(SKILL/'assets/runtime'))
    from runtime_config import codex_cli
    cli=str(args.codex.expanduser().resolve()) if args.codex else codex_cli()
    if not Path(cli).is_file() or not os.access(cli,os.X_OK):raise ValueError('Codex可执行文件不存在')
    cli_version=subprocess.check_output([cli,'--version'],text=True).strip()
    templates=args.templates.expanduser().resolve() if args.templates else SKILL/'assets/templates'
    validate_templates(templates)
    xcrun=shutil.which('xcrun')
    if args.role!='desk' and not xcrun:raise ValueError('缺少Apple Command Line Tools；请安装后再继续')
    report={'version':VERSION,'mode':args.mode,'role':args.role,'base':str(base),'capture_app':str(app),'templates':str(templates),'codex_cli':cli,'codex_version':cli_version,'port':args.port,'authentication':'existing ChatGPT only; not yet checked','checks':'environment and templates only; not live acceptance'}
    if args.check:print(json.dumps(report,ensure_ascii=False,indent=2));return
    if not 1024<=args.port<=65535:raise ValueError('本地端口不在允许范围')
    # New-install only. An update should be reviewed and backed up, not blindly replace a live app.
    if (base/'app').exists() or app.exists():raise ValueError('目标程序已存在，未覆盖。先检查版本和运行状态，再制作备份并按升级说明处理。')
    if not args.stage_only:
        dest=Path(os.environ.get('CODEX_HOME',Path.home()/'.codex'))/'skills/case-meeting-assistant'
        if dest.resolve()!=SKILL and dest.exists():raise ValueError('同名技能已存在；先核对差异，不覆盖。')
        if args.role!='desk' and (args.desktop_dir.expanduser()/'启动案件沟通辅助-公开版.command').exists():raise ValueError('同名桌面入口已存在；先核对，不覆盖。')
    if 'Mobile Documents' in str(base) or 'CloudStorage' in str(base):raise ValueError('运行数据库不能安装到云同步目录')
    base.mkdir(parents=True,exist_ok=True);os.chmod(base,0o700)
    target=base/'app';shutil.copytree(SKILL/'assets/runtime',target,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    if not args.templates:
        shutil.copytree(SKILL/'assets/templates',base/'templates');report['templates']=str(base/'templates')
    if not (base/'demo-case').exists():shutil.copytree(SKILL/'assets/example-case',base/'demo-case')
    (target/'installation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    os.chmod(target/'installation.json',0o600)
    if args.role!='desk':
        contents=app/'Contents';(contents/'MacOS').mkdir(parents=True);(contents/'Resources').mkdir()
        info={'CFBundleDisplayName':'案件沟通采集（公开版）','CFBundleName':'案件沟通采集（公开版）','CFBundleExecutable':'CaseMeetingCapture','CFBundleIdentifier':'org.case-meeting-assistant.community.capture','CFBundlePackageType':'APPL','CFBundleShortVersionString':'0.3.0','CFBundleVersion':'1','LSMinimumSystemVersion':'26.0','NSHighResolutionCapable':True,'NSMicrophoneUsageDescription':'为您主动开始的沟通保存原音频并进行本地普通话转写。','NSSpeechRecognitionUsageDescription':'使用本机Apple Speech模型转写您主动开始的录音。','CaseAssistantDataDirectory':str(base)}
        (contents/'Info.plist').write_bytes(plistlib.dumps(info))
        shutil.copy2(SKILL/'assets/native/Wisp-LICENSE',contents/'Resources/Wisp-LICENSE')
        subprocess.run([xcrun,'swiftc',str(SKILL/'assets/native/Capture.swift'),'-O','-module-cache-path',str(base/'build-cache'),'-target','arm64-apple-macosx26.0','-o',str(contents/'MacOS/CaseMeetingCapture')],check=True)
        subprocess.run(['/usr/bin/codesign','--force','--sign','-',str(app)],check=True)
        subprocess.run(['/usr/bin/codesign','--verify','--strict',str(app)],check=True)
        if not args.stage_only:
            desktop=args.desktop_dir.expanduser();desktop.mkdir(parents=True,exist_ok=True)
            launcher=desktop/'启动案件沟通辅助-公开版.command'
            if launcher.exists():raise ValueError('桌面入口已存在，保留原文件；请人工核对')
            launcher.write_text('#!/bin/sh\nexec '+shlex.quote(sys.executable)+' '+shlex.quote(str(target/'launch.py'))+'\n');launcher.chmod(0o755)
            report['launcher']=str(launcher)
    # Skill-installers normally already put this skill in the target. Standalone source install also works.
    if not args.stage_only:
        dest=Path(os.environ.get('CODEX_HOME',Path.home()/'.codex'))/'skills/case-meeting-assistant'
        if dest.resolve()!=SKILL:
            if dest.exists():raise ValueError('同名技能已存在，保留它；请先核对差异再合并')
            shutil.copytree(SKILL,dest,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    report['installed_at']=datetime.datetime.now().astimezone().isoformat()
    report['runtime_sha256']={p.name:digest(p) for p in target.iterdir() if p.is_file()}
    report['native_built']=args.role!='desk';report['microphone_started']=False
    (base/'installation-manifest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser(description='独立安装；不复制凭据、不覆盖既有版本、不启动录音。')
    p.add_argument('--mode',choices=['single','dual'],default='single')
    p.add_argument('--role',choices=['all','field','desk'],default='all')
    p.add_argument('--base',type=Path,default=Path.home()/'Library/Application Support/CaseMeetingAssistantCommunity')
    p.add_argument('--app-path',type=Path,default=Path.home()/'Applications/案件沟通采集-公开版.app')
    p.add_argument('--templates',type=Path);p.add_argument('--codex',type=Path)
    p.add_argument('--port',type=int,default=34971)
    p.add_argument('--desktop-dir',type=Path,default=Path.home()/'Desktop')
    p.add_argument('--check',action='store_true');p.add_argument('--stage-only',action='store_true',help='构建到指定测试目录，不登记技能或创建桌面入口')
    try:install(p.parse_args())
    except (ValueError,OSError,subprocess.CalledProcessError) as e:p.exit(1,str(e)+'\n')
