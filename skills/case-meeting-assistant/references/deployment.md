# 部署与回退

## 新用户

在Codex中安装本仓库`skills/case-meeting-assistant`目录，或下载仓库后运行该目录下的安装器。不要把整个仓库当作用户数据同步目录。

```sh
python3 scripts/install.py --mode single --check
python3 scripts/install.py --mode single
```

以上命令在技能目录执行，由Codex协助完成亦可。编译完成后桌面出现“启动案件沟通辅助-公开版.command”。双击打开本地页面，先虚构测试，再根据验收范围决定可用场景。首次麦克风及语音权限由本人处理；不要关闭Gatekeeper、SIP或降低审批。

双机现场端用`--mode dual --role field`，准备端用`--mode dual --role desk`。准备端不编译采集器、不创建录音启动入口；它使用技能、模板和自己的Codex登录处理文件。两端如已有同一套现行模板，用`--templates <本机目录>`引用，不覆盖、不另建互相矛盾的规范。Windows/Linux仅可参考文件流程，采集程序不支持。

## 配置与数据

- 默认本地运行目录：`~/Library/Application Support/CaseMeetingAssistantCommunity`。
- 默认本地页面：`http://127.0.0.1:34971/`；不会对公网监听。
- 独立应用：`~/Applications/案件沟通采集-公开版.app`。不覆盖官方Wisp及私人验收版。
- Codex路径自动发现，也可用`--codex <可执行文件>`指定。必须支持本仓库使用的App Server协议；兼容性需本机验证。
- 仅接受现有ChatGPT登录，不读取、复制或公开认证文件，不自动用API Key。模型分析会消耗该账号可用额度；实时请求频率和模型档位影响使用量。本地语音下载如被系统请求，应由用户确认。
- 模型/推理在页面刷新目录并显式应用；目录列出不等于全部模型已实测。已经排队的回合保留原设置。ultra档位未开放。
- 案件产物：`沟通记录-GPT/<session>/`；SQLite、队列锁、控制文件不放同步盘。
- 公开包不附带法律技能全集和商业MCP账号。根据本机来源安装匹配技能；没有经实际核验的工具，只给事实整理与待核提示。

## 升级与回退

安装器拒绝覆盖已存在程序。先结束录音，核对尾段和队列；按安装清单备份程序、模板、配置和数据库。更新源代码后还要核对实际运行副本摘要。回退只恢复程序文件，不能把旧数据库覆盖到新增会话上。原始音频、操作历史及交接版本始终保留。若数据结构有变化，先验证兼容性，失败时保留两份数据并停止写入。

卸载时停止对应本地服务和独立采集程序，移走该公开版入口/程序即可；不默认删除sessions、capture或案件目录。不改官方Wisp，不更改全局Codex配置。

## 自检与演示

`python3 scripts/demo.py`启动完全独立的虚构页面演示，默认34972端口；不打开麦克风、不连接Codex、不花模型额度。预置问答和转写明确标为脚本示例，只验证界面/状态，不证明语音质量或AI推理。

在仓库根目录：`PYTHONPATH=skills/case-meeting-assistant/assets/runtime python3 -m unittest discover -s tests -p 'test_*.py'`。JS界面逻辑测试需要Node；PDF不依赖它才能阅读。
