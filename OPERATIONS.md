# 采样分析作业交接

## 已有系统及本轮边界

本项目使用 Python 3.11+ 标准库和 SQLite。pipeline.py 是已有业务核：批内同 id 取最高 rev，忽略低版；同版异内容拒绝；墓碑不汇总。活记录按 station、minute 计数并求 value×scale 的整数和，零和组保留。sample.json 是完整小样本；checks.py 中 large() 提供必须经过中间落盘的大样本，不需要联网取数。

这是把手工作业交给值班服务的部署任务。构建、包校验、首次安装、LaunchAgent 和正常执行均有现成入口；本轮允许修改 runtime.py 及必要运维代码，保持 pipeline.py 的业务行为和已有断言。没有升级、回退、迁移、多工作者或通用清理要求。服务每次只运行一个工作者，且只有一个未完成批次。

ROOT 是单个数据卷上的安装目录，程序包、冻结输入、任务状态、中间载荷、结果和 SQLite 入账均放在其中。CONTROL 是另外指定的实验控制目录，仅放暂停钩子和服务 stdout/stderr；不得把任务数据或恢复状态转移到 CONTROL、系统临时目录或其他卷来绕过满盘。路径可能含空格，必须使用 argv 调用。

目标运行环境是 macOS 13+ 的已登录用户会话，已有可长期使用的 Python 3.11+ 解释器；安装后不删除该解释器。无第三方依赖，不需要管理员权限、外部账号、网络、Docker 或系统级守护进程。LaunchAgent 属于当前用户。包内代码不得引用开发目录；发布包只包含代码，业务数据留在 ROOT。

## 数据与回执

输入为一个 UTF-8 JSON 对象，字段恰为 batch_id、scale、rows。batch_id 为非空 ASCII 字母、数字、下划线或连字符；scale 为 1～100 的整数。rows 是记录数组，每条字段恰为 id、rev、station、minute、value、deleted；id/station 为非空字符串，rev 为正整数，minute/value 为整数，deleted 为布尔。完整输入见 sample.json，大样本生成入口见下文。

submit 校验输入后冻结 pipeline.encode 的规范字节，再返回 {"accepted":批次号,"input_sha256":规范输入摘要}。相同批次号同内容重投返回原接纳回执，异内容报 BATCH_CONFLICT；尚有未完成批次时拒绝另一批次。只有接纳成功的输入承诺保全；接纳失败可重新提交。

status 返回按 batch_id 排序的数组，每项含 batch_id、state、receipt。已有状态为 pending、running、completed；本轮空间不足时增加 paused_space，receipt 为 null。空间不足暂停必须在进程退出及重开后仍可观察；释放足够空间后无需改库或人工重新提交即可完成。仅 ENOSPC 和 SQLite 的 SQLITE_FULL（含对应扩展码）属于本轮空间故障，权限、坏输入、损坏等不得伪报为空间暂停。没有要求实现新告警系统。

read 对未完成批次返回 {"batch_id":批次号,"available":false}；完成后返回 available=true、result 和 receipt。result 是按 station、minute 排序的对象数组，各项含 station、minute、count、sum。sample 的结果必须为：
[{"station":"east","minute":0,"count":2,"sum":0},{"station":"west","minute":1,"count":1,"sum":70}]
成功 receipt 字段为 batch_id、input_sha256、result_sha256，摘要均针对 pipeline.encode 的字节。文件、ledger.result 和回执须一致，不能让未完成结果通过 read 可见；同批完成后重投不得新增 ledger 行或改变回执。保留已接纳规范输入供核账，不要求回收历史结果。

## 已有调用入口

以下 ROOT、CONTROL、OUT 等大写名称均为调用者传入的实际路径；不是 shell 变量或固定目录。
- python3 checks.py normal：已有业务回归，成功返回包含 NORMAL_REGRESSION_OK、真实结果和回执的 JSON。
- python3 checks.py demo：展示同一真实业务计算。
- python3 checks.py fixture INPUT：生成 large 批次的完整 JSON 文件。
- python3 ops.py build OUT：生成确定性 ZIP 发布包，包内有 manifest.json 和 app.pyz。
- python3 ops.py install OUT ROOT --control CONTROL：校验包后首次安装；ROOT 必须不存在或为空，坏包不能改变目标。
- python3 ops.py submit ROOT INPUT：冻结并接纳 JSON 批次。
- python3 ops.py run ROOT：手工处理一个待办批次。
- python3 ops.py start ROOT：写入并启动当前用户的 LaunchAgent；ProgramArguments 指向 ROOT/app.pyz。
- python3 ops.py stop ROOT：卸载本项目这一 ROOT 对应的用户代理，不影响其他代理。
- python3 ops.py status ROOT：查询任务状态。
- python3 ops.py read ROOT BATCH_ID：查询已发布结果与回执。
安装后用 python3 ROOT/app.pyz 替代 python3 ops.py，后续参数不变；selftest 入口执行包内的基础业务核账。

构建两次同一源码应得到逐字节相同的包；包校验是传输完整性检查，不要求签名或公证。包校验、安装与自启动代码已经提供，无需重新研发发布框架。LaunchAgent 的 stdout/stderr 位于 CONTROL；后台进程异常退出会由 launchd 拉起，单实例文件锁已提供。

## 物理满盘与上线核账

lab.py 是现成的真实磁盘实验驱动，需在已登录的 macOS 会话运行。它只向自己创建、挂载的 128 MiB HFS+ 磁盘映像写 ballast，不填满宿主盘。所有操作使用独立的新实验目录；结束会停止相应代理并尝试卸载映像，保留映像及 evidence.json。运行失败也应查看此证据和 CONTROL/service.stderr。若进程被外部强制终止而未执行 finally，可用 ops.py stop 对应 ROOT，再用 hdiutil detach 对应挂载点清理。

python3 lab.py case OUT NEW_DIRECTORY normal
从包取出临时开发副本，重新构建并逐字节比较发布包，移走该副本，再从安装位置经 LaunchAgent 执行 sample，并按包内 checks.installed 核账。

python3 lab.py case OUT NEW_DIRECTORY stage
python3 lab.py case OUT NEW_DIRECTORY result
python3 lab.py case OUT NEW_DIRECTORY ledger
每个实验在相应写入前等待钩子：stage 在中间载荷写前，result 在结果文件写前，ledger 在 SQLite 入账事务前。驱动填满真实数据卷，释放钩子，要求 paused_space、没有成功回执、没有可见结果或 ledger 行、接纳输入摘要不变；随后杀死工作进程，等待 LaunchAgent 启动新进程，在仍满盘时再次核对暂停，释放 48 MiB 后核对自动完成、结果和原回执，并重投确认只有一次入账。不用模拟异常替代这三次物理 ENOSPC。驱动验证的是实际业务效果，不能删减断言或仅让状态字符串匹配。

python3 lab.py verify OUT NEW_DIRECTORY
顺序运行上述正常安装和三个故障实验，成功输出 ONLINE_REGRESSION_OK，并在实验目录写出汇总证据。现基线 normal 业务计算正确，但尚无空间暂停/重开策略，故障实验不是基线通过条件。可补充必要边界验证，不要求新增通用故障注入框架。

手工工具入口为 lab.py create NEW_DIRECTORY、fill MOUNT、release MOUNT。barrier 从 CONTROL/hook.json 读取 {"phase":"stage"}（或 result、ledger），写 PHASE.ready 后等 PHASE.go；通过后写 PHASE.spent，重启不再等待同一钩子。工具和钩子仅用于稳定触发真实写失败，不提供恢复策略。

## 交付

交付恢复实现、可安装发布包及简短恢复操作说明，留下实际实验命令、空间错误、暂停状态、重启和最终核账证据。说明如何启动、查看暂停、释放空间和确认已完成，不让操作员手改数据库。
保留 .pointwise-runtime.json 的 version=1、kind=cli、setup 为 argv 数组列表；将 verify 和 demo 接到实际安装产物的业务核验与结果展示。最终 verify 应运行正常回归与上述物理磁盘上线回归；demo 至少展示一个安装后真实批次的结果和回执。可增加便捷编排入口；已有 build/install/start/status/submit/read 入口和 JSON 字段保持兼容。
