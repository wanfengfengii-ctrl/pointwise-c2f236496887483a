# 满盘恢复操作说明

本服务在数据卷写满（ENOSPC）或 SQLite 报 SQLITE_FULL 时，把当前批次持久
标记为 `paused_space` 并自动等待；释放空间后无需任何人工操作即可续跑完
成。恢复状态只保存在 ROOT 数据卷内（state.sqlite 与 work/.reserve），不
写入 CONTROL 或其他卷。操作员全程不需要、也不允许手改数据库。

## 原理（了解即可，不需要操作）

- ROOT/work/.reserve 常驻 4 MiB 预留文件。磁盘写满时服务先删除它腾出记
  账空间，清理半成品临时文件，再把 paused_space 写入 state.sqlite。
- 中间载荷与结果文件按“临时文件 + fsync + 原子改名”写入；入账是单个
  SQLite 事务。半成品不会出现在正式路径，read 在未完成前一律返回
  available=false，也不会产生回执或 ledger 行。
- 只有 ENOSPC 与 SQLITE_FULL 会被判为空间不足；权限、损坏、坏输入等故
  障不会伪报为 paused_space，按原故障处理即可。

## 启动服务

```
python3 ops.py install RELEASE.zip ROOT --control CONTROL
python3 ops.py start ROOT        # 写入并启动当前用户的 LaunchAgent
python3 ops.py submit ROOT INPUT # 接纳批次（可重复提交同一批次，幂等）
```

## 查看是否暂停

```
python3 ROOT/app.pyz status ROOT
```

看到 `"state": "paused_space"` 且 `"receipt": null` 即为空间不足暂停。
此时 `read` 返回 `available: false`，属正常现象，不是数据丢失。

## 释放空间

在 ROOT 所在数据卷上删除无关文件或扩容，**不要**删除 ROOT 内的
inputs/、work/、results/、state.sqlite、app.pyz。不需要重启服务、不需
要重新提交批次、不需要改库。工作者每约 2 秒自动重试，空间足够后自行
完成。若暂停期间进程被杀（如机器重启），LaunchAgent 会自动拉起，磁盘
仍满时状态保持 paused_space，不会丢批次。

## 确认已完成

```
python3 ROOT/app.pyz status ROOT          # state 应为 completed，receipt 非空
python3 ROOT/app.pyz read ROOT BATCH_ID   # available=true，含 result 与 receipt
python3 ops.py submit ROOT INPUT          # 同批同内容重投返回原接纳回执
```

重投同一批次只会返回原回执，不会新增 ledger 行；已接纳输入一直保存在
ROOT/inputs/ 供核账。

## 禁止事项

- 不要手工编辑或删除 state.sqlite；所有恢复由服务自动完成。
- 不要把任务数据或恢复状态挪到 CONTROL、系统临时目录或其他卷来“绕过”
  满盘；CONTROL 只放暂停钩子与服务 stdout/stderr。
- 磁盘仍满时不要强删 ROOT 内文件“腾地方”，那会破坏已接纳批次。
