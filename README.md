# 微震监测 SQLite 数据仓库

把采集库的变更日志（提交信封 JSONL）按源端提交顺序同步到独立 SQLite 仓库，
支持值班员按**测点 + 任意观测时段**查询观测数、以纳米计的峰值振幅及对应
观测编号（峰值并列取最小编号）。纯 Python 标准库实现。

## 一、日志格式（线上格式）

每个源端提交 = 一个 UTF-8 JSONL 文件，文件名 `<10 位补零 seq>.jsonl`：

```jsonl
{"type":"begin","seq":12,"committed_at":1732500000000000000}
{"type":"obs","off":0,"obs_id":1001,"time":1732500000000000100,"channel":"C1","amp_value":"3.75","amp_unit":"um","op":"upsert"}
{"type":"retract","off":1,"obs_id":1001}
{"type":"wire","off":2,"channel":"C1","valid_from":1732500000000000000,"valid_to":null,"point":"P9","op":"upsert"}
{"type":"end","seq":12,"events":3,"checksum":"sha256:<所有事件行字节的 sha256>"}
```

- `seq`：源端单调提交序号；`off`：提交内 0 起连续顺序号。变更严格按
  `(seq, off)` 全序生效，乱序到达只推进连续前缀，重发按序号去重。
- 时间为整数纳秒时间戳；`wire` 的区间是半开 `[valid_from, valid_to)`，
  `valid_to=null` 右端开放，允许事后补录；区间重叠时以 `(seq, off)`
  靠后的源端变更为准。
- `amp_value` 为十进制字符串，单位 `um`/`nm`，仓库用 `Decimal` 精确换算为
  **皮米整数**（1 μm = 10⁶ pm，1 nm = 10³ pm），亚皮米值直接拒收。
- 尾行 checksum 不匹配/结构损坏的文件隔离到 `deadletter/`，不推进进度。

spool 目录：`incoming/`（到达）→ `applied/`（发布后归档）；
重发文件进 `duplicates/`。

## 二、仓库结构与同步语义

| 表 | 作用 |
|---|---|
| `meta(last_seq)` | 已发布的连续提交前缀，与数据在**同一事务**提交 |
| `wiring` | 双时态接线：有效区间 + 来源定位 `(seq, off)` |
| `fact` | 每个 `obs_id` 的当前态（时间、通道、归属测点、皮米振幅） |
| `rollup` | 64 叉时间桶层级聚合（cnt / 峰值 / 峰值观测编号） |

关键保证：

1. **提交原子可见**：每提交一个 `BEGIN IMMEDIATE` 事务，提交前查询完全
   看不到半成品；WAL 快照让读写互不阻塞，一次查询内“统计 + 峰值证据 +
   同步进度”来自同一个连续提交前缀。
2. **大提交流式处理**：事件逐行读、逐行应用，引擎内可变状态只有接线缓存
   与“脏叶子桶”集合；30 万行单提交实测峰值 RSS < 90 MiB。
3. **崩溃恢复**：`SIGKILL` 后未提交事务由 SQLite 回滚，已发布前缀可直接
   查询；重启从 `last_seq` 继续，重放结果与顺序回放一致（实测验证）。
4. **rollup 双维护路径**：脏叶子少 → 自底向上定向重算；初始装载/大规模
   改归 → 集合式 SQL 整层重建（窗口函数，峰值并列取最小 obs_id）。

## 三、查询与回调预算

任意 `[lo, hi)` 被贪心分解为少量**完整层级桶**（读 rollup 行）+ 至多两个
**叶子残区**（合计宽度 < 2×64 个时间槽，直扫 fact）。百万宽度区间实测
仅 64 个完整桶；查询连接安装 `set_progress_handler(cb, 1)`（步长 1），
两次查询回调合计约 2×10³，远低于 10 万上限。同步连接不安装 handler，
不计入查询连接的回调预算。

## 四、接口与启动方式

```bash
# 端到端验证（一条命令，80 秒内输出可读结果与回调计数）
bash run.sh [数据目录，默认 /tmp/mseis-verify]

# 语义自检（临时目录，秒级）
PYTHONSAFEPATH=1 PYTHONPATH=. python3 -m mseis selftest

# 生成回放数据 / 同步一次 / 守护式同步（空闲退出）/ 查询
python3 -m mseis generate --data /tmp/d
python3 -m mseis sync     --data /tmp/d
python3 -m mseis sync     --data /tmp/d --daemon --idle-timeout 5
python3 -m mseis query    --data /tmp/d --point P0 --lo 0 --hi 1000000 [--json]
```

Python 代码 API：

```python
from mseis.db import open_writer, open_reader
from mseis.sync import process_once, run_daemon
from mseis.query import point_query, install_progress_handler

process_once(db_path, spool)                 # 处理当前可应用的连续前缀
reader = open_reader(db_path)                # 只读连接（WAL 快照）
counter = install_progress_handler(reader)   # 步长 1 回调计数
res = point_query(reader, "P0", lo, hi)      # QueryResult(count, peak_nm, peak_obs_id, prefix_seq)
```

## 五、验证结果（本机实测）

```
冷启动回放 1,000,000 观测（7 个提交，含修订/撤回/复活/接线更正）
kill@seq4 后重启续传 → seq8（10 万观测的 C0 通道改归 P0→P9）
独立流式期望模型对照：P0/P9 观测数、峰值、峰值编号全部一致
进度回调合计 2122 次（预算 100000，步长 1）；端到端 20.5s（限制 80s）
```
