"""同步进程：监听 spool 目录，按源端提交顺序把日志落进 SQLite 仓库。

目录布局（data_root 下）
=======================
``incoming/``  新到达的提交文件（可能乱序、重发，文件名 0000000012.jsonl）
``applied/``   已成功发布的提交，归档保留（重发同名文件时用来比对去重）
``duplicates/`` 重发且已发布的文件，移动到这里留痕
``deadletter/`` 校验失败/结构损坏的文件，隔离等待人工处理（不推进进度）
``warehouse.db`` SQLite 仓库（WAL 模式，附带 -wal/-shm）

顺序与可见性
============
* 仓库只推进*连续前缀*：last_seq 之后的下一个序号未到，则等待，已到的
  更大序号文件留在 incoming 不处理 —— 乱序到达天然安全。
* 每个提交先完整流式校验（checksum），再在单事务内应用并更新 last_seq，
  事务提交后查询侧立即可见整笔提交；处理期间查询照常读旧快照。
* 重发：序号 <= last_seq 的文件不会再次应用。
"""
from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass

from .db import SyncEngine, get_progress, open_writer
from .logfmt import CommitFormatError, CommitInfo, parse_commit


@dataclass
class SyncStats:
    applied: int = 0          # 本次运行新发布的提交数
    duplicates: int = 0
    deadlettered: int = 0
    waited: bool = False      # 是否经历过“等日志”（用于演示不阻塞查询）


def _seq_from_name(name: str) -> int | None:
    stem = name.removesuffix(".jsonl")
    if stem.isdigit():
        return int(stem)
    return None


def _move(path: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, os.path.basename(path))
    os.replace(path, dest)
    return dest


def process_once(db_path: str, spool: str,
                 conn: sqlite3.Connection | None = None) -> SyncStats:
    """处理所有当前可按连续前缀应用的提交后返回（不阻塞等待）。"""
    stats = SyncStats()
    incoming = os.path.join(spool, "incoming")
    applied_dir = os.path.join(spool, "applied")
    dup_dir = os.path.join(spool, "duplicates")
    dead_dir = os.path.join(spool, "deadletter")
    for d in (incoming, applied_dir, dup_dir, dead_dir):
        os.makedirs(d, exist_ok=True)

    own_conn = conn is None
    if own_conn:
        conn = open_writer(db_path)
    assert conn is not None
    try:
        while True:
            last_seq = get_progress(conn)
            present = {
                s: os.path.join(incoming, n)
                for n in os.listdir(incoming)
                if (s := _seq_from_name(n)) is not None
            }
            # 先处理重发：序号已发布的文件不允许再次进入引擎。
            for s in sorted(present):
                if s <= last_seq:
                    _move(present[s], dup_dir)
                    stats.duplicates += 1
                    present.pop(s)
            nxt = last_seq + 1
            if nxt not in present:  # 连续前缀断档：等待乱序日志补齐
                stats.waited = stats.applied > 0 or stats.duplicates > 0
                return stats
            path = present[nxt]
            try:
                parser = parse_commit(path)
                info, begin = next(parser)  # 头行；info.seq 已校验
                assert isinstance(info, CommitInfo)
                if info.seq != nxt:
                    raise CommitFormatError("头行序号与文件名不一致")
                with SyncEngine(conn, info) as engine:
                    for _info, event in parser:  # 尾行前逐行流式应用
                        kind = event["type"]
                        if kind == "wire":
                            engine.apply_wire(event)
                        elif kind == "obs":
                            engine.apply_obs(event)
                        elif kind == "retract":
                            engine.apply_retract(event)
                        else:
                            raise CommitFormatError(
                                f"未知事件类型 {kind!r}")
                # 事务已提交（崩溃点之后也不丢），再归档文件。
                _move(path, applied_dir)
                stats.applied += 1
            except CommitFormatError as exc:
                _move(path, dead_dir)
                stats.deadlettered += 1
                print(f"[sync] 提交 {nxt} 隔离到 deadletter: {exc}")
                return stats
    finally:
        if own_conn:
            # 合并 WAL，保证强制终止后只需打开主库即可查询。
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()


def run_daemon(db_path: str, spool: str,
               poll_interval: float = 0.2,
               idle_timeout: float | None = None) -> SyncStats:
    """持续同步：周期性轮询 incoming，idle_timeout 秒无新提交则退出。

    轮询间隙不持有任何事务，查询连接始终能读到最近一次已发布前缀。
    """
    conn = open_writer(db_path)
    total = SyncStats()
    idle_since: float | None = None
    try:
        while True:
            before = (total.applied, total.duplicates, total.deadlettered)
            part = process_once(db_path, spool, conn)
            total.applied += part.applied
            total.duplicates += part.duplicates
            total.deadlettered += part.deadlettered
            progressed = (total.applied, total.duplicates,
                          total.deadlettered) != before
            if progressed:
                idle_since = None
            else:
                if idle_since is None:
                    idle_since = time.monotonic()
                if idle_timeout is not None and \
                        time.monotonic() - idle_since >= idle_timeout:
                    return total
            time.sleep(poll_interval)
    finally:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
