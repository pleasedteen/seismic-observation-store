"""内置小型语义自检：用临时仓库快速验证全部关键规则（秒级完成）。

覆盖：修订/撤回/复活、微米纳米精确换算与并列取小编号、接线补录与
重叠区间后生效优先、乱序到达按连续前缀推进、重发幂等、kill 后重启
（last_seq 持久化）、读不阻塞写且同响应同前缀、区间分解正确性。
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile

from . import db as dbmod
from .logfmt import write_commit
from .query import decompose, install_progress_handler, point_query
from .sync import process_once


class CheckError(AssertionError):
    pass


def _spool_commit(root: str, spool: str, seq: int, events: list[dict]) -> str:
    incoming = os.path.join(spool, "incoming")
    os.makedirs(incoming, exist_ok=True)
    path = os.path.join(incoming, f"{seq:010d}.jsonl")
    write_commit(path, seq, iter(events), committed_at=10_000 + seq)
    return path


def _sync(root: str, spool: str) -> None:
    process_once(os.path.join(root, "w.db"), spool)


def _reader(root: str) -> sqlite3.Connection:
    return dbmod.open_reader(os.path.join(root, "w.db"))


def run() -> None:
    root = tempfile.mkdtemp(prefix="mseis-selftest-")
    spool = os.path.join(root, "spool")
    dbp = os.path.join(root, "w.db")
    try:
        # 1) 接线 + 观测：换算、修订、撤回、复活、并列取最小 obs_id
        _spool_commit(root, spool, 1, [
            {"type": "wire", "channel": "C1", "valid_from": 0,
             "valid_to": None, "point": "P1"},
        ])
        _spool_commit(root, spool, 2, [
            # 2 um = 2_000_000 pm = 2000 nm
            {"type": "obs", "obs_id": 10, "time": 10, "channel": "C1",
             "amp_value": "2", "amp_unit": "um", "op": "upsert"},
            # 2000 nm 与上者并列，编号 11 更大
            {"type": "obs", "obs_id": 11, "time": 11, "channel": "C1",
             "amp_value": "2000", "amp_unit": "nm", "op": "upsert"},
            {"type": "obs", "obs_id": 5, "time": 12, "channel": "C1",
             "amp_value": "2000", "amp_unit": "nm", "op": "upsert"},
            {"type": "retract", "obs_id": 11},
            {"type": "obs", "obs_id": 11, "time": 11, "channel": "C1",
             "amp_value": "0.003", "amp_unit": "um", "op": "upsert"},
            {"type": "obs", "obs_id": 7, "time": 70, "channel": "C1",
             "amp_value": "9.5", "amp_unit": "nm", "op": "upsert"},
        ])
        _sync(root, spool)
        r = point_query(_reader(root), "P1", 0, 100)
        assert r.count == 4, r
        assert r.peak_nm == "2000", r          # 2 um 精确换算
        assert r.peak_obs_id == 5, r           # 并列取最小编号
        assert r.prefix_seq == 2, r

        # 2) 接线事后补录：先到的无接线观测随后被归属
        _spool_commit(root, spool, 3, [
            {"type": "obs", "obs_id": 20, "time": 20, "channel": "C2",
             "amp_value": "50", "amp_unit": "nm", "op": "upsert"},
        ])
        _sync(root, spool)
        r0 = point_query(_reader(root), "P2", 0, 100)
        assert r0.count == 0 and r0.peak_obs_id is None, r0
        _spool_commit(root, spool, 4, [
            {"type": "wire", "channel": "C2", "valid_from": 0,
             "valid_to": None, "point": "P2"},
        ])
        _sync(root, spool)
        r0 = point_query(_reader(root), "P2", 0, 100)
        assert r0.count == 1 and r0.peak_obs_id == 20, r0

        # 3) 重叠区间以后生效的源端变更为准（含更正后再被更新区间覆盖）
        _spool_commit(root, spool, 5, [
            {"type": "wire", "channel": "C1", "valid_from": 10,
             "valid_to": 11, "point": "PX"},  # 半开区间只含 t=10
        ])
        _sync(root, spool)
        rx = point_query(_reader(root), "PX", 0, 100)
        assert rx.count == 1 and rx.peak_obs_id == 10, rx  # t=10 改归
        r1 = point_query(_reader(root), "P1", 0, 100)
        assert r1.count == 3, r1  # 11,12,70 仍在 P1

        # 4) 乱序到达：只推进连续前缀；补齐后自动继续
        _spool_commit(root, spool, 7, [
            {"type": "obs", "obs_id": 30, "time": 30, "channel": "C2",
             "amp_value": "1", "amp_unit": "nm", "op": "upsert"},
        ])
        _sync(root, spool)  # 7 到达但 6 缺失：不能应用
        w = dbmod.open_writer(dbp)
        assert dbmod.get_progress(w) == 5
        w.close()
        _spool_commit(root, spool, 6, [
            {"type": "obs", "obs_id": 29, "time": 29, "channel": "C2",
             "amp_value": "1", "amp_unit": "nm", "op": "upsert"},
        ])
        _sync(root, spool)  # 6、7 连续补齐
        w = dbmod.open_writer(dbp)
        assert dbmod.get_progress(w) == 7
        w.close()
        assert point_query(_reader(root), "P2", 0, 100).count == 3

        # 5) 重发幂等：重复文件进 duplicates，结果与进度不变
        dup = os.path.join(spool, "incoming", "0000000006.jsonl")
        write_commit(dup, 6, iter([
            {"type": "obs", "obs_id": 999, "time": 29, "channel": "C2",
             "amp_value": "1", "amp_unit": "nm", "op": "upsert"},
        ]), committed_at=999)
        _sync(root, spool)
        assert os.path.exists(os.path.join(spool, "duplicates",
                                           "0000000006.jsonl"))
        r2 = point_query(_reader(root), "P2", 0, 100)
        assert r2.count == 3 and r2.prefix_seq == 7, r2

        # 6) 模拟 kill 后重启：新进程重开库，进度与数据都在
        del w
        w2 = dbmod.open_writer(dbp)
        assert dbmod.get_progress(w2) == 7
        w2.close()
        assert point_query(_reader(root), "P1", 0, 100).count == 3

        # 7) 读不阻塞写 + 同响应同前缀：先开读事务，再发布提交
        reader = _reader(root)
        install_progress_handler(reader)
        reader.execute("BEGIN")
        before = dbmod.get_progress(reader)
        _spool_commit(root, spool, 8, [
            {"type": "obs", "obs_id": 40, "time": 40, "channel": "C2",
             "amp_value": "1", "amp_unit": "nm", "op": "upsert"},
        ])
        _sync(root, spool)  # 写入不被长事务阻塞（WAL 快照隔离）
        snap_seq = dbmod.get_progress(reader)  # 旧快照仍读到旧前缀
        assert snap_seq == before, (snap_seq, before)
        reader.execute("COMMIT")
        after = point_query(reader, "P2", 0, 100)
        assert after.prefix_seq == 8 and after.count == 4, after

        # 8) 区间分解：暴力枚举对比（小宇宙）
        for lo in range(0, 300, 37):
            for hi in range(lo + 1, 900, 53):
                nodes, scans = decompose(lo, hi, 3)
                covered: list[int] = []
                for level, bucket in nodes:
                    width = dbmod.W0 * dbmod.BRANCH ** level
                    covered.extend(range(bucket * width,
                                         bucket * width + width))
                for a, b in scans:
                    covered.extend(range(a, b))
                assert sorted(covered) == list(range(lo, hi)), (lo, hi)

        # 9) 损坏文件进 deadletter，不推进进度
        bad = os.path.join(spool, "incoming", "0000000009.jsonl")
        with open(bad, "w", encoding="utf-8") as fh:
            fh.write('{"type":"begin","seq":9,"committed_at":1}\n')
            fh.write("not-json\n")
        _sync(root, spool)
        assert os.path.exists(os.path.join(spool, "deadletter",
                                           "0000000009.jsonl"))
        w3 = dbmod.open_writer(dbp)
        assert dbmod.get_progress(w3) == 8
        w3.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
