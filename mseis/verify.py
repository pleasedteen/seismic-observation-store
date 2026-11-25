"""端到端验证：冷启动回放百万观测 + 十万级通道改归 + 回调预算校验。

流程
====
1. 生成百万观测日志（独立数据目录，代码目录与数据目录分离）。
2. 冷启动打开仓库，先同步到 seq=4 后*强制关闭连接*模拟进程被 kill；
   重开仓库校验 last_seq 持久化，再继续回放（顺序回放一致性）。
3. 验证者在已有观测范围内选定改归窗口 [250000, 750000)，生成 seq 8：
   通道 C0 在该窗口改接到新测点 P9（窗口内 C0 观测恰为 10 万条）。
4. 回放 seq 8；随后重发一个已发布提交，验证幂等去重。
5. 用*独立的流式期望模型*（只依赖生成规则与日志，不读仓库）计算
   P0、P9 在覆盖改归区间的时段上的观测数、峰值、峰值观测编号。
6. 打开查询连接并安装**步长 1** 进度处理器，对新旧测点各查一次，
   汇总自“打开仓库”起所有连接的回调总数，断言 <= 100_000。
"""
from __future__ import annotations

import array
import os
import shutil
import time
from dataclasses import dataclass

from . import datagen
from . import db as dbmod
from .logfmt import parse_commit, write_commit
from .query import (QueryResult, decompose, install_progress_handler,
                    point_query)
from .sync import process_once

REWIRE_SEQ = 8
NEW_POINT = "P9"
WINDOW_LO = 250_000
WINDOW_HI = 750_000
CALLBACK_BUDGET = 100_000


@dataclass
class Expected:
    count: int
    peak_pm: int | None
    peak_obs: int | None


# --------------------------------------------------------------------- #
# 独立期望模型：流式重放全部日志，紧凑整数数组保存当前态，不依赖仓库代码
# --------------------------------------------------------------------- #
def build_expectation(spool: str, n: int) -> dict[str, Expected]:
    active = array.array("b", [0]) * n          # 1=有效 0=已撤回
    state_id = array.array("i", [-1]) * n       # obs_id-1 -> 槽位 id
    st_time = array.array("i")
    st_chan = array.array("b")
    st_amp = array.array("q")                   # 皮米
    wires: dict[str, list[dict]] = {}

    applied = sorted(
        f for f in os.listdir(os.path.join(spool, "applied"))
        if f.endswith(".jsonl")
    )
    for name in applied:
        parser = parse_commit(os.path.join(spool, "applied", name))
        info, _begin = next(parser)
        for _info, ev in parser:
            t = ev["type"]
            if t == "wire":
                wires.setdefault(ev["channel"], []).append({
                    "from": int(ev["valid_from"]),
                    "to": (None if ev["valid_to"] is None
                           else int(ev["valid_to"])),
                    "point": ev["point"], "seq": info.seq,
                    "off": int(ev["off"]),
                })
            elif t == "obs":
                i = int(ev["obs_id"]) - 1
                from .logfmt import amplitude_to_picometers
                pm = amplitude_to_picometers(str(ev["amp_value"]),
                                             str(ev["amp_unit"]))
                slot = state_id[i]
                if slot < 0:
                    slot = len(st_time)
                    state_id[i] = slot
                    st_time.append(int(ev["time"]))
                    st_chan.append(int(str(ev["channel"])[1:]))
                    st_amp.append(pm)
                    active[i] = 1
                else:
                    st_time[slot] = int(ev["time"])
                    st_chan[slot] = int(str(ev["channel"])[1:])
                    st_amp[slot] = pm
                    active[i] = 1
            elif t == "retract":
                active[int(ev["obs_id"]) - 1] = 0

    def resolve(channel: str, t_ns: int) -> str | None:
        best, point = None, None
        for w in wires.get(channel, ()):
            if t_ns < w["from"]:
                continue
            if w["to"] is not None and t_ns >= w["to"]:
                continue
            key = (w["seq"], w["off"])
            if best is None or key > best:
                best, point = key, w["point"]
        return point

    def expected_for(point: str, lo: int, hi: int) -> Expected:
        cnt = 0
        peak_pm: int | None = None
        peak_obs: int | None = None
        for slot in range(len(st_time)):
            t_ns = st_time[slot]
            if t_ns < lo or t_ns >= hi:
                continue
            i = st_time[slot]  # 本数据集 time==obs_id-1，且修订不改时间
            if not active[i]:
                continue
            channel = f"C{st_chan[slot]}"
            if resolve(channel, t_ns) != point:
                continue
            cnt += 1
            pm = st_amp[slot]
            obs = i + 1
            if peak_pm is None or pm > peak_pm or (
                    pm == peak_pm and obs < peak_obs):
                peak_pm, peak_obs = pm, obs
        return Expected(cnt, peak_pm, peak_obs)

    qlo, qhi = 0, n
    return {
        "P0": expected_for("P0", qlo, qhi),
        NEW_POINT: expected_for(NEW_POINT, qlo, qhi),
    }


def _emit_rewire(spool: str) -> str:
    incoming = os.path.join(spool, "incoming")
    path = os.path.join(incoming, f"{REWIRE_SEQ:010d}.jsonl")
    write_commit(path, REWIRE_SEQ, iter([{
        "type": "wire", "channel": "C0",
        "valid_from": WINDOW_LO, "valid_to": WINDOW_HI,
        "point": NEW_POINT, "op": "upsert",
    }]), committed_at=9_000)
    return path


def run(data_root: str) -> bool:
    spool = os.path.join(data_root, "spool")
    dbp = os.path.join(data_root, "warehouse.db")
    if os.path.exists(data_root):
        shutil.rmtree(data_root)
    os.makedirs(data_root)

    t0 = time.monotonic()
    print("== 1/6 生成百万观测回放日志 ==")
    datagen.generate(spool)
    print(f"   日志就绪：{os.path.join(spool, 'incoming')} "
          f"({time.monotonic()-t0:.1f}s)")

    print("== 2/6 冷启动回放，并模拟进程被 kill 后重启 ==")
    conn = dbmod.open_writer(dbp)                 # 打开仓库
    # 先只发布 seq 1..4（process_once 按连续前缀自动停在断档前——
    # 这里用临时移走后续文件制造“只到 4”的现场）
    incoming = os.path.join(spool, "incoming")
    held = []
    for name in sorted(os.listdir(incoming)):
        seq = int(name.split(".")[0])
        if seq > 4:
            held.append(name)
            os.rename(os.path.join(incoming, name),
                      os.path.join(data_root, name))
    process_once(dbp, spool, conn)
    assert dbmod.get_progress(conn) == 4
    conn.close()                                   # 强制终止：无收尾提交
    conn = dbmod.open_writer(dbp)                 # 重启：从持久化进度继续
    assert dbmod.get_progress(conn) == 4, "kill 后进度丢失"
    for name in held:
        os.rename(os.path.join(data_root, name),
                  os.path.join(incoming, name))
    process_once(dbp, spool, conn)
    assert dbmod.get_progress(conn) == 7
    conn.close()
    print(f"   kill@seq4 -> 重启续传 -> seq7 完成 "
          f"({time.monotonic()-t0:.1f}s)")

    print("== 3/6 验证者选定改归窗口，生成十万级通道改归提交 ==")
    _emit_rewire(spool)
    n_in_window = sum(
        1 for i in range(WINDOW_LO, WINDOW_HI) if i % datagen.N_CHANNELS == 0
    )
    print(f"   窗口 [{WINDOW_LO}, {WINDOW_HI}) 内 C0 观测 = "
          f"{n_in_window} 条（>=100000）")

    print("== 4/6 回放改归提交，并重发已发布提交验证幂等 ==")
    process_once(dbp, spool)
    applied = os.path.join(spool, "applied", "0000000002.jsonl")
    dup = os.path.join(spool, "incoming", "0000000002.jsonl")
    shutil.copyfile(applied, dup)
    stats = process_once(dbp, spool)
    assert stats.duplicates == 1 and not os.path.exists(dup)
    print(f"   已发布 seq={REWIRE_SEQ}；重发文件进入 duplicates，"
          f"结果不变 ({time.monotonic()-t0:.1f}s)")

    print("== 5/6 独立流式期望模型计算对照值 ==")
    expected = build_expectation(spool, datagen.N_OBS)
    print(f"   期望 P0: count={expected['P0'].count} | "
          f"{NEW_POINT}: count={expected[NEW_POINT].count} "
          f"({time.monotonic()-t0:.1f}s)")

    print("== 6/6 打开查询连接（步长 1 进度回调），新旧测点各查一次 ==")
    total_callbacks = 0

    def one_query(point: str) -> tuple[QueryResult, int, int, int]:
        reader = dbmod.open_reader(dbp)           # 新连接即“打开仓库”
        counter = install_progress_handler(reader)
        try:
            nodes, scans = decompose(0, datagen.N_OBS, 3)
            res = point_query(reader, point, 0, datagen.N_OBS)
        finally:
            total = counter.n
            reader.close()
        return res, total, len(nodes), len(scans)

    r_old, cb_old, n_nodes, n_scans = one_query("P0")
    total_callbacks += cb_old
    r_new, cb_new, _, _ = one_query(NEW_POINT)
    total_callbacks += cb_new

    from .logfmt import picometers_to_nanometers
    exp_old, exp_new = expected["P0"], expected[NEW_POINT]
    ok = True

    def check(label: str, got, exp: Expected, res: QueryResult) -> bool:
        nonlocal ok
        cond = (
            res.count == exp.count
            and res.peak_obs_id == exp.peak_obs
            and (exp.peak_pm is None
                 or res.peak_nm == picometers_to_nanometers(exp.peak_pm))
            and res.prefix_seq == REWIRE_SEQ
        )
        ok &= cond
        print(f"   [{ 'PASS' if cond else 'FAIL' }] {label}")
        print(f"        仓库: count={got} 峰值={res.peak_nm}nm "
              f"obs_id={res.peak_obs_id} 前缀=seq{res.prefix_seq}")
        print(f"        期望: count={exp.count} 峰值="
              f"{picometers_to_nanometers(exp.peak_pm) if exp.peak_pm is not None else None}nm "
              f"obs_id={exp.peak_obs}")
        return cond

    print(f"   查询分解：完整桶 {n_nodes} 个，叶子残区 {n_scans} 段")
    check("旧测点 P0（覆盖改归区间 [0, 1e6)）", r_old.count, exp_old, r_old)
    check(f"新测点 {NEW_POINT}（覆盖改归区间 [0, 1e6)）",
          r_new.count, exp_new, r_new)

    budget_ok = total_callbacks <= CALLBACK_BUDGET
    ok &= budget_ok
    print(f"   [{'PASS' if budget_ok else 'FAIL'}] 进度回调合计 "
          f"{total_callbacks} 次（P0 {cb_old} + {NEW_POINT} {cb_new}），"
          f"预算 {CALLBACK_BUDGET}，步长=1")

    elapsed = time.monotonic() - t0
    print(f"-- 总耗时 {elapsed:.1f}s（限制 80s），"
          f"结果：{'全部通过' if ok and elapsed < 80 else '存在失败'} --")
    return ok and elapsed < 80
