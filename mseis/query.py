"""查询接口：按测点 + 任意观测时段统计观测数、峰值振幅（纳米）与对应观测编号。

回调预算的实现
==============
查询连接可以安装 *步长为 1* 的 SQLite 进度处理器
（``set_progress_handler(h, 1)``，每执行一个虚拟机指令回调一次）。
若直接扫描十万/百万级 fact 行，回调数必然超过十万上限。因此查询路径为：

* 任意 [lo, hi) 经 64 叉层级桶贪心分解为少量*完整桶*（读 rollup 聚合行）
  加至多两个*叶子残区*（残区合计不超过 2*W0=128 个时间槽，直接读 fact）；
* 整次查询（读取同步进度、聚合、峰值证据）在同一个只读事务内完成，
  WAL 快照保证三者来自同一个连续提交前缀。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .db import BRANCH, W0, get_progress
from .logfmt import picometers_to_nanometers


@dataclass(frozen=True)
class QueryResult:
    point: str
    lo: int
    hi: int
    prefix_seq: int          # 本结果基于的连续提交前缀
    count: int
    peak_nm: str | None      # 精确纳米值；无观测时为 None
    peak_obs_id: int | None  # 并列峰值取最小观测编号


def decompose(lo: int, hi: int, lvl_max: int
              ) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """把 [lo, hi) 分解为 (完整桶列表 (level, bucket), 叶子残区列表 [l, h))。

    贪心：从最低级开始，能升整桶就升级（受 lvl_max 限制），对不齐或越过
    右端就降级；入口/出口处对不齐 W0 的部分留作叶子残区直接扫描。
    """
    nodes: list[tuple[int, int]] = []
    scans: list[tuple[int, int]] = []
    cur = lo
    while cur < hi:
        # 自细到粗找“起点对齐且整个桶不越过 hi”的最大级（宽度逐级嵌套，
        # 一旦某级不对齐/越界，更粗级也不可能成立，直接停止）。
        chosen = -1
        for lvl in range(0, lvl_max + 1):
            width = W0 * BRANCH ** lvl
            bstart = (cur // width) * width
            if bstart == cur and bstart + width <= hi:
                chosen = lvl
            else:
                break
        if chosen >= 0:
            width = W0 * BRANCH ** chosen
            nodes.append((chosen, cur // width))
            cur += width
        else:  # 放不下任何完整叶子桶：扫描到下一个叶子边界（或 hi）
            b0 = (cur // W0) * W0
            end = min(hi, b0 + W0)
            scans.append((cur, end))
            cur = end
    return nodes, scans


class ProgressCounter:
    """SQLite 进度处理器容器；n 即该连接自安装以来的回调总次数。"""

    def __init__(self) -> None:
        self.n = 0

    def __call__(self) -> int:  # 返回 0 表示不中断长查询
        self.n += 1
        return 0


def install_progress_handler(conn: sqlite3.Connection) -> ProgressCounter:
    """在连接上安装步长为 1 的进度处理器并返回计数器。"""
    counter = ProgressCounter()
    conn.set_progress_handler(counter, 1)
    return counter


def install_progress_counter(conn: sqlite3.Connection) -> ProgressCounter:
    """在连接上安装步长为 1 的进度处理器并返回计数器。"""
    counter = ProgressCounter()
    conn.set_progress_handler(counter, 1)
    return counter


def clear_progress_handler(conn: sqlite3.Connection) -> None:
    conn.set_progress_handler(None, 0)


def _agg_leaf(conn: sqlite3.Connection, point: str, lo: int, hi: int
              ) -> tuple[int, int | None, int | None]:
    """直接扫描 fact 的一个叶子残区（宽度 < W0）。"""
    row = conn.execute(
        "SELECT COUNT(*), MAX(amp_pm), "
        "  MIN(CASE WHEN amp_pm=(SELECT MAX(amp_pm) FROM fact "
        "            WHERE point=? AND time_ns>=? AND time_ns<?) "
        "       THEN obs_id END) "
        "FROM fact WHERE point=? AND time_ns>=? AND time_ns<?",
        (point, lo, hi, point, lo, hi),
    ).fetchone()
    cnt = int(row[0])
    if cnt == 0:
        return 0, None, None
    return cnt, int(row[1]), int(row[2])


def point_query(conn: sqlite3.Connection, point: str,
                lo: int, hi: int) -> QueryResult:
    """在单个只读事务/快照内完成一次查询。"""
    if hi <= lo:
        raise ValueError("查询时段必须满足 lo < hi")
    conn.execute("BEGIN")
    try:
        prefix_seq = get_progress(conn)
        row = conn.execute(
            "SELECT MAX(level) FROM rollup WHERE point=?", (point,)
        ).fetchone()
        lvl_max = -1 if row is None or row[0] is None else int(row[0])

        count = 0
        best_pm: int | None = None
        best_obs: int | None = None

        def consider(c: int, pm: int | None, obs: int | None) -> None:
            nonlocal count, best_pm, best_obs
            count += c
            if pm is None:
                return
            if best_pm is None or pm > best_pm or (pm == best_pm and obs < best_obs):
                best_pm, best_obs = pm, obs

        if lvl_max >= 0:
            nodes, scans = decompose(lo, hi, lvl_max)
            for level, bucket in nodes:
                r = conn.execute(
                    "SELECT cnt, max_pm, max_obs FROM rollup "
                    "WHERE point=? AND level=? AND bucket=?",
                    (point, level, bucket),
                ).fetchone()
                if r is not None:
                    consider(int(r[0]), int(r[1]), int(r[2]))
            for slo, shi in scans:
                consider(*_agg_leaf(conn, point, slo, shi))
        else:  # 该测点尚无任何聚合：直接查（结果必然很少/为空）
            consider(*_agg_leaf(conn, point, lo, hi))
        return QueryResult(
            point=point, lo=lo, hi=hi, prefix_seq=prefix_seq,
            count=count,
            peak_nm=(None if best_pm is None
                     else picometers_to_nanometers(best_pm)),
            peak_obs_id=best_obs,
        )
    finally:
        conn.execute("COMMIT")
