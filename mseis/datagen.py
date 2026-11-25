"""本地回放数据生成器：百万条观测的采集库变更日志（确定性、可重复）。

数据集（默认 N=1,000,000）
=========================
* 5 个通道 C0..C4，各 20 万条观测；观测 i（obs_id=i+1）发生在 time_ns=i，
  通道为 C(i%5)，时间范围 [0, N)。
* seq 1：初始接线，Ck -> Pk，有效区间 [0, 开放)。
* seq 2..7：6 个数据提交，流式产出观测 upsert；
  - 每 997 条产生一条同提交内修订（换单位/改振幅，验证提交内顺序）；
  - 每 7001 条撤回一次，并在最后一个数据提交统一复活；
* seq 5 额外携带一条接线更正：C4 在 [1000, 2000) 改接 P4B，
  与原开放区间重叠，用于验证“后生效的源端变更优先”。
* 大规模通道改归提交（>=10 万观测）不由本生成器产出，
  由 verify 模块在验证者选定的窗口内即时生成（seq 8）。
"""
from __future__ import annotations

import os

from .logfmt import write_commit

N_OBS = 1_000_000
N_CHANNELS = 5
N_DATA_COMMITS = 6
FIRST_DATA_SEQ = 2
RETRACT_EVERY = 7_001
REVISE_EVERY = 997
REWIRE_SEQ = 5


def channel_of(i: int) -> str:
    return f"C{i % N_CHANNELS}"


def point_of_channel(ch: str) -> str:
    return f"P{ch[1:]}"


def amp_for(i: int, revised: bool = False) -> tuple[str, str]:
    """确定性振幅；revised=True 给出修订值（同时换单位，考验精确换算）。"""
    if revised:
        return str((i % 5_000) + 7), "nm"
    if i % 3 == 0:
        return f"{(i % 977) / 1000 + 0.001:.6f}", "um"
    return str((i % 5_000) + 1), "nm"


def _wire_events() -> list[dict]:
    return [
        {"type": "wire", "channel": f"C{k}", "valid_from": 0,
         "valid_to": None, "point": f"P{k}", "op": "upsert"}
        for k in range(N_CHANNELS)
    ]


def _data_events(lo: int, hi: int, resurrect_ids: list[int]) -> list[dict]:
    # 列表大小受单提交规模约束（约 17 万），生成端允许使用内存；
    # 仓库侧始终按行流式消费，不依赖该列表驻留。
    events: list[dict] = []
    for i in range(lo, hi):
        value, unit = amp_for(i)
        events.append({
            "type": "obs", "obs_id": i + 1, "time": i,
            "channel": channel_of(i), "amp_value": value,
            "amp_unit": unit, "op": "upsert",
        })
        if i % REVISE_EVERY == 0:
            value2, unit2 = amp_for(i, revised=True)
            events.append({
                "type": "obs", "obs_id": i + 1, "time": i,
                "channel": channel_of(i), "amp_value": value2,
                "amp_unit": unit2, "op": "upsert",
            })
        if i % RETRACT_EVERY == 0:
            events.append({"type": "retract", "obs_id": i + 1})
    for obs_id in resurrect_ids:
        i = obs_id - 1
        value, unit = amp_for(i, revised=True)
        events.append({
            "type": "obs", "obs_id": obs_id, "time": i,
            "channel": channel_of(i), "amp_value": value,
            "amp_unit": unit, "op": "upsert",
        })
    return events


def retracted_ids(n: int = N_OBS) -> list[int]:
    return [i + 1 for i in range(0, n, RETRACT_EVERY)]


def generate(spool: str, n: int = N_OBS) -> list[str]:
    """在 spool/incoming 下生成完整初始日志，返回文件路径列表。"""
    incoming = os.path.join(spool, "incoming")
    os.makedirs(incoming, exist_ok=True)
    paths: list[str] = []

    p = os.path.join(incoming, "0000000001.jsonl")
    write_commit(p, 1, iter(_wire_events()), committed_at=1_000)
    paths.append(p)

    last_seq = FIRST_DATA_SEQ + N_DATA_COMMITS - 1
    for c in range(N_DATA_COMMITS):
        lo = c * n // N_DATA_COMMITS
        hi = (c + 1) * n // N_DATA_COMMITS
        seq = FIRST_DATA_SEQ + c
        events = _data_events(lo, hi,
                              retracted_ids(n) if seq == last_seq else [])
        if seq == REWIRE_SEQ:  # 接线更正：重叠区间以后生效者为准
            events.append({
                "type": "wire", "channel": "C4",
                "valid_from": 1_000, "valid_to": 2_000,
                "point": "P4B", "op": "upsert",
            })
        p = os.path.join(incoming, f"{seq:010d}.jsonl")
        write_commit(p, seq, iter(events), committed_at=1_000 + seq)
        paths.append(p)
    return paths
