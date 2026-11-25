"""微震采集库变更日志的线上格式（JSONL 提交信封）。

每个源端提交对应一个独立文件，UTF-8、逐行 JSON，结构如下：

    {"type":"begin","seq":12,"committed_at":1732500000000000000}
    {"type":"obs","off":0,"obs_id":1001,"time":...,"channel":"C1",
     "amp_value":"3.75","amp_unit":"um","op":"upsert"}
    {"type":"retract","off":1,"obs_id":1001}
    {"type":"wire","off":2,"channel":"C1","valid_from":...,"valid_to":null,
     "point":"P9","op":"upsert"}
    {"type":"end","seq":12,"events":3,"checksum":"sha256:..."}

规则
----
* ``seq`` 是源端单调递增的提交序号；事件行 ``off`` 是提交内 0 起的顺序号，
  变更在同一提交内严格按 ``off`` 生效。
* 时间均为整数纳秒时间戳（Unix epoch，任意起点亦可，仓库只做整数比较）。
* ``amp_value`` 为十进制字符串（避免二进制浮点误差），单位 ``um`` 或 ``nm``；
  仓库按精确换算存为皮米整数（1 um = 1_000_000 pm，1 nm = 1_000 pm）。
* ``wire`` 描述通道在有效时间区间 [valid_from, valid_to) 内接入的测点；
  ``valid_to`` 为 null 表示右端开放。区间可重叠，重叠部分以提交顺序
  （seq, off）靠后的源端变更为准；接线允许事后补录（valid_from 早于提交时间）。
* ``retract`` 撤回观测；撤回后允许再次 upsert（复活/重新修订）。
* 尾行 checksum 为头行之后所有事件行字节内容的 sha256，防止截断/损坏文件入库。

文件命名：``<10 位补零 seq>.jsonl``，例如 ``0000000012.jsonl``。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Iterable, Iterator

# 各单位到皮米的精确整数换算系数（皮米是微震振幅的统一内部计量单位）。
UNIT_TO_PM = {"nm": 1_000, "um": 1_000_000}

BEGIN = "begin"
END = "end"
OBS = "obs"
RETRACT = "retract"
WIRE = "wire"


class CommitFormatError(ValueError):
    """日志文件结构或校验和不符合线上格式。"""


@dataclass(frozen=True)
class CommitInfo:
    seq: int
    committed_at: int
    events: int
    checksum: str


def _loads(path: str, line: bytes) -> dict:
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        raise CommitFormatError(f"{path}: 非法 JSON 行: {exc}") from exc


def parse_commit(path: str) -> Iterator[tuple[CommitInfo | None, dict]]:
    """流式解析一个提交文件，按行 yield (info, event)。

    第一帧为 (CommitInfo, begin 事件 dict)；之后每帧 info 为 None；
    尾行只做校验，不产出事件。文件整体读完且 checksum 匹配才算合法，
    调用方应在迭代器正常耗尽（无异常）后才提交事务。
    """
    with open(path, "rb") as fh:
        raw = fh.readlines()
    if len(raw) < 2:
        raise CommitFormatError(f"{path}: 提交至少包含头行和尾行")

    digest = hashlib.sha256()
    begin = _loads(path, raw[0])
    if begin.get("type") != BEGIN:
        raise CommitFormatError(f"{path}: 缺少 begin 头行")
    seq = int(begin["seq"])
    info = CommitInfo(
        seq=seq,
        committed_at=int(begin["committed_at"]),
        events=-1,
        checksum="",
    )
    yield info, begin

    end = _loads(path, raw[-1])
    if end.get("type") != END or int(end.get("seq", -1)) != seq:
        raise CommitFormatError(f"{path}: 尾行缺失或提交序号不一致")
    declared_events = int(end["events"])
    checksum = str(end["checksum"])

    seen = 0
    for i in range(1, len(raw) - 1):
        line = raw[i]
        digest.update(line)
        event = _loads(path, line)
        if int(event.get("off", -1)) != seen:
            raise CommitFormatError(f"{path}: 事件 off 必须从 0 连续编号")
        seen += 1
        yield None, event

    if seen != declared_events:
        raise CommitFormatError(
            f"{path}: 事件数不符（尾行 {declared_events}，实际 {seen}）"
        )
    actual = "sha256:" + digest.hexdigest()
    if actual != checksum:
        raise CommitFormatError(f"{path}: checksum 校验失败（文件损坏或被截断）")


def write_commit(
    path: str,
    seq: int,
    events: Iterable[dict],
    committed_at: int,
) -> CommitInfo:
    """将事件迭代器流式写成一个提交文件（先写 .tmp 再原子 rename）。

    事件 dict 只需提供业务字段，``off`` 由本函数按产出顺序统一编号，
    保证“提交内顺序”即写入顺序。
    """
    tmp = path + ".tmp"
    digest = hashlib.sha256()
    count = 0
    with open(tmp, "wb") as fh:
        fh.write(
            (json.dumps(
                {"type": BEGIN, "seq": seq, "committed_at": committed_at},
                ensure_ascii=False, separators=(",", ":")) + "\n"
             ).encode("utf-8")
        )
        for ev in events:
            ev = dict(ev)
            ev["off"] = count
            count += 1
            line = (json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
                    + "\n").encode("utf-8")
            digest.update(line)
            fh.write(line)
        fh.write(
            (json.dumps(
                {"type": END, "seq": seq, "events": count,
                 "checksum": "sha256:" + digest.hexdigest()},
                ensure_ascii=False, separators=(",", ":")) + "\n"
             ).encode("utf-8")
        )
    os_replace(tmp, path)
    return CommitInfo(seq, committed_at, count,
                      "sha256:" + digest.hexdigest())


def os_replace(src: str, dst: str) -> None:
    import os
    os.replace(src, dst)


def amplitude_to_picometers(value: str, unit: str) -> int:
    """把源端振幅精确换算为皮米整数。

    使用高精度十进制上下文，换算结果必须为整数皮米（本系统最小刻度）；
    若源端给出比皮米更细的小数则直接报错，避免静默截断。
    """
    if unit not in UNIT_TO_PM:
        raise CommitFormatError(f"不支持的振幅单位: {unit!r}")
    with localcontext() as ctx:
        ctx.prec = 40
        dec = Decimal(value) * UNIT_TO_PM[unit]
        if dec != dec.to_integral_value():
            raise CommitFormatError(
                f"振幅 {value} {unit} 换算到皮米不是整数，精度无法无损表示"
            )
        return int(dec)


def picometers_to_nanometers(pm: int) -> str:
    """皮米整数转回纳米展示值（保留精确小数，去除多余的 0）。"""
    with localcontext() as ctx:
        ctx.prec = 40
        s = format(Decimal(pm) / Decimal(1000), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
