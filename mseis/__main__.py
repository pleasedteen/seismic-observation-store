"""命令行入口。

用法
====
    python -m mseis selftest                 # 内置语义自检（临时目录，秒级）
    python -m mseis generate --data DIR [--n 1000000]
    python -m mseis sync --data DIR [--daemon --idle-timeout 5]
    python -m mseis query --data DIR --point P0 --lo 0 --hi 1000000
    python -m mseis verify --data DIR       # 端到端验证（含回调计数）

所有模块只用 Python 标准库；建议用 ``python -I`` 运行，避免当前目录里
被植入的同名模块影响导入（数据目录与代码目录始终分离，路径通过参数传入）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import datagen, db, selftest, verify
from .query import install_progress_handler, point_query
from .sync import process_once, run_daemon


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mseis")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("selftest")

    g = sub.add_parser("generate")
    g.add_argument("--data", required=True)
    g.add_argument("--n", type=int, default=datagen.N_OBS)

    s = sub.add_parser("sync")
    s.add_argument("--data", required=True)
    s.add_argument("--daemon", action="store_true")
    s.add_argument("--idle-timeout", type=float, default=None)
    s.add_argument("--poll", type=float, default=0.2)

    q = sub.add_parser("query")
    q.add_argument("--data", required=True)
    q.add_argument("--point", required=True)
    q.add_argument("--lo", type=int, required=True)
    q.add_argument("--hi", type=int, required=True)
    q.add_argument("--json", action="store_true")

    v = sub.add_parser("verify")
    v.add_argument("--data", required=True)

    args = parser.parse_args(argv)

    if args.cmd == "selftest":
        selftest.run()
        print("selftest: 全部通过")
        return 0

    spool = os.path.join(args.data, "spool")
    dbp = os.path.join(args.data, "warehouse.db")

    if args.cmd == "generate":
        paths = datagen.generate(spool, args.n)
        print(f"已生成 {len(paths)} 个提交文件于 {os.path.join(spool, 'incoming')}")
        return 0

    if args.cmd == "sync":
        if args.daemon:
            stats = run_daemon(dbp, spool, args.poll, args.idle_timeout)
        else:
            stats = process_once(dbp, spool)
        print(json.dumps({"applied": stats.applied,
                          "duplicates": stats.duplicates,
                          "deadlettered": stats.deadlettered},
                         ensure_ascii=False))
        return 0

    if args.cmd == "query":
        reader = db.open_reader(dbp)
        counter = install_progress_handler(reader)
        try:
            res = point_query(reader, args.point, args.lo, args.hi)
        finally:
            callbacks = counter.n
            reader.close()
        payload = {
            "point": res.point, "window": [res.lo, res.hi],
            "prefix_seq": res.prefix_seq, "count": res.count,
            "peak_amplitude_nm": res.peak_nm,
            "peak_obs_id": res.peak_obs_id,
            "progress_callbacks": callbacks,
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"测点 {res.point}  时段 [{res.lo}, {res.hi})")
            print(f"  同步前缀        : seq {res.prefix_seq}")
            print(f"  观测数          : {res.count}")
            print(f"  峰值振幅(nm)    : {res.peak_nm}")
            print(f"  峰值观测编号    : {res.peak_obs_id}（并列取最小）")
            print(f"  进度回调次数    : {callbacks}（步长 1）")
        return 0

    if args.cmd == "verify":
        ok = verify.run(args.data)
        return 0 if ok else 1

    parser.error(f"未知子命令 {args.cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
