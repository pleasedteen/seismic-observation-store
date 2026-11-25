"""SQLite 独立数据仓库：连接管理、表结构、同步写入引擎与层级 rollup。

设计要点
========
1. 每个源端提交在*一个* SQLite 事务内完成（“每笔提交整体可见”），
   提交序号写入 meta 并与数据同事务持久化；强制 kill 后已 COMMIT 的提交
   不受影响（WAL 自动恢复），重启从 seq 续传，重复提交直接跳过。
2. 大提交流式处理：事件逐行读、逐行小批量执行，引擎内的可变状态只有
   两个“脏叶子桶”集合，与提交大小无关，不整体装入内存。
3. 观测当前态表 ``fact`` 只保留每个 obs_id 的最新状态；所有变更事件的
   先后由同步引擎严格按 (seq, off) 喂入，天然满足 last-write-wins。
4. 接线是双时态记录：业务有效时间区间 + 来源提交定位 (seq, off)；
   重叠区间以后生效（seq/off 更大）的源端变更为准。
5. ``rollup`` 是 64 叉时间桶层级表，查询把任意 [lo, hi) 分解成少量
   完整桶 + 至多两个叶子残区，避免大范围扫描（进度回调预算的关键）。
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from .logfmt import CommitInfo, amplitude_to_picometers

# ---- rollup 几何：叶子桶宽 W0，每级 64 叉 --------------------------------
W0 = 64
BRANCH = 64

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wiring(
    seq        INTEGER NOT NULL,
    off        INTEGER NOT NULL,
    channel    TEXT NOT NULL,
    valid_from INTEGER NOT NULL,
    valid_to   INTEGER,
    point      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wiring_lookup
    ON wiring(channel, valid_from);
CREATE TABLE IF NOT EXISTS fact(
    obs_id  INTEGER PRIMARY KEY,
    time_ns INTEGER NOT NULL,
    channel TEXT NOT NULL,
    point   TEXT NOT NULL,
    amp_pm  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_point_time
    ON fact(point, time_ns);
CREATE INDEX IF NOT EXISTS idx_fact_channel_time
    ON fact(channel, time_ns);
CREATE TABLE IF NOT EXISTS rollup(
    point   TEXT NOT NULL,
    level   INTEGER NOT NULL,
    bucket  INTEGER NOT NULL,
    cnt     INTEGER NOT NULL,
    max_pm  INTEGER NOT NULL,
    max_obs INTEGER NOT NULL,
    PRIMARY KEY(point, level, bucket)
) WITHOUT ROWID;
"""


def open_writer(db_path: str) -> sqlite3.Connection:
    """打开同步进程使用的读写连接（WAL，长时间存活）。"""
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-262144")  # 256 MiB 页缓存
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def open_reader(db_path: str) -> sqlite3.Connection:
    """打开查询连接。只读 URI 打开，绝不写库；WAL 下不阻塞写入。"""
    conn = sqlite3.connect(
        f"file:{db_path}?mode=ro", uri=True, timeout=30, isolation_level=None
    )
    conn.execute("PRAGMA query_only=ON")
    return conn


def get_progress(conn: sqlite3.Connection) -> int:
    """已持久化的连续提交前缀序号（meta 中 last_seq，无则 0）。"""
    row = conn.execute(
        "SELECT value FROM meta WHERE key='last_seq'"
    ).fetchone()
    return int(row[0]) if row else 0


def bucket_at(time_ns: int, level: int) -> int:
    """time_ns 在 level 级对应的桶号（level 0 桶宽 W0，每升一级 ×BRANCH）。"""
    width = W0 * BRANCH ** level
    return time_ns // width


class SyncEngine:
    """单提交事务内驱动的写入引擎；实例生命周期 = 一个提交。"""

    # 脏叶子超过该数量则整条层级用集合式 SQL 重建（初始大装载走这条），
    # 否则只做受影响节点的定向重算。
    BULK_LEAF_THRESHOLD = 20_000

    def __init__(self, conn: sqlite3.Connection, info: CommitInfo):
        self.conn = conn
        self.info = info
        self._wire_cache: dict[str, list[tuple]] = {}
        # 受影响的叶子桶：(point, bucket0)；set 内存与提交大小无关地受控
        self.dirty: set[tuple[str, int]] = set()

    # ---- 事务边界 ---------------------------------------------------------
    def __enter__(self) -> "SyncEngine":
        self.conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._rebuild_rollups()
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES('last_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(self.info.seq),),
            )
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")

    # ---- 接线事件 ---------------------------------------------------------
    def apply_wire(self, event: dict) -> None:
        channel = str(event["channel"])
        valid_to = event.get("valid_to")
        self.conn.execute(
            "INSERT INTO wiring(seq, off, channel, valid_from, valid_to, point) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (self.info.seq, int(event["off"]), channel,
             int(event["valid_from"]),
             None if valid_to is None else int(valid_to),
             str(event["point"])),
        )
        # 该通道在受影响时间范围内的现存观测归属可能改变。
        lo = int(event["valid_from"])
        # 游标 + fetchmany 分批：即使整通道有数十万观测也不整体装入内存；
        # 该循环内没有对 fact 的同连接写操作（先收集再更新），游标安全。
        cur = self.conn.execute(
            "SELECT obs_id, time_ns, point FROM fact "
            "WHERE channel=? AND time_ns >= ? ORDER BY time_ns",
            (channel, lo),
        )
        self._wire_cache.pop(channel, None)
        pending: list[tuple[int, str]] = []
        while batch := cur.fetchmany(10_000):
            for obs_id, time_ns, old_point in batch:
                new_point = self.resolve_point(channel, time_ns)
                if new_point is not None and new_point != old_point:
                    self._mark(old_point, time_ns)
                    pending.append((obs_id, new_point))
                    self._mark(new_point, time_ns)
        for obs_id, new_point in pending:
            self.conn.execute(
                "UPDATE fact SET point=? WHERE obs_id=?",
                (new_point, obs_id),
            )

    # ---- 观测 upsert / retract -------------------------------------------
    def apply_obs(self, event: dict) -> None:
        obs_id = int(event["obs_id"])
        time_ns = int(event["time"])
        channel = str(event["channel"])
        amp_pm = amplitude_to_picometers(
            str(event["amp_value"]), str(event["amp_unit"])
        )
        point = self.resolve_point(channel, time_ns)
        if point is None:
            # 源端观测先于接线到达：保留事实，point 暂记空串；
            # 后续接线补录事件会重新归属（apply_wire 主动重扫）。
            point = ""
        old = self.conn.execute(
            "SELECT time_ns, point FROM fact WHERE obs_id=?", (obs_id,)
        ).fetchone()
        if old is not None:
            self._mark(old[1], old[0])
        self.conn.execute(
            "INSERT INTO fact(obs_id, time_ns, channel, point, amp_pm) "
            "VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(obs_id) DO UPDATE SET "
            "time_ns=excluded.time_ns, channel=excluded.channel, "
            "point=excluded.point, amp_pm=excluded.amp_pm",
            (obs_id, time_ns, channel, point, amp_pm),
        )
        self._mark(point, time_ns)

    def apply_retract(self, event: dict) -> None:
        obs_id = int(event["obs_id"])
        old = self.conn.execute(
            "SELECT time_ns, point FROM fact WHERE obs_id=?", (obs_id,)
        ).fetchone()
        if old is not None:
            self._mark(old[1], old[0])
            self.conn.execute("DELETE FROM fact WHERE obs_id=?", (obs_id,))

    # ---- 归属解析：重叠区间以后生效 (seq, off) 的源端变更为准 -------------
    def resolve_point(self, channel: str, time_ns: int) -> str | None:
        wires = self._wire_cache.get(channel)
        if wires is None:
            wires = self.conn.execute(
                "SELECT valid_from, valid_to, point, seq, off "
                "FROM wiring WHERE channel=? ORDER BY valid_from",
                (channel,),
            ).fetchall()
            self._wire_cache[channel] = wires
        best: tuple[int, int] | None = None
        best_point: str | None = None
        for valid_from, valid_to, point, seq, off in wires:
            if time_ns < valid_from:
                break
            if valid_to is not None and time_ns >= valid_to:
                continue
            key = (seq, off)
            if best is None or key > best:
                best = key
                best_point = point
        return best_point

    # ---- rollup 维护 ------------------------------------------------------
    def _mark(self, point: str, time_ns: int) -> None:
        self.dirty.add((point, bucket_at(time_ns, 0)))

    def _rebuild_rollups(self) -> None:
        if not self.dirty:
            return
        if len(self.dirty) >= self.BULK_LEAF_THRESHOLD:
            self._bulk_rebuild()
        else:
            self._targeted_recompute()

    def _targeted_recompute(self) -> None:
        conn = self.conn
        # 1) 叶子级：用 fact 重算每个脏桶
        for point, b0 in sorted(self.dirty):
            lo = b0 * W0
            hi = lo + W0
            row = conn.execute(
                "SELECT COUNT(*), MAX(amp_pm), "
                "  MIN(CASE WHEN amp_pm=(SELECT MAX(amp_pm) FROM fact "
                "            WHERE point=? AND time_ns>=? AND time_ns<?) "
                "       THEN obs_id END) "
                "FROM fact WHERE point=? AND time_ns>=? AND time_ns<?",
                (point, lo, hi, point, lo, hi),
            ).fetchone()
            self._upsert_node(point, 0, b0, row)
        # 2) 自底向上传播：父桶只需读子级 rollup。根桶号为 0 时
        # 0 // BRANCH == 0，集合不再收缩，以“父子集合相同”为终止条件。
        level = 1
        current: set[tuple[str, int]] = self.dirty
        while current:
            parents = {(p, b // BRANCH) for p, b in current}
            if parents == current:
                break
            for point, pb in sorted(parents):
                child_lo = pb * BRANCH
                child_hi = child_lo + BRANCH
                row = conn.execute(
                    "SELECT COALESCE(SUM(cnt),0), MAX(max_pm), "
                    "  MIN(CASE WHEN max_pm=(SELECT MAX(max_pm) FROM rollup "
                    "            WHERE point=? AND level=? "
                    "              AND bucket BETWEEN ? AND ?) "
                    "       THEN max_obs END) "
                    "FROM rollup WHERE point=? AND level=? "
                    "  AND bucket BETWEEN ? AND ?",
                    (point, level - 1, child_lo, child_hi - 1,
                     point, level - 1, child_lo, child_hi - 1),
                ).fetchone()
                self._upsert_node(point, level, pb, row)
            current = parents
            level += 1

    def _upsert_node(self, point: str, level: int, bucket: int,
                     row: tuple) -> None:
        cnt, max_pm, max_obs = row
        if not cnt:
            self.conn.execute(
                "DELETE FROM rollup WHERE point=? AND level=? AND bucket=?",
                (point, level, bucket),
            )
            return
        self.conn.execute(
            "INSERT INTO rollup(point, level, bucket, cnt, max_pm, max_obs) "
            "VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(point, level, bucket) DO UPDATE SET "
            "cnt=excluded.cnt, max_pm=excluded.max_pm, max_obs=excluded.max_obs",
            (point, level, bucket, int(cnt), int(max_pm), int(max_obs)),
        )

    def _bulk_rebuild(self) -> None:
        """脏叶子很多时（初始装载/大规模改归），用集合式 SQL 整层重建。

        峰值并列取最小 obs_id 用窗口函数在*分组内*比较（MAX() OVER 分区），
        避免跨分组/跨测点关联造成 NULL。
        """
        conn = self.conn
        points = sorted({p for p, _ in self.dirty})
        marks = ",".join("?" * len(points))
        conn.execute(
            f"DELETE FROM rollup WHERE point IN ({marks})", points
        )
        # level 0：直接按叶子桶分组；并列峰值取最小 obs_id。
        conn.execute(
            f"""
            INSERT INTO rollup(point, level, bucket, cnt, max_pm, max_obs)
            SELECT point, 0, bucket, cnt, max_pm, max_obs FROM (
                SELECT point, bucket,
                       COUNT(*) OVER w AS cnt,
                       MAX(amp_pm) OVER w AS max_pm,
                       MIN(CASE WHEN amp_pm = grp_max THEN obs_id END)
                           OVER w AS max_obs,
                       ROW_NUMBER() OVER w AS rn
                FROM (
                    SELECT point, time_ns / {W0} AS bucket, amp_pm, obs_id,
                           MAX(amp_pm) OVER (
                               PARTITION BY point, time_ns / {W0}) AS grp_max
                    FROM fact WHERE point IN ({marks})
                )
                WINDOW w AS (PARTITION BY point, bucket)
            ) WHERE rn = 1
            """,
            points,
        )
        level = 1
        while True:
            conn.execute(
                f"""
                INSERT INTO rollup(point, level, bucket, cnt, max_pm, max_obs)
                SELECT point, {level}, parent, cnt, max_pm, max_obs FROM (
                    SELECT point, parent,
                           SUM(cnt) OVER w AS cnt,
                           MAX(max_pm) OVER w AS max_pm,
                           MIN(CASE WHEN max_pm = grp_max THEN max_obs END)
                               OVER w AS max_obs,
                           ROW_NUMBER() OVER w AS rn
                    FROM (
                        SELECT point, bucket / {BRANCH} AS parent,
                               bucket, cnt, max_pm, max_obs,
                               MAX(max_pm) OVER (
                                   PARTITION BY point,
                                   bucket / {BRANCH}) AS grp_max
                        FROM rollup
                        WHERE level = {level - 1} AND point IN ({marks})
                    )
                    WINDOW w AS (PARTITION BY point, parent)
                ) WHERE rn = 1
                """,
                points,
            )
            top = conn.execute(
                "SELECT COUNT(*) FROM rollup WHERE level=? "
                f"AND point IN ({marks})",
                (level, *points),
            ).fetchone()[0]
            if top <= len(points):
                break
            level += 1


def iter_apply(engine: SyncEngine, events: Iterable[dict]) -> None:
    """把已校验的事件流按提交内顺序喂给引擎（不做整体缓存）。"""
    for event in events:
        kind = event["type"]
        if kind == "wire":
            engine.apply_wire(event)
        elif kind == "obs":
            engine.apply_obs(event)
        elif kind == "retract":
            engine.apply_retract(event)
        else:
            raise ValueError(f"未知事件类型: {kind!r}")
