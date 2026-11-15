#!/usr/bin/env python3
"""Event-sourced daily inventory projections over movement_log.jsonl.

Ordering semantics, derived from the meaning of the two timestamp columns:
  - biz_time   : the business moment -> decides which business DAY a record
                 belongs to (daily attribution always uses biz_time's date).
  - append_time: the arrival moment into the log -> the only trustworthy total
                 order. Used to sequence records as they are applied, including
                 ties on biz_time (same SKU, same biz_time): those are ordered
                 by (append_time, doc).

Late arrival: the projection's watermark is the ARRIVAL day (append_time's
date) — day d is closed once the arrival stream moves past d. A record whose
biz day is earlier than the current watermark is late: it is written back into
its biz day and every later day of the affected key is recomputed; each such
recompute increments the projection version, and the version is stamped on
every recomputed row. (A record whose biz day is later than its append day —
clock skew — is not late; its delta simply becomes visible when the arrival
stream reaches its biz day.)

Outputs (written next to the input file unless --out is given):
  by_wh.csv       warehouse x day closing projection
  by_sku.csv      SKU x day closing projection
  divergence.txt  per-day cross-check of the two projections' totals,
                  with SKU-level rows for any day whose totals disagree
"""

import argparse
import csv
import json
import os
import sys


def load_events(path):
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    # Arrival order = the log's own order: append_time, tie-broken by doc id.
    events.sort(key=lambda e: (e["append_time"], e["doc"]))
    return events


class Projection:
    """A daily-closing projection keyed by `keyfn`, fed events in append order."""

    def __init__(self, name, keyfn, days):
        self.name = name
        self.keyfn = keyfn
        self.days = days               # all business days, sorted
        self.version = 1               # bumped on every late-arrival recompute
        self.hwm = None                # high-water biz day already published
        self.delta = {}                # key -> {day: [in_qty, out_qty]}
        self.first = {}                # key -> first biz day with activity
        self.closing = {}              # key -> {day: closing balance}
        self.rowver = {}               # key -> {day: projection version when computed}
        self.recomputes = []           # (new_version, key, from_day, doc)

    def _add(self, key, day, qty):
        slot = self.delta.setdefault(key, {}).setdefault(day, [0, 0])
        if qty >= 0:
            slot[0] += qty
        else:
            slot[1] += -qty
        if key not in self.first or day < self.first[key]:
            self.first[key] = day

    def _net(self, key, day):
        io = self.delta.get(key, {}).get(day)
        return io[0] - io[1] if io else 0

    def _recalc(self, key, frm, ver):
        """Recompute closings for `key` over its published range; rows at or
        after `frm` are stamped with version `ver`."""
        run = 0
        for d in self.days:
            if d < self.first[key] or d > self.hwm:
                continue
            run += self._net(key, d)
            self.closing.setdefault(key, {})[d] = run
            rv = self.rowver.setdefault(key, {})
            if d >= frm or d not in rv:
                rv[d] = ver

    def apply(self, ev):
        key = self.keyfn(ev)
        day = ev["biz_time"][:10]
        aday = ev["append_time"][:10]
        qty = ev["qty"]
        # Advance the arrival watermark; on rollover, extend every known
        # key's rows to the new day.
        if self.hwm is None:
            self.hwm = aday
        elif aday > self.hwm:
            self.hwm = aday
            for k2 in self.first:
                self._recalc(k2, aday, self.version)
        late = day < self.hwm
        self._add(key, day, qty)
        if late:
            # Late record: back to its biz day, recompute it and all later
            # days of this key; the projection version increments.
            self.version += 1
            self._recalc(key, day, self.version)
            self.recomputes.append((self.version, key, day, ev["doc"]))
        else:
            self._recalc(key, day, self.version)

    def rows(self):
        for key in sorted(self.first):
            for d in self.days:
                if d < self.first[key] or d > self.hwm:
                    continue
                io = self.delta.get(key, {}).get(d, [0, 0])
                yield (key, d, io[0], io[1], io[0] - io[1],
                       self.closing[key][d], self.rowver[key][d])


def write_csv(path, key_col, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([key_col, "date", "in_qty", "out_qty", "net_qty",
                    "closing_qty", "version"])
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("log", nargs="?", default="movement_log.jsonl")
    ap.add_argument("--out", default=None,
                    help="output directory (default: alongside the log)")
    args = ap.parse_args()

    events = load_events(args.log)
    days = sorted({e["biz_time"][:10] for e in events})
    out_dir = args.out or os.path.dirname(os.path.abspath(args.log))
    os.makedirs(out_dir, exist_ok=True)

    by_wh = Projection("by_wh", lambda e: e["wh"], days)
    by_sku = Projection("by_sku", lambda e: e["sku"], days)
    for ev in events:
        by_wh.apply(ev)
        by_sku.apply(ev)

    wh_rows = list(by_wh.rows())
    sku_rows = list(by_sku.rows())
    wh_csv = os.path.join(out_dir, "by_wh.csv")
    sku_csv = os.path.join(out_dir, "by_sku.csv")
    write_csv(wh_csv, "wh", wh_rows)
    write_csv(sku_csv, "sku", sku_rows)

    # ---- divergence: cross-check the two projections' daily totals --------
    def totals(rows):
        net, clo = {}, {}
        for _, d, i, o, n, c, _v in rows:
            net[d] = net.get(d, 0) + n
            clo[d] = clo.get(d, 0) + c
        return net, clo

    wh_net, wh_clo = totals(wh_rows)
    sku_net, sku_clo = totals(sku_rows)

    # Reference per-SKU per-day nets straight from the log, used to localize
    # any nonzero divergence down to SKU level.
    log_net = {}
    for e in events:
        k = (e["biz_time"][:10], e["sku"])
        log_net[k] = log_net.get(k, 0) + e["qty"]
    sku_net_by_sku = {}
    for sku, d, i, o, n, c, _v in sku_rows:
        sku_net_by_sku[(d, sku)] = n

    div_path = os.path.join(out_dir, "divergence.txt")
    max_net_diff = max_clo_diff = 0
    detail = []
    lines = [
        "divergence report: by_wh vs by_sku daily totals",
        f"source: {os.path.abspath(args.log)} ({len(events)} docs, "
        f"{len(days)} business days)",
        "date       | net_by_wh | net_by_sku | net_diff | "
        "closing_by_wh | closing_by_sku | closing_diff",
    ]
    for d in days:
        nd = wh_net.get(d, 0) - sku_net.get(d, 0)
        cd = wh_clo.get(d, 0) - sku_clo.get(d, 0)
        max_net_diff = max(max_net_diff, abs(nd))
        max_clo_diff = max(max_clo_diff, abs(cd))
        lines.append(f"{d} | {wh_net.get(d, 0):>9} | {sku_net.get(d, 0):>10} "
                     f"| {nd:>8} | {wh_clo.get(d, 0):>13} "
                     f"| {sku_clo.get(d, 0):>14} | {cd:>12}")
        if nd or cd:
            for sku in sorted({s for (dd, s) in log_net if dd == d}):
                a = sku_net_by_sku.get((d, sku), 0)
                b = log_net.get((d, sku), 0)
                if a != b:
                    detail.append(
                        f"  {d} {sku}: by_sku_net={a} log_net={b} diff={a-b}")
    lines.append("")
    if detail:
        lines.append("SKU-level diff rows for days with nonzero divergence:")
        lines.extend(detail)
    else:
        lines.append(f"max |net_diff| = {max_net_diff}, "
                     f"max |closing_diff| = {max_clo_diff}: "
                     f"the two projections reconcile on all {len(days)} days; "
                     "no SKU-level diff rows.")
    with open(div_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # ---- console summary ---------------------------------------------------
    late_docs = [e for e in events
                 if e["append_time"][:10] > e["biz_time"][:10]]
    print(f"docs={len(events)} days={len(days)} "
          f"late_docs={len(late_docs)}")
    for p in (by_wh, by_sku):
        print(f"{p.name}: version={p.version} recomputes={len(p.recomputes)}")
        for ver, key, frm, doc in p.recomputes:
            print(f"  v{ver} <- {doc} late into {frm} (key={key})")
    print(f"wrote {wh_csv}, {sku_csv}, {div_path}")
    print(f"divergence: max|net|={max_net_diff} max|closing|={max_clo_diff}")


if __name__ == "__main__":
    sys.exit(main())
