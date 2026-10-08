"""Admin CLI: pair | update | list | revoke | check.
Usage: uv run python scripts/consumers.py pair --id research --scopes podcast,report,list,read --label "To Review"
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from nlm_gateway.consumers import Registry


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default=os.environ.get("NLM_STATE_DIR", "state"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pair")
    p.add_argument("--id", required=True)
    p.add_argument("--scopes", default="podcast,report")
    p.add_argument("--label", default="To Review", help='default notebook name (exact NotebookLM title), default "To Review"')
    p.add_argument("--audio-cap", type=int, default=3)
    p.add_argument("--report-cap", type=int, default=10)
    p.add_argument("--note", default="")
    u = sub.add_parser("update", help="change fields; the token is kept")
    u.add_argument("--id", required=True)
    u.add_argument("--scopes", help="comma-separated: podcast,report,list,read,studio,notes")
    u.add_argument("--label", help="default notebook name (exact NotebookLM title)")
    u.add_argument("--audio-cap", type=int)
    u.add_argument("--report-cap", type=int)
    r = sub.add_parser("revoke")
    r.add_argument("--id", required=True)
    sub.add_parser("list")
    sub.add_parser("check")
    a = ap.parse_args(argv)
    reg = Registry(Path(a.state_dir) / "consumers.json")
    if a.cmd == "pair":
        token = reg.pair(a.id, a.scopes.split(","), a.label, a.audio_cap, a.report_cap, a.note)
        print("Token (shown once; give it to the client as 'Authorization: Bearer <token>'):")
        print(token)
    elif a.cmd == "update":
        ok = reg.update(a.id, scopes=a.scopes.split(",") if a.scopes else None, notebook_label=a.label,
                        daily_audio_cap=a.audio_cap, daily_report_cap=a.report_cap)
        print("updated" if ok else "no such consumer")
        return 0 if ok else 1
    elif a.cmd == "revoke":
        print("revoked" if reg.revoke(a.id) else "no such consumer")
    elif a.cmd == "list":
        for c in reg.load():
            print(f"{c.id}\tscopes={','.join(sorted(c.scopes))}\tlabel={c.notebook_label}\taudio/day={c.daily_audio_cap}\treport/day={c.daily_report_cap}\tcreated={c.created_at}\t{c.note}")
    elif a.cmd == "check":
        n = len(reg.load())
        print(f"{n} consumer(s)")
        return 0 if n else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
