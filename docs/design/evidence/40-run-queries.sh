#!/bin/sh
# Run every block of queries.sql on the fixture with the parameters verify.md uses.
P="$(dirname "$0")/psql-tenant.sh"
NOW="--set=as_of=2026-11-23 12:00+00"
AD="--set=scope={-1001000000001,-1001000000002,-1001000000003,-1001000000004}"
run() { "$P" -q -P pager=off "$@" -f /work/queries.sql 2>&1 | grep -v '^as_of '; }
run "$NOW" -v only=R1                        # chat_day, first build
run "$NOW" -v only=R2                        # first build: plain REFRESH
run "$NOW" -v only=R2                        # second run: REFRESH ... CONCURRENTLY inside the transaction
run "$NOW" -v only=R3
run "$NOW" -v only=1 "$AD"
run "$NOW" -v only=2a
run "$NOW" -v only=2a "$AD"
run "$NOW" -v only=2b -v user=1001
run "$NOW" -v only=2b -v user=1006
run "$NOW" -v only=3 "$AD"
run "$NOW" -v only=3 "--set=scope={-1001000000001,-1001000000002}"
run "$NOW" -v only=4
run "$NOW" -v only=5 "$AD" -v since=2026-08-01
run "$NOW" -v only=6 "$AD" -v since=2026-08-01 -v burst_users=2
run "$NOW" -v only=7a "$AD"
run "$NOW" -v only=7b "--set=scope={-1001000000001,-1001000000002}" -v since=2026-08-01
run "$NOW" -v only=8 "--set=scope={-1001000000001}" -v since=2026-08-01
run "$NOW" -v only=9a "--set=scope={-1001000000001,-1001000000002}" -v since=2026-08-01
run "$NOW" -v only=9b "$AD"
run "$NOW" -v only=10 -v since=2026-08-01
run "$NOW" -v only=11
run "$NOW" -v only=12 -v since=2026-08-01
run "$NOW" -v only=13 -v raid_min_joins=5
run "$NOW" -v only=14 "$AD"
run "$NOW" -v only=15 "$AD"
run "$NOW" -v only=16a -v chat=-1001000000001 "--set=day=2026-11-07 00:00+00"
run "$NOW" -v only=16a -v chat=-1001000000001 "--set=day=2026-07-15 00:00+00"
run "$NOW" -v only=16a -v chat=-1001000000002 "--set=day=2026-11-07 00:00+00"
run "$NOW" -v only=16b -v chat=-1001000000002 "--set=day=2026-11-07 00:00+00"
run "$NOW" -v only=16c "$AD" "--set=day=2026-11-07 00:00+00"
run "$NOW" -v only=16d "$AD" "--set=day=2026-11-07 00:00+00"
run "$NOW" -v only=16a -v chat=-1001000000001 "--set=day=2026-11-08 00:00+00"
run "$NOW" -v only=16a -v chat=-1001000000002 "--set=day=2026-11-08 00:00+00"
