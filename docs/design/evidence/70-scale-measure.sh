#!/bin/sh
# Measure the heavy queries on the scale database as the tenant, with the server's limits
# (512 MB container, shared_buffers 64MB, work_mem 2MB, no parallel workers). Writes out-scale-*.txt.
cd "$(dirname "$0")"
export DB=audscale
NOW="--set=as_of=2026-11-23 18:00+00"
EX="--set=explain=EXPLAIN (ANALYZE, BUFFERS)"
q() { name=$1; shift; echo "### $name" ; ./psql-tenant.sh -q -P pager=off -c '\timing on' "$@" -f /work/queries.sql 2>&1 | grep -v '^as_of '; }
{
q "R2 first build (plain REFRESH + overlap)"        "$NOW" -v only=R2 "$EX"
q "R2 second run (REFRESH CONCURRENTLY + overlap)"  "$NOW" -v only=R2
q "R1 first build (all days, 200 chats)"            "$NOW" -v only=R1
q "R1 one day (lookback 0)"                         "$NOW" -v only=R1 -v lookback_days=0 "$EX"
q "2a chats per user, all tracked"                  "$NOW" -v only=2a "$EX"
q "2b user lookup"                                  "$NOW" -v only=2b -v user=10000042 "$EX"
q "3 unique per chat, all tracked"                  "$NOW" -v only=3 "$EX"
q "3 unique per chat, scope of 10 chats"            "$NOW" -v only=3 "--set=scope={-1003000000001,-1003000000002,-1003000000003,-1003000000004,-1003000000005,-1003000000006,-1003000000007,-1003000000008,-1003000000009,-1003000000010}" "$EX"
q "4a giveaway report, 1M chat"                     "$NOW" -v only=4a -v campaign=1 "$EX"
q "4b giveaway per person, 1M chat"                 "$NOW" -v only=4b -v campaign=1 "$EX"
q "16a as-of counts, 1M chat, D = 30 days ago"      "$NOW" -v only=16a -v chat=-1003000000001 "--set=day=2026-10-24 18:00+00" "$EX"
q "16a as-of counts, 1M chat, D before the bot"     "$NOW" -v only=16a -v chat=-1003000000001 "--set=day=2025-10-01 00:00+00" "$EX"
q "16b as-of list, 1M chat, D = 30 days ago"        "$NOW" -v only=16b -v chat=-1003000000001 "--set=day=2026-10-24 18:00+00" "$EX"
q "16c overlap at a past date, all 200 chats"       "$NOW" -v only=16c "--set=day=2026-10-24 18:00+00" "$EX"
} > out-scale-measure.txt 2>&1
