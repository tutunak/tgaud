#!/bin/sh
# R1 after merging its scans (2 passes over membership instead of 4), and item 11 from the pair matrix.
cd "$(dirname "$0")"
export DB=audscale
NOW="--set=as_of=2026-11-23 18:00+00"
EX="--set=explain=EXPLAIN (ANALYZE, BUFFERS)"
q() { name=$1; shift; echo "### $name" ; ./psql-tenant.sh -q -P pager=off -c '\timing on' "$@" -f /work/queries.sql 2>&1 | grep -v '^as_of '; }
{
./psql-tenant.sh -q -c 'TRUNCATE stats.chat_day'
q "R1 first build (all days, 200 chats)"   "$NOW" -v only=R1
q "R1 one day (lookback 0)"                "$NOW" -v only=R1 -v lookback_days=0 "$EX"
q "R1 default run (lookback 2 days)"       "$NOW" -v only=R1
q "11 channel vs discussion group, all"    "$NOW" -v only=11 "$EX"
q "16d members at end of day, all 200 chats" "$NOW" -v only=16d "--set=day=2026-10-24 18:00+00"
} > out-scale-r1.txt 2>&1
