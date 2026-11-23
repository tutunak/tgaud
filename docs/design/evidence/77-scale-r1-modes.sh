#!/bin/sh
# R1 in both modes on the scale data: full (set-based) first build, then the normal per-chat run; the rows of
# the recomputed days must be the same in both modes (checksum without refreshed_at).
cd "$(dirname "$0")"
export DB=audscale
NOW="--set=as_of=2026-11-23 18:00+00"
EX="--set=explain=EXPLAIN (ANALYZE, BUFFERS)"
q() { name=$1; shift; echo "### $name" ; ./psql-tenant.sh -q -P pager=off -c '\timing on' "$@" -f /work/queries.sql 2>&1 | grep -v '^as_of '; }
SUM="select count(*), md5(string_agg(row(chat_id, day, joins, rejoins, exits, kicks, bans_from_out, net, short_stays, corrections_in, corrections_out, reported_eod, known_eod, messages, active_users, members_started, members_ended, unsure_started, unsure_ended)::text, ',' order by chat_id, day)) from stats.chat_day where day >= '2026-11-21'"
{
./psql-tenant.sh -q -c 'TRUNCATE stats.chat_day'
q "R1 first build, full mode (set-based)"  "$NOW" -v only=R1 "$EX"
echo "### checksum of 2026-11-21.. after the full build"; ./psql-tenant.sh -At -c "$SUM"
q "R1 default run, per-chat mode (lookback 2 days)" "$NOW" -v only=R1 "$EX"
echo "### checksum of 2026-11-21.. after the per-chat run"; ./psql-tenant.sh -At -c "$SUM"
q "16a as-of counts, 1M chat, D before the bot" "$NOW" -v only=16a -v chat=-1003000000001 "--set=day=2025-10-01 00:00+00" "$EX"
} > out-scale-r1-modes.txt 2>&1
