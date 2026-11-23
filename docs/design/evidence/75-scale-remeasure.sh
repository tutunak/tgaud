#!/bin/sh
# Re-measure after the redesign (chat_set, chat_day member deltas, LATERAL R1). Writes out-scale-remeasure.txt.
cd "$(dirname "$0")"
export DB=audscale
NOW="--set=as_of=2026-11-23 18:00+00"
EX="--set=explain=EXPLAIN (ANALYZE, BUFFERS)"
q() { name=$1; shift; echo "### $name" ; ./psql-tenant.sh -q -P pager=off -c '\timing on' "$@" -f /work/queries.sql 2>&1 | grep -v '^as_of '; }
{
./psql-tenant.sh -q -P pager=off -f /work/evidence/66-scale-migrate.sql 2>&1   # 65 was applied by the first (stopped) run
q "R2 first build (plain REFRESH, chat_set, overlap from chat_set)" "$NOW" -v only=R2 "$EX"
q "R1 first build (all days, 200 chats)"            "$NOW" -v only=R1
q "R1 one day (lookback 0)"                         "$NOW" -v only=R1 -v lookback_days=0 "$EX"
q "R1 default run (lookback 2 days)"                "$NOW" -v only=R1
q "R2 second run (REFRESH CONCURRENTLY)"           "$NOW" -v only=R2
q "16a as-of counts, 1M chat, D = 30 days ago"      "$NOW" -v only=16a -v chat=-1003000000001 "--set=day=2026-10-24 18:00+00" "$EX"
q "16c overlap at a past date, all 200 chats"       "$NOW" -v only=16c "--set=day=2026-10-24 18:00+00" "$EX"
q "2a chats per user, all tracked"                  "$NOW" -v only=2a "$EX"
q "2a chats per user, scope of 10 chats"            "$NOW" -v only=2a "--set=scope={-1003000000001,-1003000000002,-1003000000003,-1003000000004,-1003000000005,-1003000000006,-1003000000007,-1003000000008,-1003000000009,-1003000000010}" "$EX"
q "3 unique per chat, all tracked"                  "$NOW" -v only=3 "$EX"
q "3 unique per chat, scope of 10 chats"            "$NOW" -v only=3 "--set=scope={-1003000000001,-1003000000002,-1003000000003,-1003000000004,-1003000000005,-1003000000006,-1003000000007,-1003000000008,-1003000000009,-1003000000010}" "$EX"
q "11 channel vs discussion group, all"             "$NOW" -v only=11 "$EX"
q "16d members at end of day, all 200 chats"        "$NOW" -v only=16d "--set=day=2026-10-24 18:00+00" "$EX"
q "1 overlap matrix read, all pairs"                "$NOW" -v only=1 "$EX"
} > out-scale-remeasure.txt 2>&1
DB=audscale ./psql-tenant.sh -P pager=off -c "select count(*) as chat_sets, sum(users) as people from tg.chat_set" \
  -c "select chat_id, members, exclusive from stats.unique_per_chat order by members desc limit 3" \
  -c "select chat_count, users from stats.chats_per_user order by chat_count limit 12" \
  -c "select d.chat_id, d.members_eod, d.unsure_eod from stats.v_chat_day d where d.chat_id = -1003000000001 and d.day = '2026-10-23'" >> out-scale-remeasure.txt 2>&1
