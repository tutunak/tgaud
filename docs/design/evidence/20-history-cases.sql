-- The spells of the history-case chats H01..H14, as the fixture wrote them by hand (compare with verify.md 2).
\pset footer off
SELECT right(m.chat_id::text, 2) AS "case", m.user_id AS "user", m.start_source AS start, m.joined_at, m.known_from,
       m.joined_hint::date AS hint, m.last_confirmed_at AS last_in, m.suspect_since AS suspect,
       m.end_source AS "end", m.end_reason AS reason, m.left_at, m.left_after, m.left_before
FROM tg.membership m WHERE m.chat_id BETWEEN -1002000000014 AND -1002000000001
ORDER BY m.chat_id DESC, m.user_id, m.known_from;
SELECT right(e.chat_id::text, 2) AS "case", e.event_id, e.user_id AS "user", e.event_at, e.old_status || coalesce('(' || e.old_is_member || ')', '') AS old,
       e.new_status || coalesce('(' || e.new_is_member || ')', '') AS new, e.kind, e.source_chat_id
FROM tg.member_event e WHERE e.chat_id BETWEEN -1002000000014 AND -1002000000001 ORDER BY e.chat_id DESC, e.event_at, e.event_id;
SELECT * FROM tg.chat_alias;
SELECT chat_id, period FROM tg.coverage WHERE chat_id = -1002000000013 ORDER BY period;
