-- origin: design schema.sql (brainstorm/final, 2026-11-23), copied verbatim below this line; migration 001 of tgaud
-- schema.sql: PostgreSQL 17 schema of the bot audience-statistics service (final design, 2026-11-23).
-- Load into an empty database:  psql -v ON_ERROR_STOP=1 -f schema.sql
--
-- Schema tg    = user-level data: the service, the CLI and the web UI read it. Personal data (D17).
-- Schema stats = aggregates only: rollup tables and views for BI, readable by role bi_reader.
--
-- Writers named in the table comments:
--   bot          the long-polling ingest process (one getUpdates stream, one transaction per update, D1)
--   reconciler   the same service: applies finished snapshots and probe answers, rebuilds spells (D3-D5)
--   prober       the same service: getChatMember calls from the probe queue (D5)
--   sampler      the same service: getChatMemberCount every 10 minutes per chat (D7)
--   collect      the owner's adapted MTProto `tgstats collect` script (the snapshot contract, design.md 5)
--   CLI          owner commands (campaigns, bot links, forget-user)
--   batch        the rollup job (chat_day, user_chats, overlap, unique, chats_per_user)
-- Chat ids are Bot API marked ids (-100... for channels and supergroups, -N for basic groups), the same
-- as telethon.utils.get_peer_id in collector.py. User ids are Telegram user ids. Times are timestamptz (UTC).

BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;  -- bigint equality inside GiST indexes and exclusion constraints

CREATE SCHEMA tg;
COMMENT ON SCHEMA tg IS 'User-level audience data (personal data). Service, CLI and web UI only; never BI.';
CREATE SCHEMA stats;
COMMENT ON SCHEMA stats IS 'Aggregates for BI: rollup tables and views without user ids. Role bi_reader.';

------------------------------------------------------------------------------------------------------------
-- Helper functions used by generated columns and reports
------------------------------------------------------------------------------------------------------------

-- "In" = creator, administrator, member, or restricted with is_member (D3). A restricted member whose
-- is_member is missing (the server omits fields outside supergroups, F§2) counts as in.
CREATE FUNCTION tg.status_in(p_status text, p_is_member boolean) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN p_status IN ('creator', 'administrator', 'member')
    OR (p_status = 'restricted' AND coalesce(p_is_member, true));

-- The kind of a chat_member transition, from old and new status (D3).
CREATE FUNCTION tg.event_kind(p_old_status text, p_old_is_member boolean,
                              p_new_status text, p_new_is_member boolean) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN CASE
  WHEN NOT tg.status_in(p_old_status, p_old_is_member) AND tg.status_in(p_new_status, p_new_is_member)
    THEN 'join'
  WHEN tg.status_in(p_old_status, p_old_is_member) AND NOT tg.status_in(p_new_status, p_new_is_member)
    THEN CASE p_new_status WHEN 'kicked' THEN 'kick' WHEN 'restricted' THEN 'restrict_out' ELSE 'leave' END
  WHEN NOT tg.status_in(p_old_status, p_old_is_member)   -- out -> out: moderation only, never a spell change
    THEN CASE WHEN p_new_status = 'kicked' AND p_old_status <> 'kicked' THEN 'ban_out'
              WHEN p_old_status = 'kicked' AND p_new_status <> 'kicked' THEN 'unban'
              ELSE 'out_change' END
  ELSE                                                    -- in -> in: role or restriction change
    CASE WHEN p_new_status = 'restricted' AND p_old_status <> 'restricted' THEN 'restrict'
         WHEN p_old_status = 'restricted' AND p_new_status <> 'restricted' THEN 'unrestrict'
         WHEN p_new_status IN ('administrator', 'creator') AND p_old_status NOT IN ('administrator', 'creator')
           THEN 'promote'
         WHEN p_old_status IN ('administrator', 'creator') AND p_new_status NOT IN ('administrator', 'creator')
           THEN 'demote'
         ELSE 'in_change' END
END;

-- Where a spell stands at time t: before | in | out | unknown. Exact ends have left_after = left_before.
-- Open spells are in, unless suspect since t or earlier. A migration end is censoring, never an exit.
CREATE FUNCTION tg.state_at(p_known_from timestamptz, p_left_after timestamptz, p_left_before timestamptz,
                            p_end_source text, p_suspect_since timestamptz, p_t timestamptz) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN CASE
  WHEN p_t < p_known_from THEN 'before'
  WHEN p_end_source IS NULL THEN CASE WHEN p_suspect_since <= p_t THEN 'unknown' ELSE 'in' END
  WHEN p_t < p_left_after THEN 'in'
  WHEN p_end_source = 'migration' THEN 'unknown'
  WHEN p_t >= p_left_before THEN 'out'
  ELSE 'unknown'
END;

------------------------------------------------------------------------------------------------------------
-- Ingest state
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.bot_state (
  bot_id               bigint PRIMARY KEY,                 -- the bot's own user id (getMe)
  next_offset          bigint NOT NULL DEFAULT 0,          -- offset for the next getUpdates = last update_id + 1
  last_update_id       bigint,
  last_update_at       timestamptz,                        -- date of the last committed update
  last_poll_ok_at      timestamptz,                        -- last getUpdates that returned without error
  allowed_updates      text[] NOT NULL,                    -- what the poller sends; checked at start (D1)
  updated_at           timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE tg.bot_state IS
  'The getUpdates offset. Written by the bot in the same transaction as each update (D1), so a crash '
  're-delivers only uncommitted updates.';

CREATE TABLE tg.update_receipt (
  update_id            bigint NOT NULL,
  update_date          timestamptz NOT NULL,               -- the date inside the update (ids can restart, F§1)
  update_type          text NOT NULL,
  chat_id              bigint,
  received_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (update_id, update_date)
) PARTITION BY RANGE (update_date);
COMMENT ON TABLE tg.update_receipt IS
  'Idempotency key of every processed update, all types (D1). Written by the bot; daily partitions kept '
  '8 days (24 h Telegram retention + the 7-day id restart, F§1).';

CREATE TABLE tg.raw_update (
  update_id            bigint NOT NULL,
  update_date          timestamptz NOT NULL,
  update_type          text NOT NULL CHECK (update_type IN ('chat_member', 'my_chat_member', 'chat_join_request')),
  chat_id              bigint NOT NULL,
  user_id              bigint,                             -- subject of the update, for forget-user
  received_at          timestamptz NOT NULL DEFAULT now(),
  payload              jsonb NOT NULL,                     -- the inner object (ChatMemberUpdated / ChatJoinRequest)
  PRIMARY KEY (update_id, update_date),
  CHECK (NOT (payload ? 'bio') AND NOT (payload ? 'user_chat_id'))   -- stripped before storing (D10, D14)
) PARTITION BY RANGE (update_date);
COMMENT ON TABLE tg.raw_update IS
  'Membership-type updates as received, for re-parsing after a parser fix (D14). Never messages. Written by '
  'the bot; daily partitions kept 30 days. Names are stripped when store_names is off.';

------------------------------------------------------------------------------------------------------------
-- Chats, coverage and gaps
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.chat (
  chat_id              bigint PRIMARY KEY,
  chat_type            text NOT NULL CHECK (chat_type IN ('group', 'supergroup', 'channel')),
  title                text NOT NULL,
  username             text,
  linked_chat_id       bigint,                             -- discussion group of a channel and back (getChat)
  is_forum             boolean,
  join_by_request      boolean,
  has_hidden_members   boolean,
  tracked              boolean NOT NULL DEFAULT false,     -- in reports and rollups
  bot_status           text NOT NULL DEFAULT 'unknown'
                       CHECK (bot_status IN ('creator', 'administrator', 'member', 'restricted', 'left',
                                             'kicked', 'unknown')),
  can_invite_users     boolean,                            -- needed for chat_join_request (F§3)
  can_restrict_members boolean,
  rights_checked_at    timestamptz,
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE tg.chat IS
  'Every chat the bot or the collect script knows. Written by the bot (getChat, my_chat_member), the collect '
  'script (inserts a missing row only) and the CLI (tracked).';

CREATE TABLE tg.chat_alias (
  old_chat_id          bigint PRIMARY KEY,                 -- the basic group id before migration
  chat_id              bigint NOT NULL REFERENCES tg.chat, -- the supergroup id that holds the history now
  migrated_at          timestamptz NOT NULL,               -- date of the migrate_to_chat_id service message
  moved_at             timestamptz NOT NULL DEFAULT now(), -- when tg.move_chat_history ran
  CHECK (old_chat_id <> chat_id)
);
COMMENT ON TABLE tg.chat_alias IS
  'Group to supergroup migration (D2): the old id maps to the new one, history is moved by '
  'tg.move_chat_history in one transaction. Written by the bot. Ingest resolves every chat id through it.';

CREATE TABLE tg.coverage (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  period               tstzrange NOT NULL,                 -- [admin since, admin until); open upper = still admin
  source               text NOT NULL DEFAULT 'my_chat_member' CHECK (source IN ('my_chat_member', 'rights_check')),
  EXCLUDE USING gist (chat_id WITH =, period WITH &&)
);
COMMENT ON TABLE tg.coverage IS
  'Periods the bot was admin in a chat, so chat_member updates could arrive (D6). Written by the bot from '
  'my_chat_member and the startup rights check.';

CREATE TABLE tg.gap (
  gap_id               bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint REFERENCES tg.chat ON UPDATE CASCADE,   -- NULL = every chat
  period               tstzrange NOT NULL,
  reason               text NOT NULL CHECK (reason IN ('service_down', 'not_admin', 'rights_lost',
                                                       'updates_not_allowed', 'contradiction')),
  confirmed            boolean NOT NULL DEFAULT true,      -- false = suspected (e.g. lag near 24 h)
  detected_at          timestamptz NOT NULL DEFAULT now(),
  note                 text
);
CREATE INDEX gap_period ON tg.gap USING gist (period);
COMMENT ON TABLE tg.gap IS
  'Periods with lost or doubtful events, with a reason (D6): service down longer than the 24 h retention, '
  'rights lost, chat_member not allowed, unexplained contradictions. Written by the bot and the reconciler. '
  'Every report that touches a time window checks it (tg.window_flags).';

------------------------------------------------------------------------------------------------------------
-- Users, links, campaigns
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.tg_user (
  user_id              bigint PRIMARY KEY,
  is_bot               boolean NOT NULL DEFAULT false,
  is_deleted           boolean,                            -- from snapshots only; NULL = unknown (D11)
  is_premium           boolean,                            -- true or NULL: the API sends it only when true (F§9)
  language_code        text,
  username             text,                               -- latest value; NULL when store_names is off
  first_name           text,
  last_name            text,
  first_seen_at        timestamptz NOT NULL,
  last_seen_at         timestamptz NOT NULL,
  CHECK (first_seen_at <= last_seen_at)
);
-- Bots and deleted accounts are few: reports drop them with NOT IN over this tiny index instead of joining 3M users.
CREATE INDEX tg_user_not_human ON tg.tg_user (user_id) WHERE is_bot OR is_deleted;
COMMENT ON TABLE tg.tg_user IS
  'One person across all chats (D11). Latest profile values only. Written by the bot (from any update) and '
  'the reconciler (from snapshot rows; is_deleted only from there).';

CREATE TABLE tg.campaign (
  campaign_id          bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  name                 text NOT NULL,
  kind                 text NOT NULL CHECK (kind IN ('giveaway', 'ad', 'tg_giveaway', 'other')),
  origin               text NOT NULL CHECK (origin IN ('annotation', 'observation')),
  starts_at            timestamptz NOT NULL,               -- entry window start (giveaway message date)
  draw_at              timestamptz NOT NULL,               -- entry window end (winners_selection_date)
  only_new_members     boolean NOT NULL DEFAULT false,
  winner_count         integer,
  post_chat_id         bigint,                             -- the Telegram giveaway message, when observed
  post_message_id      bigint,
  note                 text,
  created_at           timestamptz NOT NULL DEFAULT now(),
  CHECK (starts_at < draw_at),
  CHECK ((origin = 'observation') = (kind = 'tg_giveaway')),
  CHECK ((origin = 'observation') = (post_message_id IS NOT NULL)),
  UNIQUE (post_chat_id, post_message_id)
);
COMMENT ON TABLE tg.campaign IS
  'Giveaways and ads (D9). origin=annotation: entered by the owner with the CLI; origin=observation: parsed '
  'by the bot from message.giveaway / channel_post.giveaway. No winner lists in v1.';

CREATE TABLE tg.campaign_chat (
  campaign_id          bigint NOT NULL REFERENCES tg.campaign ON DELETE CASCADE,
  chat_id              bigint NOT NULL,                    -- no FK: a giveaway can require untracked chats
  PRIMARY KEY (campaign_id, chat_id)
);
COMMENT ON TABLE tg.campaign_chat IS
  'Chats a campaign covers (Giveaway.chats[] or the owner''s list). Written by the bot and the CLI.';

CREATE TABLE tg.invite_link (
  invite_link_id       bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  masked_link          text,                               -- other admins' links as received, with the "..." (F§4)
  link_digest          bytea,                              -- HMAC-SHA256 of a bot-made link; the token is never stored
  made_by_bot          boolean GENERATED ALWAYS AS (link_digest IS NOT NULL) STORED,
  creator_id           bigint,                             -- an admin, not an audience member
  name                 text,                               -- latest name; the campaign label in practice
  is_primary           boolean,
  creates_join_request boolean,
  subscription_price   integer,                            -- Stars per period, paid channel links
  campaign_id          bigint REFERENCES tg.campaign ON DELETE SET NULL,
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  last_seen_at         timestamptz NOT NULL DEFAULT now(),
  CHECK (num_nonnulls(masked_link, link_digest) = 1)
);
CREATE UNIQUE INDEX invite_link_bot ON tg.invite_link (chat_id, link_digest) WHERE link_digest IS NOT NULL;
CREATE UNIQUE INDEX invite_link_masked ON tg.invite_link (chat_id, masked_link, creator_id) NULLS NOT DISTINCT
  WHERE link_digest IS NULL;
COMMENT ON TABLE tg.invite_link IS
  'Join-source dimension (D8). Written by the bot (links seen in chat_member and chat_join_request) and the '
  'CLI (bot-made named links for a campaign, stored as an HMAC digest).';

------------------------------------------------------------------------------------------------------------
-- History: the event log and the spells derived from all evidence
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.member_event (
  event_id             bigint GENERATED BY DEFAULT AS IDENTITY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  event_at             timestamptz NOT NULL,               -- ChatMemberUpdated.date (= the update's date)
  update_id            bigint NOT NULL,
  received_at          timestamptz NOT NULL DEFAULT now(),
  old_status           text NOT NULL CHECK (old_status IN ('creator', 'administrator', 'member', 'restricted',
                                                           'left', 'kicked')),
  old_is_member        boolean,                            -- restricted only
  new_status           text NOT NULL CHECK (new_status IN ('creator', 'administrator', 'member', 'restricted',
                                                           'left', 'kicked')),
  new_is_member        boolean,
  kind                 text NOT NULL GENERATED ALWAYS AS
                         (tg.event_kind(old_status, old_is_member, new_status, new_is_member)) STORED,
  actor_id             bigint,                             -- `from` when it is not the member (admin, inviter)
  invite_link_id       bigint REFERENCES tg.invite_link,
  via_join_request     boolean NOT NULL DEFAULT false,
  via_folder_link      boolean NOT NULL DEFAULT false,
  old_until_date       timestamptz,                        -- ban/restriction end or paid subscription expiry
  new_until_date       timestamptz,
  source_chat_id       bigint,                             -- the id the update carried, when moved by a migration
  PRIMARY KEY (event_id, event_at),
  UNIQUE (update_id, event_at),                            -- update identity (D1): a replay is a no-op
  CHECK (actor_id IS DISTINCT FROM user_id)
) PARTITION BY RANGE (event_at);
CREATE INDEX member_event_chat_time ON tg.member_event (chat_id, event_at);
CREATE INDEX member_event_pair ON tg.member_event (chat_id, user_id, event_at);
CREATE INDEX member_event_user ON tg.member_event (user_id, event_at);
COMMENT ON TABLE tg.member_event IS
  'Every chat_member update, append-only, never deleted (D3). Written by the bot. Monthly partitions. '
  'A join, leave, join in one second stays three rows: the key is the update, not the status pair.';

CREATE TABLE tg.membership (
  membership_id        bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,   -- not stable across rebuilds
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  -- start
  start_source         text NOT NULL CHECK (start_source IN ('event', 'snapshot', 'probe', 'inferred')),
  joined_at            timestamptz,                        -- exact join time, from an event only
  join_event_id        bigint,                             -- the event that opened the spell
  joined_hint          timestamptz,                        -- Telegram's join date from a snapshot (unchecked, latest join)
  known_from           timestamptz NOT NULL,               -- earliest moment known to be in
  join_route           text CHECK (join_route IN ('link', 'request', 'folder', 'added', 'unattributed')),
  invite_link_id       bigint REFERENCES tg.invite_link,
  last_confirmed_at    timestamptz NOT NULL,               -- latest positive evidence (event, sighting, probe)
  suspect_since        timestamptz,                        -- open but doubted: absent from a good snapshot or a gap
  -- end
  end_source           text CHECK (end_source IN ('event', 'probe', 'inferred', 'migration')),
  end_reason           text CHECK (end_reason IN ('left', 'kicked', 'gone', 'migrated')),
  left_at              timestamptz,                        -- exact exit time, from an event only
  left_event_id        bigint,
  left_after           timestamptz,                        -- exit bounds: last known in .. first known out
  left_before          timestamptz,                        -- (both = left_at for an exact exit)
  stay                 tstzrange NOT NULL GENERATED ALWAYS AS
                         (tstzrange(known_from, coalesce(left_before, 'infinity'::timestamptz), '[)')) STORED,
  rebuilt_at           timestamptz NOT NULL DEFAULT now(),
  -- start provenance: an exact join time needs its event
  CONSTRAINT exact_join_iff_event_start CHECK ((start_source = 'event') = (joined_at IS NOT NULL)),
  CONSTRAINT exact_join_has_event_id CHECK ((joined_at IS NULL) = (join_event_id IS NULL)),
  CONSTRAINT exact_join_is_known_from CHECK (joined_at IS NULL OR joined_at = known_from),
  CONSTRAINT route_iff_event_start CHECK ((start_source = 'event') = (join_route IS NOT NULL)),
  CONSTRAINT join_event_exists FOREIGN KEY (join_event_id, joined_at) REFERENCES tg.member_event (event_id, event_at),
  -- end provenance: an end needs a reason and bounds; an exact exit needs its event
  CONSTRAINT end_has_reason CHECK ((end_source IS NULL) = (end_reason IS NULL)),
  CONSTRAINT end_has_bounds CHECK ((end_source IS NULL) = (left_after IS NULL) AND (end_source IS NULL) = (left_before IS NULL)),
  CONSTRAINT exact_exit_iff_event_end CHECK ((left_at IS NOT NULL) = coalesce(end_source = 'event', false)),
  CONSTRAINT exact_exit_has_event_id CHECK ((left_at IS NULL) = (left_event_id IS NULL)),
  CONSTRAINT exact_exit_bounds CHECK (left_at IS NULL OR (left_after = left_at AND left_before = left_at)),
  CONSTRAINT migration_end_reason CHECK (coalesce(end_source = 'migration', false) = coalesce(end_reason = 'migrated', false)),
  CONSTRAINT exit_event_exists FOREIGN KEY (left_event_id, left_at) REFERENCES tg.member_event (event_id, event_at),
  -- time order
  CONSTRAINT confirmed_after_start CHECK (known_from <= last_confirmed_at),
  CONSTRAINT exit_bounds_order CHECK (left_after <= left_before),
  CONSTRAINT exit_after_evidence CHECK (known_from <= left_after AND last_confirmed_at <= left_before),
  CONSTRAINT suspect_only_open CHECK (suspect_since IS NULL OR end_source IS NULL),
  -- one stay at a time per (chat, user)
  CONSTRAINT one_stay_at_a_time EXCLUDE USING gist (chat_id WITH =, user_id WITH =, stay WITH &&)
);
CREATE UNIQUE INDEX membership_one_open ON tg.membership (chat_id, user_id) WHERE end_source IS NULL;
CREATE INDEX membership_open_by_user ON tg.membership (user_id, chat_id) WHERE end_source IS NULL;
CREATE INDEX membership_pair ON tg.membership (chat_id, user_id, known_from);
CREATE INDEX membership_join ON tg.membership (chat_id, joined_at) WHERE joined_at IS NOT NULL;
CREATE INDEX membership_user_join ON tg.membership (user_id, joined_at) WHERE joined_at IS NOT NULL;
CREATE INDEX membership_end ON tg.membership (chat_id, left_before) WHERE end_source IS NOT NULL;
CREATE INDEX membership_suspect ON tg.membership (chat_id) WHERE suspect_since IS NOT NULL AND end_source IS NULL;
CREATE INDEX membership_found ON tg.membership (chat_id, known_from) WHERE start_source <> 'event';  -- starts not from an event
COMMENT ON TABLE tg.membership IS
  'Spells: one row per continuous stay of a user in a chat (D3). Derived, rebuildable per (chat, user) from '
  'member_event, sighting, probe, gap and chat_alias by the rebuild rules (design.md 4). Written by the bot '
  '(incremental apply) and the reconciler (rebuild). The exclusion constraint is also the GiST index for '
  'as-of-date membership (D13).';

------------------------------------------------------------------------------------------------------------
-- Snapshots (the collect script's input) and reconciliation
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.snapshot (
  snapshot_id          bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  started_at           timestamptz NOT NULL,
  finished_at          timestamptz,                        -- NULL while the script is still writing
  method               text NOT NULL CHECK (method IN ('basic_group', 'listing', 'deep')),
  reported_at_start    integer,                            -- Telegram's member count before the listing
  reported_at_end      integer,                            -- and after it
  fetched              integer,                            -- distinct users written; set when finished
  complete             boolean,                            -- the script's own verdict; false after an interrupt
  -- set by the reconciler
  quality              text CHECK (quality IN ('good', 'partial', 'rejected')),
  reconciled_at        timestamptz,
  members_pruned_at    timestamptz,                        -- snapshot_member rows removed (retention, D4)
  CHECK (finished_at IS NULL OR finished_at >= started_at),
  CHECK ((finished_at IS NULL) = (fetched IS NULL) AND (finished_at IS NULL) = (complete IS NULL)),
  CHECK (quality IS NULL OR finished_at IS NOT NULL),
  CHECK ((quality IS NULL) = (reconciled_at IS NULL))
);
CREATE INDEX snapshot_chat ON tg.snapshot (chat_id, started_at);
CREATE INDEX snapshot_to_reconcile ON tg.snapshot (snapshot_id) WHERE finished_at IS NOT NULL AND reconciled_at IS NULL;
COMMENT ON TABLE tg.snapshot IS
  'One member listing of one chat by the collect script (D4). The script inserts it with finished_at NULL, '
  'writes its members, then sets finished_at, fetched and complete in a last transaction. The reconciler '
  'sets quality: good when complete and the shortfall against max(reported) is at most max(10, 0.1%).';

CREATE TABLE tg.snapshot_member (
  snapshot_id          bigint NOT NULL REFERENCES tg.snapshot ON DELETE CASCADE,
  user_id              bigint NOT NULL,                    -- no FK: tg_user rows are made by the reconciler
  seen_at              timestamptz NOT NULL,               -- when the page with this member arrived
  joined_date          timestamptz,                        -- participant.date: Telegram's join date
  inviter_id           bigint,
  via_request          boolean,
  subscription_until   timestamptz,
  is_bot               boolean NOT NULL DEFAULT false,
  is_deleted           boolean NOT NULL DEFAULT false,
  is_premium           boolean,
  username             text,
  first_name           text,
  last_name            text,
  PRIMARY KEY (snapshot_id, user_id)
);
COMMENT ON TABLE tg.snapshot_member IS
  'Members of one snapshot, written by the collect script. Input only: the reconciler folds it into '
  'sighting and tg_user; rows are kept for the last 2 good snapshots per chat, then deleted (D4).';

CREATE TABLE tg.sighting (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  first_seen_at        timestamptz NOT NULL,
  first_snapshot_id    bigint NOT NULL REFERENCES tg.snapshot,
  last_seen_at         timestamptz NOT NULL,
  last_snapshot_id     bigint NOT NULL REFERENCES tg.snapshot,
  times_seen           integer NOT NULL DEFAULT 1 CHECK (times_seen >= 1),
  joined_date          timestamptz,                        -- latest Telegram join date seen in this run
  PRIMARY KEY (chat_id, user_id, first_seen_at),
  CHECK (first_seen_at <= last_seen_at)
);
COMMENT ON TABLE tg.sighting IS
  'Durable snapshot evidence: one row per run of consecutive sightings of a user in a chat''s snapshots, '
  'broken by any exit evidence. Survives the pruning of snapshot_member and feeds the rebuild. Written by '
  'the reconciler.';

CREATE TABLE tg.probe (
  probe_id             bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  reason               text NOT NULL CHECK (reason IN ('snapshot_absent', 'outage', 'conflict', 'sample', 'manual')),
  snapshot_id          bigint REFERENCES tg.snapshot,      -- the good snapshot the user was missing from
  absent_since         timestamptz,                        -- that snapshot's started_at: durable absence evidence
  queued_at            timestamptz NOT NULL DEFAULT now(),
  attempts             integer NOT NULL DEFAULT 0,
  last_attempt_at      timestamptz,
  outcome              text NOT NULL DEFAULT 'pending'
                       CHECK (outcome IN ('pending', 'in', 'out', 'error', 'cancelled')),
  answered_at          timestamptz,                        -- time of the successful getChatMember answer
  status_seen          text CHECK (status_seen IN ('creator', 'administrator', 'member', 'restricted', 'left', 'kicked')),
  is_member_seen       boolean,
  error_code           text CHECK (error_code IN ('429', '403', '400', '5xx', 'timeout', 'other')),
  CHECK ((reason = 'snapshot_absent') = (snapshot_id IS NOT NULL) AND (snapshot_id IS NULL) = (absent_since IS NULL)),
  CHECK ((outcome IN ('in', 'out')) = (answered_at IS NOT NULL) AND (answered_at IS NULL) = (status_seen IS NULL)),
  CHECK ((outcome = 'error') = (error_code IS NOT NULL))
);
CREATE UNIQUE INDEX probe_one_pending ON tg.probe (chat_id, user_id) WHERE outcome = 'pending';
CREATE INDEX probe_queue ON tg.probe (chat_id, queued_at) WHERE outcome = 'pending';
CREATE INDEX probe_pair ON tg.probe (chat_id, user_id);
COMMENT ON TABLE tg.probe IS
  'getChatMember checks (D5): the queue and their answers. Queued by the reconciler (absence from a good '
  'snapshot, after an outage, conflicts); answered by the prober within a per-chat budget. in/out answers '
  'are evidence; error is unknown and leaves the spell open and suspect.';

CREATE TABLE tg.reconcile_run (
  reconcile_run_id     bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  snapshot_id          bigint REFERENCES tg.snapshot,      -- NULL for a probe batch or a rebuild
  started_at           timestamptz NOT NULL DEFAULT now(),
  finished_at          timestamptz,
  listed               integer,                            -- users in the snapshot
  seeded               integer,                            -- spells opened with no earlier spell
  missed_joins         integer,                            -- spells opened after a closed one
  confirmed            integer,                            -- open spells seen again
  absent_candidates    integer,                            -- open spells missing from a good snapshot
  probes_queued        integer,
  closed_by_probe      integer,
  conflicts            integer                             -- sightings within the grace period after an exit
);
COMMENT ON TABLE tg.reconcile_run IS
  'What each reconciliation did, as counts (the reconcile record of D4, kept forever). Written by the '
  'reconciler. Feeds the drift report and metrics.';

CREATE TABLE tg.count_sample (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  sampled_at           timestamptz NOT NULL,
  reported             integer NOT NULL,                   -- getChatMemberCount
  known_open           integer NOT NULL,                   -- open spells, all accounts (bots included)
  suspect_open         integer NOT NULL DEFAULT 0,
  net_events           integer NOT NULL,                   -- joins minus exits by event time since the previous sample
  request_ms           integer,
  PRIMARY KEY (chat_id, sampled_at)
);
COMMENT ON TABLE tg.count_sample IS
  'Drift samples every 10 minutes per chat, staggered (D7). Written by the sampler. Absolute mismatch = '
  'reported - known_open; delta residual = (reported - previous reported) - net_events.';

------------------------------------------------------------------------------------------------------------
-- Join requests, posts, activity
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.join_request (
  join_request_id      bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  requested_at         timestamptz NOT NULL,
  update_id            bigint,
  invite_link_id       bigint REFERENCES tg.invite_link,
  outcome              text CHECK (outcome IN ('approved', 'declined', 'unknown')),   -- NULL = pending
  resolved_at          timestamptz,
  resolved_by          bigint,                             -- the approving admin (`from` of the join)
  UNIQUE (chat_id, user_id, requested_at),
  CHECK ((outcome IS NULL) = (resolved_at IS NULL) OR outcome = 'unknown')
);
CREATE INDEX join_request_chat_time ON tg.join_request (chat_id, requested_at);
COMMENT ON TABLE tg.join_request IS
  'chat_join_request updates (D10); never bio or user_chat_id. Written by the bot; the outcome is matched '
  'from the later chat_member join (approved), from the bot''s own decline, or set unknown after 7 days.';

CREATE TABLE tg.post (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  message_id           bigint NOT NULL,
  posted_at            timestamptz NOT NULL,
  kind                 text NOT NULL CHECK (kind IN ('text', 'photo', 'video', 'animation', 'audio', 'document',
                                                     'poll', 'giveaway', 'other')),
  PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX post_chat_time ON tg.post (chat_id, posted_at);
COMMENT ON TABLE tg.post IS
  'Channel posts, id and time only, never text (D12). Written by the bot from channel_post. For "leaves after '
  'a post".';

CREATE TABLE tg.activity_daily (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  user_id              bigint NOT NULL REFERENCES tg.tg_user,
  day                  date NOT NULL,                      -- UTC day
  messages             integer NOT NULL CHECK (messages > 0),
  PRIMARY KEY (chat_id, user_id, day)
);
CREATE INDEX activity_daily_chat_day ON tg.activity_daily (chat_id, day);
COMMENT ON TABLE tg.activity_daily IS
  'Messages per user per day in groups and discussion groups, counts only (D12). Written by the bot. '
  'Anonymous admins, sender_chat and automatic forwards are never attributed to a user (see unattributed_daily).';

CREATE TABLE tg.unattributed_daily (
  chat_id              bigint NOT NULL REFERENCES tg.chat ON UPDATE CASCADE,
  day                  date NOT NULL,
  sender_kind          text NOT NULL CHECK (sender_kind IN ('sender_chat', 'automatic_forward', 'anonymous_admin')),
  messages             integer NOT NULL CHECK (messages > 0),
  PRIMARY KEY (chat_id, day, sender_kind)
);
COMMENT ON TABLE tg.unattributed_daily IS
  'Group messages not sent as a user (D12). Written by the bot.';

------------------------------------------------------------------------------------------------------------
-- Rollups (D13)
------------------------------------------------------------------------------------------------------------

CREATE TABLE tg.rollup_dirty (
  chat_id              bigint PRIMARY KEY REFERENCES tg.chat ON UPDATE CASCADE ON DELETE CASCADE,
  from_day             date NOT NULL,                      -- earliest affected UTC day
  marked_at            timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE tg.rollup_dirty IS
  'chat_day invalidation: the earliest day touched by late data, reconciliation, a rebuild or a migration. '
  'Upserted with least(from_day) by the bot and the reconciler; cleared by the batch job.';

-- Current chat set of every human with an open spell in a tracked chat. Feeds overlap, unique and the
-- per-user chat count. Refreshed by the batch job (CONCURRENTLY after the first build).
CREATE MATERIALIZED VIEW tg.user_chats AS
SELECT m.user_id,
       array_agg(m.chat_id ORDER BY m.chat_id) AS chat_ids,
       count(*)::integer AS chat_count
FROM tg.membership m
JOIN tg.chat c ON c.chat_id = m.chat_id AND c.tracked
WHERE m.end_source IS NULL
  AND m.user_id NOT IN (SELECT n.user_id FROM tg.tg_user n WHERE n.is_bot OR n.is_deleted)
GROUP BY m.user_id
WITH NO DATA;
CREATE UNIQUE INDEX user_chats_user ON tg.user_chats (user_id);
CREATE INDEX user_chats_count ON tg.user_chats (chat_count);
COMMENT ON MATERIALIZED VIEW tg.user_chats IS
  'Per-user chat count and chat set (D13), user-level so in tg. Written by the batch job.';

CREATE TABLE stats.rollup_run (
  rollup_run_id        bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  rollup               text NOT NULL CHECK (rollup IN ('chat_day', 'audience')),   -- audience = user_chats+overlap+unique
  started_at           timestamptz NOT NULL,
  finished_at          timestamptz NOT NULL,
  as_of                timestamptz NOT NULL,               -- the data the rollup describes
  row_count            bigint NOT NULL
);
CREATE INDEX rollup_run_latest ON stats.rollup_run (rollup, finished_at DESC);
COMMENT ON TABLE stats.rollup_run IS 'One row per finished rollup run, for as-of display and freshness metrics. Written by the batch job.';

CREATE TABLE stats.chat_day (
  chat_id              bigint NOT NULL,
  day                  date NOT NULL,                      -- UTC
  joins                integer NOT NULL,                   -- spells opened by a join event (all accounts)
  rejoins              integer NOT NULL,                   -- of those, users with an earlier spell in the chat
  exits                integer NOT NULL,                   -- spells closed by an exit event (leave, kick, restrict out)
  kicks                integer NOT NULL,                   -- of those, kicked
  bans_from_out        integer NOT NULL,                   -- moderation of non-members (not exits)
  net                  integer NOT NULL,                   -- joins - exits
  short_stays          integer NOT NULL,                   -- joined this day, exit confirmed within 48 h
  corrections_in       integer NOT NULL,                   -- spells opened by snapshot or probe
  corrections_out      integer NOT NULL,                   -- spells closed by probe or inferred end
  reported_eod         integer,                            -- last count_sample of the day
  known_eod            integer,
  messages             integer NOT NULL,
  active_users         integer NOT NULL,
  -- daily deltas of the human members holding a stay; their running sums give members at the end of each day
  members_started      integer NOT NULL,                   -- stays whose known_from falls on this day
  members_ended        integer NOT NULL,                   -- stays whose left_before falls on this day
  unsure_started       integer NOT NULL,                   -- bounded exits whose left_after falls on this day
  unsure_ended         integer NOT NULL,                   -- bounded exits whose left_before falls on this day
  refreshed_at         timestamptz NOT NULL,
  PRIMARY KEY (chat_id, day)
);
COMMENT ON TABLE stats.chat_day IS
  'Daily per-chat movement (D13). Written by the batch job: recomputed from the earliest dirty day (or the '
  'last 2 days, because short stays mature after 48 h) to today. The members_* and unsure_* deltas make '
  '"members at the end of day D" a running sum (stats.v_chat_day), so as-of counts need no scan of a 1M chat.';

CREATE TABLE tg.chat_set (
  chat_ids             bigint[] PRIMARY KEY,               -- a sorted set of tracked chats
  chat_count           integer NOT NULL,
  users                bigint NOT NULL,                    -- humans whose current chat set is exactly this
  as_of                timestamptz NOT NULL
);
CREATE INDEX chat_set_count ON tg.chat_set (chat_count);
COMMENT ON TABLE tg.chat_set IS
  'tg.user_chats grouped by identical chat sets (a few hundred thousand rows for 3M people). The pair matrix, '
  'chats per user and unique per chat for any scope are computed from it. Written by the batch job (R2). In tg: '
  'a set with one user describes that person.';

CREATE TABLE stats.overlap (
  chat_a               bigint NOT NULL,
  chat_b               bigint NOT NULL,
  shared               bigint NOT NULL,                    -- humans with open spells in both; a = b: chat size
  as_of                timestamptz NOT NULL,
  PRIMARY KEY (chat_a, chat_b),
  CHECK (chat_a <= chat_b)
);
COMMENT ON TABLE stats.overlap IS
  'Pair matrix of tracked chats incl. diagonal and zero pairs, one row per unordered pair (D13). Written by '
  'the batch job from tg.user_chats by grouping identical chat sets.';

CREATE TABLE stats.unique_per_chat (
  chat_id              bigint PRIMARY KEY,
  members              bigint NOT NULL,                    -- distinct humans with an open spell
  exclusive            bigint NOT NULL,                    -- of those, in no other tracked chat
  as_of                timestamptz NOT NULL
);
COMMENT ON TABLE stats.unique_per_chat IS 'Unique (exclusive) users per chat over all tracked chats. Written by the batch job.';

CREATE TABLE stats.chats_per_user (
  chat_count           integer PRIMARY KEY,
  users                bigint NOT NULL,
  as_of                timestamptz NOT NULL
);
COMMENT ON TABLE stats.chats_per_user IS 'Distribution: how many humans are in k tracked chats. Written by the batch job.';

------------------------------------------------------------------------------------------------------------
-- Functions: partitions, report flags, campaign entrants, migration, forget-user
------------------------------------------------------------------------------------------------------------

-- Create the partitions of tg.<parent> that cover [p_from, p_to). p_step = 'month' or 'day' (UTC).
CREATE FUNCTION tg.ensure_partitions(p_parent text, p_step text, p_from timestamptz, p_to timestamptz)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  v_lo timestamptz := date_trunc(p_step, p_from, 'UTC');
  v_hi timestamptz;
  v_name text;
  v_made integer := 0;
BEGIN
  IF p_step NOT IN ('month', 'day') THEN RAISE EXCEPTION 'step must be month or day, not %', p_step; END IF;
  WHILE v_lo < p_to LOOP
    v_hi := CASE p_step WHEN 'month' THEN (v_lo AT TIME ZONE 'UTC' + interval '1 month') AT TIME ZONE 'UTC'
                        ELSE v_lo + interval '1 day' END;
    v_name := p_parent || CASE p_step WHEN 'month' THEN to_char(v_lo AT TIME ZONE 'UTC', '"_y"YYYY"m"MM')
                                      ELSE to_char(v_lo AT TIME ZONE 'UTC', '"_d"YYYYMMDD') END;
    IF to_regclass('tg.' || v_name) IS NULL THEN
      EXECUTE format('CREATE TABLE tg.%I PARTITION OF tg.%I FOR VALUES FROM (%L) TO (%L)',
                     v_name, p_parent, v_lo, v_hi);
      v_made := v_made + 1;
    END IF;
    v_lo := v_hi;
  END LOOP;
  RETURN v_made;
END $$;

-- Drop the partitions of tg.<parent> whose upper bound is older than p_now - p_keep.
CREATE FUNCTION tg.drop_old_partitions(p_parent text, p_keep interval, p_now timestamptz)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
  r record;
  v_dropped integer := 0;
BEGIN
  FOR r IN
    SELECT c.relname,
           (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \(''([^'']+)''\)'))[1]::timestamptz AS upper_bound
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = format('tg.%I', p_parent)::regclass
  LOOP
    IF r.upper_bound <= p_now - p_keep THEN
      EXECUTE format('DROP TABLE tg.%I', r.relname);
      v_dropped := v_dropped + 1;
    END IF;
  END LOOP;
  RETURN v_dropped;
END $$;

-- Called by the service at start and every day: partitions ahead for writes, retention for raw data.
-- member_event keeps everything (full history); raw_update 30 days (D14); update_receipt 8 days (D1).
CREATE FUNCTION tg.maintain_partitions(p_now timestamptz DEFAULT now())
RETURNS TABLE (parent text, created integer, dropped integer) LANGUAGE plpgsql AS $$
BEGIN
  parent := 'member_event';
  created := tg.ensure_partitions('member_event', 'month', p_now - interval '1 month', p_now + interval '3 months');
  dropped := 0;
  RETURN NEXT;
  parent := 'raw_update';
  created := tg.ensure_partitions('raw_update', 'day', p_now - interval '2 days', p_now + interval '7 days');
  dropped := tg.drop_old_partitions('raw_update', interval '30 days', p_now);
  RETURN NEXT;
  parent := 'update_receipt';
  created := tg.ensure_partitions('update_receipt', 'day', p_now - interval '2 days', p_now + interval '7 days');
  dropped := tg.drop_old_partitions('update_receipt', interval '8 days', p_now);
  RETURN NEXT;
END $$;

-- Report flags for one chat and time window: gaps that overlap it, and whether the bot covered all of it.
-- NULL = clean. Security definer so the stats views can call it for bi_reader; it returns no user data.
CREATE FUNCTION tg.window_flags(p_chat_id bigint, p_window tstzrange) RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, tg AS $$
  SELECT nullif(concat_ws('; ',
    (SELECT string_agg(DISTINCT 'gap:' || g.reason, ',' ORDER BY 'gap:' || g.reason)
       FROM tg.gap g
      WHERE (g.chat_id = p_chat_id OR g.chat_id IS NULL) AND g.period && p_window),
    CASE WHEN NOT coalesce((SELECT range_agg(c.period) FROM tg.coverage c WHERE c.chat_id = p_chat_id) @> p_window,
                           false)
         THEN 'not covered' END), '')
$$;

-- The order of the spells of one person in one chat that start at the same instant (join, leave, join in one
-- second), as a bigint[] {update_id, rank, membership_id} that compares lexicographically. Replay order is
-- (event_at, update_id), events before sightings before probes (design.md 4). An exact spell is ordered by its
-- join event. An inferred start (E6, E9) has no join event of its own: the function replays the events of that
-- instant the way the rebuild does (a join opens an exact stay; an exit or an in -> in event with no stay open
-- opens an inferred one; an exit, a join over an open stay or an out -> out event closes one) and so knows, for
-- each inferred stay of the replay, the update id of the event that opened it and how it ended: closed by an
-- exit event (that event's update id), closed by inference (-1), or not at all within that instant (0). The
-- stored spell is matched to its replayed stay by that ending (its left event, or left_before = the instant, or
-- later), so it is the provenance of a stored stay, never the order of the membership ids, that decides; only
-- stays with the very same ending (several inferred closes) fall back to the order of their ids, and those are
-- interchangeable for every count. Snapshot starts follow the events (rank 2), probes follow those (rank 3).
-- Callers compare two stays of the same instant: tg.spell_order(a) < tg.spell_order(b).
CREATE FUNCTION tg.spell_order(p_membership_id bigint) RETURNS bigint[]
LANGUAGE plpgsql STABLE AS $$
DECLARE
  s record;
  e record;
  v_kind text;                       -- the stay open during the replay: none | exact | inferred
  v_cur integer := 0;                -- its index in the arrays, when inferred
  v_opener bigint[] := '{}';         -- inferred stays of the replay: the opening event's update id ...
  v_closer bigint[] := '{}';         -- ... and how it ended: an exit's update id, -1 by inference, 0 not within the instant
  v_key bigint;
  v_k integer;
  v_i integer;
  v_seen integer := 0;
BEGIN
  SELECT m.chat_id, m.user_id, m.known_from, m.start_source, m.join_event_id, m.joined_at, m.end_source,
         m.left_event_id, m.left_at, m.left_before INTO s
    FROM tg.membership m WHERE m.membership_id = p_membership_id;
  IF NOT FOUND THEN
    RETURN NULL;
  ELSIF s.join_event_id IS NOT NULL THEN
    RETURN ARRAY[coalesce((SELECT x.update_id FROM tg.member_event x
                            WHERE x.event_id = s.join_event_id AND x.event_at = s.joined_at), 9223372036854775807),
                 0, p_membership_id];
  ELSIF s.start_source = 'snapshot' THEN
    RETURN ARRAY[9223372036854775807, 2, p_membership_id];
  ELSIF s.start_source = 'probe' THEN
    RETURN ARRAY[9223372036854775807, 3, p_membership_id];
  END IF;
  v_kind := CASE WHEN EXISTS (SELECT 1 FROM tg.membership x
                               WHERE x.chat_id = s.chat_id AND x.user_id = s.user_id AND x.known_from < s.known_from
                                 AND (x.left_before IS NULL OR x.left_before >= s.known_from))
                 THEN 'exact' ELSE 'none' END;
  FOR e IN SELECT x.update_id, x.kind FROM tg.member_event x
            WHERE x.chat_id = s.chat_id AND x.user_id = s.user_id AND x.event_at = s.known_from
            ORDER BY x.update_id LOOP
    IF e.kind = 'join' THEN
      IF v_kind = 'inferred' THEN v_closer[v_cur] := -1; END IF;      -- E4: the old stay ends by inference
      v_kind := 'exact';
    ELSIF e.kind IN ('ban_out', 'unban', 'out_change') THEN
      IF v_kind = 'inferred' THEN v_closer[v_cur] := -1; END IF;      -- E10 closes, E11 changes nothing
      v_kind := 'none';
    ELSIF e.kind IN ('leave', 'kick', 'restrict_out') THEN
      IF v_kind = 'none' THEN                                         -- E6, E7: inferred, closed by this event
        v_opener := v_opener || e.update_id;
        v_closer := v_closer || e.update_id;
      ELSIF v_kind = 'inferred' THEN
        v_closer[v_cur] := e.update_id;                               -- E5
      END IF;
      v_kind := 'none';
    ELSIF v_kind = 'none' THEN                                        -- E9: a missed join opens an inferred stay
      v_opener := v_opener || e.update_id;
      v_closer := v_closer || 0::bigint;
      v_cur := cardinality(v_opener);
      v_kind := 'inferred';
    END IF;
  END LOOP;
  -- how this stored stay ended, in the replay's terms
  v_key := CASE WHEN s.end_source IS NULL OR s.left_before > s.known_from THEN 0
                WHEN s.left_event_id IS NOT NULL AND s.left_at = s.known_from
                  THEN coalesce((SELECT x.update_id FROM tg.member_event x
                                  WHERE x.event_id = s.left_event_id AND x.event_at = s.left_at), -1)
                ELSE -1 END;
  SELECT count(*) + 1 INTO v_k FROM tg.membership x            -- among stays with the same ending: by id
   WHERE x.chat_id = s.chat_id AND x.user_id = s.user_id AND x.known_from = s.known_from
     AND x.start_source = 'inferred' AND x.membership_id < p_membership_id
     AND (CASE WHEN x.end_source IS NULL OR x.left_before > x.known_from THEN 0
               WHEN x.left_event_id IS NOT NULL AND x.left_at = x.known_from
                 THEN coalesce((SELECT y.update_id FROM tg.member_event y
                                 WHERE y.event_id = x.left_event_id AND y.event_at = x.left_at), -1)
               ELSE -1 END) = v_key;
  FOR v_i IN 1 .. cardinality(v_opener) LOOP
    IF v_closer[v_i] = v_key THEN
      v_seen := v_seen + 1;
      IF v_seen = v_k THEN
        RETURN ARRAY[v_opener[v_i], 1, p_membership_id];
      END IF;
    END IF;
  END LOOP;
  RETURN ARRAY[9223372036854775807, 1, p_membership_id];             -- the stored stays do not match the replay
END $$;
REVOKE EXECUTE ON FUNCTION tg.spell_order(bigint) FROM PUBLIC;

-- Giveaway entrants of one campaign (analytics item 4), one row per (chat, user): the earliest spell with
-- an exact join inside [starts_at, min(draw_at, as_of)] (tied starts by the opening event's order), and its
-- outcomes. Every entrant is reported; first_time = no earlier stay of the person in the chat (returners
-- apart); eligible = not only_new_members, or first_time (a campaign with only_new_members does not admit
-- returners). A campaign that starts after as_of has no entrants. Text outcomes: yes | no | unknown | immature,
-- n/a when the question does not apply. Only confirmed evidence counts as an exit (bounds decide), and an
-- outcome after as_of is never counted: it is immature (the quick exit and the exit after the draw look only
-- as far as as_of; still_in is the person's state at as_of, not today's open row).
CREATE FUNCTION tg.campaign_entrant(p_campaign_id bigint, p_hours numeric, p_as_of timestamptz)
RETURNS TABLE (campaign_id bigint, chat_id bigint, user_id bigint, joined_at timestamptz, first_time boolean,
               quick_exit text, exit_reason text, at_draw text, exit_after_draw text, still_in text,
               eligible boolean)
LANGUAGE sql STABLE AS $$
WITH g AS (
  SELECT c.campaign_id, c.starts_at, c.draw_at, c.only_new_members, make_interval(secs => p_hours * 3600) AS h
  FROM tg.campaign c WHERE c.campaign_id = p_campaign_id
), entrant AS (
  SELECT DISTINCT ON (m.chat_id, m.user_id)
         g.campaign_id, g.draw_at, g.h, g.only_new_members, m.membership_id, m.join_event_id, m.chat_id, m.user_id,
         m.joined_at, m.known_from, m.left_after, m.left_before, m.end_source, m.end_reason, m.suspect_since
  FROM g
  JOIN tg.campaign_chat cc ON cc.campaign_id = g.campaign_id
  JOIN tg.membership m ON m.chat_id = cc.chat_id
                      AND m.joined_at >= g.starts_at AND m.joined_at <= least(g.draw_at, p_as_of)
  JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
  ORDER BY m.chat_id, m.user_id, m.joined_at,
           tg.spell_order(m.membership_id)
), outcome AS (
  SELECT e.*,
         tg.state_at(e.known_from, e.left_after, e.left_before, e.end_source, e.suspect_since,
                     least(e.joined_at + e.h, p_as_of)) AS quick_state,
         CASE WHEN e.draw_at > p_as_of THEN 'immature'
              ELSE coalesce(tg.state_at(d.known_from, d.left_after, d.left_before, d.end_source, d.suspect_since,
                                        e.draw_at), 'out') END AS draw_state,
         tg.state_at(d.known_from, d.left_after, d.left_before, d.end_source, d.suspect_since,
                     least(e.draw_at + e.h, p_as_of)) AS after_draw_state,
         coalesce(tg.state_at(n.known_from, n.left_after, n.left_before, n.end_source, n.suspect_since,
                              p_as_of), 'out') AS as_of_state
  FROM entrant e
  LEFT JOIN LATERAL (                                      -- the spell that holds the draw time
    SELECT s.known_from, s.left_after, s.left_before, s.end_source, s.suspect_since
    FROM tg.membership s
    WHERE s.chat_id = e.chat_id AND s.user_id = e.user_id AND s.known_from <= e.draw_at
    ORDER BY (s.stay @> e.draw_at) DESC, s.known_from DESC, tg.spell_order(s.membership_id) DESC
    LIMIT 1) d ON true
  LEFT JOIN LATERAL (                                      -- the latest spell that had started by as_of
    SELECT s.known_from, s.left_after, s.left_before, s.end_source, s.suspect_since
    FROM tg.membership s
    WHERE s.chat_id = e.chat_id AND s.user_id = e.user_id AND s.known_from <= p_as_of
    ORDER BY (s.stay @> p_as_of) DESC, s.known_from DESC, tg.spell_order(s.membership_id) DESC
    LIMIT 1) n ON true
), flagged AS (
  SELECT o.*,
         NOT EXISTS (SELECT 1 FROM tg.membership p
                      WHERE p.chat_id = o.chat_id AND p.user_id = o.user_id AND p.membership_id <> o.membership_id
                        AND (p.known_from < o.joined_at
                             OR (p.known_from = o.joined_at          -- the same instant: the replay order
                                 AND tg.spell_order(p.membership_id) < tg.spell_order(o.membership_id)))) AS first_time
  FROM outcome o
)
SELECT f.campaign_id, f.chat_id, f.user_id, f.joined_at, f.first_time,
       CASE WHEN f.quick_state = 'out' THEN 'yes'
            WHEN f.joined_at + f.h > p_as_of THEN 'immature'
            WHEN f.quick_state = 'unknown' THEN 'unknown'
            ELSE 'no' END,
       CASE WHEN f.quick_state = 'out' THEN f.end_reason END,
       f.draw_state,
       CASE f.draw_state
            WHEN 'in' THEN CASE WHEN f.after_draw_state = 'out' THEN 'yes'
                                WHEN f.draw_at + f.h > p_as_of THEN 'immature'
                                WHEN f.after_draw_state = 'unknown' THEN 'unknown'
                                ELSE 'no' END
            WHEN 'out' THEN 'n/a'
            ELSE f.draw_state END,
       f.as_of_state,
       NOT f.only_new_members OR f.first_time
FROM flagged f
$$;
REVOKE EXECUTE ON FUNCTION tg.campaign_entrant(bigint, numeric, timestamptz) FROM PUBLIC;

-- Per-chat campaign summary for BI and the CLI (item 4a). Aggregates only; security definer so bi_reader
-- can read it through stats.v_campaign_summary without access to tg. entrants = everyone who joined in the
-- window (first_time and returners apart); eligible is filled only for a campaign with only_new_members and
-- counts the first-time joiners (returners are not eligible), else NULL.
CREATE FUNCTION stats.campaign_summary(p_hours numeric DEFAULT 48, p_as_of timestamptz DEFAULT now(),
                                       p_campaign_id bigint DEFAULT NULL)
RETURNS TABLE (campaign_id bigint, campaign text, origin text, only_new_members boolean, chat_id bigint,
               entrants bigint, first_time bigint, returners bigint, eligible bigint,
               quick_exit bigint, quick_exit_kicked bigint, quick_exit_unknown bigint, quick_exit_immature bigint,
               in_at_draw bigint, in_at_draw_unknown bigint,
               exit_after_draw bigint, exit_after_draw_unknown bigint, exit_after_draw_immature bigint,
               still_in bigint, still_in_suspect bigint, window_flags text, hours numeric, as_of timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, tg AS $$
  SELECT c.campaign_id, c.name, c.origin, c.only_new_members, cc.chat_id,
         count(e.user_id), count(*) FILTER (WHERE e.first_time), count(*) FILTER (WHERE NOT e.first_time),
         CASE WHEN c.only_new_members THEN count(*) FILTER (WHERE e.eligible) END,
         count(*) FILTER (WHERE e.quick_exit = 'yes'),
         count(*) FILTER (WHERE e.quick_exit = 'yes' AND e.exit_reason = 'kicked'),
         count(*) FILTER (WHERE e.quick_exit = 'unknown'),
         count(*) FILTER (WHERE e.quick_exit = 'immature'),
         count(*) FILTER (WHERE e.at_draw = 'in'), count(*) FILTER (WHERE e.at_draw = 'unknown'),
         count(*) FILTER (WHERE e.exit_after_draw = 'yes'), count(*) FILTER (WHERE e.exit_after_draw = 'unknown'),
         count(*) FILTER (WHERE e.exit_after_draw = 'immature'),
         count(*) FILTER (WHERE e.still_in = 'in'), count(*) FILTER (WHERE e.still_in = 'unknown'),
         CASE WHEN c.starts_at < p_as_of THEN              -- a campaign still to start has no window yet
           tg.window_flags(cc.chat_id, tstzrange(c.starts_at,
                           least(c.draw_at + make_interval(secs => p_hours * 3600), p_as_of))) END,
         p_hours, p_as_of
  FROM tg.campaign c
  JOIN tg.campaign_chat cc ON cc.campaign_id = c.campaign_id
  LEFT JOIN tg.campaign_entrant(c.campaign_id, p_hours, p_as_of) e ON e.chat_id = cc.chat_id
  WHERE p_campaign_id IS NULL OR c.campaign_id = p_campaign_id
  GROUP BY c.campaign_id, c.name, c.origin, c.only_new_members, cc.chat_id, c.starts_at, c.draw_at
$$;

-- Group to supergroup migration (D2): move every row of the old chat id to the new one in the caller's
-- transaction, record the alias and drop the old chat row. Spells of the old id are deleted; the caller
-- rebuilds the affected (chat, user) pairs from the moved evidence (join echoes in the new id are absorbed
-- by the rebuild rules). Returns the number of moved events.
CREATE FUNCTION tg.move_chat_history(p_old_chat_id bigint, p_new_chat_id bigint, p_migrated_at timestamptz)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_events bigint;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM tg.chat WHERE chat_id = p_new_chat_id) THEN
    RAISE EXCEPTION 'new chat % must exist before the move', p_new_chat_id;
  END IF;
  DELETE FROM tg.membership WHERE chat_id = p_old_chat_id;
  UPDATE tg.member_event SET chat_id = p_new_chat_id, source_chat_id = coalesce(source_chat_id, p_old_chat_id)
   WHERE chat_id = p_old_chat_id;
  GET DIAGNOSTICS v_events = ROW_COUNT;
  UPDATE tg.coverage SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.gap SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.invite_link SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.snapshot SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.sighting SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.probe SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.reconcile_run SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.join_request SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.post SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;
  UPDATE tg.campaign_chat SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id
     AND NOT EXISTS (SELECT 1 FROM tg.campaign_chat x WHERE x.campaign_id = campaign_chat.campaign_id
                                                         AND x.chat_id = p_new_chat_id);
  INSERT INTO tg.count_sample SELECT p_new_chat_id, sampled_at, reported, known_open, suspect_open, net_events, request_ms
    FROM tg.count_sample WHERE chat_id = p_old_chat_id ON CONFLICT DO NOTHING;
  INSERT INTO tg.activity_daily AS a SELECT p_new_chat_id, user_id, day, messages
    FROM tg.activity_daily WHERE chat_id = p_old_chat_id
    ON CONFLICT (chat_id, user_id, day) DO UPDATE SET messages = a.messages + excluded.messages;
  INSERT INTO tg.unattributed_daily AS a SELECT p_new_chat_id, day, sender_kind, messages
    FROM tg.unattributed_daily WHERE chat_id = p_old_chat_id
    ON CONFLICT (chat_id, day, sender_kind) DO UPDATE SET messages = a.messages + excluded.messages;
  DELETE FROM tg.count_sample WHERE chat_id = p_old_chat_id;
  DELETE FROM tg.activity_daily WHERE chat_id = p_old_chat_id;
  DELETE FROM tg.unattributed_daily WHERE chat_id = p_old_chat_id;
  DELETE FROM tg.campaign_chat WHERE chat_id = p_old_chat_id;
  DELETE FROM tg.rollup_dirty WHERE chat_id = p_old_chat_id;
  DELETE FROM stats.chat_day WHERE chat_id = p_old_chat_id;
  DELETE FROM tg.chat WHERE chat_id = p_old_chat_id;
  INSERT INTO tg.chat_alias (old_chat_id, chat_id, migrated_at) VALUES (p_old_chat_id, p_new_chat_id, p_migrated_at);
  UPDATE tg.chat_alias SET chat_id = p_new_chat_id WHERE chat_id = p_old_chat_id;   -- chained migrations
  INSERT INTO tg.rollup_dirty (chat_id, from_day)
    SELECT p_new_chat_id, coalesce(min(event_at)::date, p_migrated_at::date) FROM tg.member_event
     WHERE chat_id = p_new_chat_id
  ON CONFLICT (chat_id) DO UPDATE SET from_day = least(tg.rollup_dirty.from_day, excluded.from_day), marked_at = now();
  RETURN v_events;
END $$;

-- Erase one person (D17, CLI forget-user). The CLI runs it only under tgaud.db.maintenance (the service stopped)
-- with lock_chats of every chat the person appears in, then refreshes the audience rollup (R2) itself; this
-- function is the one transaction of the erasure itself:
--  * the subject rows go: spells, events, sightings, probes, snapshot rows, join requests, activity counts,
--    the tg_user row, and every raw payload that names the person anywhere (as subject, `from` or link creator);
--  * secondary references are cut, never the other people's history: actor_id (actor_is_other keeps "kicked" and
--    the "added" route), join_request.resolved_by, snapshot_member.inviter_id, invite_link.creator_id (a masked
--    link row that would then equal another is merged into it);
--  * tg.erasure records a hash of the id and the time, so old in-flight evidence (an update, a listing, a probe
--    answer dated before it) is dropped by ingest and the reconciler (tg.is_erased);
--  * every chat the person touched gets a dirty marker from the earliest affected day (chat_day counts them).
-- It refuses while a snapshot is unfinished: the collect script would still write rows for the person.
CREATE FUNCTION tg.forget_user(p_user_id bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r record;
BEGIN
  -- No page of the collect script can be committed from here to the end of this transaction: a page waiting
  -- for the lock is checked against tg.erasure (trigger snapshot_member_skip_erased, migration 008) after it,
  -- and a page in flight is waited for, so the delete below sees it.
  LOCK TABLE tg.snapshot_member IN SHARE ROW EXCLUSIVE MODE;
  IF EXISTS (SELECT 1 FROM tg.snapshot WHERE finished_at IS NULL) THEN
    RAISE EXCEPTION 'a snapshot is still being written: finish or delete it before forget-user'
      USING ERRCODE = 'object_not_in_prerequisite_state';
  END IF;
  INSERT INTO tg.erasure (user_hash, erased_at) VALUES (tg.erasure_hash(p_user_id), clock_timestamp())
    ON CONFLICT (user_hash) DO UPDATE SET erased_at = excluded.erased_at;

  PERFORM tg.mark_dirty(d.chat_id, d.day) FROM (
    SELECT chat_id, min(day) AS day FROM (
      SELECT chat_id, (least(known_from, left_after, left_before, suspect_since) AT TIME ZONE 'UTC')::date AS day
        FROM tg.membership WHERE user_id = p_user_id
      UNION ALL
      SELECT chat_id, day FROM tg.activity_daily WHERE user_id = p_user_id
    ) x GROUP BY chat_id) d;

  UPDATE tg.member_event SET actor_id = NULL WHERE actor_id = p_user_id;
  UPDATE tg.join_request SET resolved_by = NULL WHERE resolved_by = p_user_id;
  UPDATE tg.snapshot_member SET inviter_id = NULL WHERE inviter_id = p_user_id;
  -- A creator is cleared on its own row, so no link id, campaign or reference of anyone else changes. A masked
  -- link (the redacted text is all the Bot API shows of other admins' links, and two links can share it) gets
  -- its id appended: its identity was (chat, text, creator), and with the creator gone a later update for the
  -- same text, which ingest resolves to "no creator", must open a link of unknown origin, not match and
  -- rename this one or its twin. Bot-made links are found by their digest and keep it.
  FOR r IN SELECT l.invite_link_id, l.masked_link FROM tg.invite_link l WHERE l.creator_id = p_user_id LOOP
    IF r.masked_link IS NOT NULL THEN
      UPDATE tg.invite_link SET creator_id = NULL, masked_link = masked_link || ' #' || invite_link_id
       WHERE invite_link_id = r.invite_link_id;
    ELSE
      UPDATE tg.invite_link SET creator_id = NULL WHERE invite_link_id = r.invite_link_id;
    END IF;
  END LOOP;

  DELETE FROM tg.raw_update
   WHERE user_id = p_user_id
      OR jsonb_path_exists(payload, '$.** ? (@.id == $uid)', jsonb_build_object('uid', p_user_id));
  DELETE FROM tg.membership WHERE user_id = p_user_id;
  DELETE FROM tg.member_event WHERE user_id = p_user_id;
  DELETE FROM tg.sighting WHERE user_id = p_user_id;
  DELETE FROM tg.probe WHERE user_id = p_user_id;
  DELETE FROM tg.snapshot_member WHERE user_id = p_user_id;
  DELETE FROM tg.join_request WHERE user_id = p_user_id;
  DELETE FROM tg.activity_daily WHERE user_id = p_user_id;
  DELETE FROM tg.tg_user WHERE user_id = p_user_id;

  -- Reconciliation records that hold the id as a number: the diagnostic endpoints are cleared, and a live cursor
  -- (every user_id <= it is applied) moves to the nearest surviving id below, which sorts the survivors alike.
  UPDATE tg.reconcile_run SET from_user_id = NULL WHERE from_user_id = p_user_id;
  UPDATE tg.reconcile_run SET through_user_id = NULL WHERE through_user_id = p_user_id;
  UPDATE tg.snapshot s SET applied_through_user_id =
      (SELECT max(u.user_id) FROM (SELECT m.user_id FROM tg.snapshot_member m WHERE m.snapshot_id = s.snapshot_id
                                   UNION ALL SELECT t.user_id FROM tg.tg_user t) u WHERE u.user_id < p_user_id)
   WHERE s.applied_through_user_id = p_user_id;
  UPDATE tg.gap_progress g SET last_user_id = (SELECT max(t.user_id) FROM tg.tg_user t WHERE t.user_id < p_user_id)
   WHERE g.last_user_id = p_user_id;
END $$;

------------------------------------------------------------------------------------------------------------
-- BI views (schema stats): aggregates only, no user ids
------------------------------------------------------------------------------------------------------------

CREATE VIEW stats.v_chat_status AS
SELECT c.chat_id, c.title, c.chat_type, c.tracked, c.bot_status,
       (SELECT lower(v.period) FROM tg.coverage v WHERE v.chat_id = c.chat_id AND upper_inf(v.period)) AS admin_since,
       s.sampled_at, s.reported, s.known_open, s.suspect_open,
       s.reported - s.known_open AS mismatch,
       round(100.0 * s.known_open / nullif(s.reported, 0), 2) AS coverage_pct,
       u.members AS known_humans,
       (SELECT max(n.finished_at) FROM tg.snapshot n WHERE n.chat_id = c.chat_id AND n.quality = 'good') AS last_good_snapshot,
       (SELECT count(*) FROM tg.gap g WHERE (g.chat_id = c.chat_id OR g.chat_id IS NULL) AND upper_inf(g.period)) AS open_gaps,
       (SELECT count(*) FROM tg.probe p WHERE p.chat_id = c.chat_id AND p.outcome = 'pending') AS probes_pending
FROM tg.chat c
LEFT JOIN LATERAL (SELECT * FROM tg.count_sample x WHERE x.chat_id = c.chat_id ORDER BY x.sampled_at DESC LIMIT 1) s ON true
LEFT JOIN stats.unique_per_chat u ON u.chat_id = c.chat_id;
COMMENT ON VIEW stats.v_chat_status IS 'Per chat: Telegram''s count, known members, suspect spells, coverage, last good snapshot, open gaps.';

CREATE VIEW stats.v_overlap AS
WITH both_ways AS (
  SELECT chat_a, chat_b, shared, as_of FROM stats.overlap
  UNION ALL
  SELECT chat_b, chat_a, shared, as_of FROM stats.overlap WHERE chat_a <> chat_b
)
SELECT o.chat_a, ca.title AS title_a, o.chat_b, cb.title AS title_b, o.shared,
       da.shared AS size_a, db.shared AS size_b,
       round(100.0 * o.shared / nullif(da.shared, 0), 2) AS pct_of_a,
       round(100.0 * o.shared / nullif(db.shared, 0), 2) AS pct_of_b,
       round(o.shared::numeric / nullif(da.shared + db.shared - o.shared, 0), 4) AS jaccard,
       o.as_of
FROM both_ways o
JOIN stats.overlap da ON da.chat_a = o.chat_a AND da.chat_b = o.chat_a
JOIN stats.overlap db ON db.chat_a = o.chat_b AND db.chat_b = o.chat_b
LEFT JOIN tg.chat ca ON ca.chat_id = o.chat_a
LEFT JOIN tg.chat cb ON cb.chat_id = o.chat_b;
COMMENT ON VIEW stats.v_overlap IS 'Every ordered pair of tracked chats with shared count, % of A, % of B and Jaccard (item 1).';

CREATE VIEW stats.v_chats_per_user AS
SELECT chat_count, users, round(100.0 * users / sum(users) OVER (), 2) AS pct_of_users, as_of
FROM stats.chats_per_user;
COMMENT ON VIEW stats.v_chats_per_user IS 'How many humans are in 1, 2, ... tracked chats (item 2).';

CREATE VIEW stats.v_unique_per_chat AS
SELECT u.chat_id, c.title, u.members, u.exclusive, round(100.0 * u.exclusive / nullif(u.members, 0), 2) AS exclusive_pct, u.as_of
FROM stats.unique_per_chat u LEFT JOIN tg.chat c USING (chat_id);
COMMENT ON VIEW stats.v_unique_per_chat IS 'Members and exclusive members per tracked chat (item 3, scope = all tracked chats).';

CREATE VIEW stats.v_chat_day AS
SELECT d.*, d.reported_eod - lag(d.reported_eod) OVER w AS reported_change,
       (d.reported_eod - lag(d.reported_eod) OVER w) - d.net AS residual,
       sum(d.members_started - d.members_ended) OVER w AS members_eod,
       sum(d.unsure_started - d.unsure_ended) OVER w AS unsure_eod
FROM stats.chat_day d
WINDOW w AS (PARTITION BY d.chat_id ORDER BY d.day ROWS UNBOUNDED PRECEDING);
COMMENT ON VIEW stats.v_chat_day IS
  'Daily growth with the residual against Telegram''s count (item 9), and human members at the end of each day: '
  'members_eod stays held, of which unsure_eod between the last sighting and a confirmed exit (item 16d).';

CREATE VIEW stats.v_drift AS
SELECT s.chat_id, s.sampled_at, s.reported, s.known_open, s.suspect_open, s.net_events,
       s.reported - s.known_open AS mismatch,
       (s.reported - lag(s.reported) OVER w) - s.net_events AS residual
FROM tg.count_sample s
WINDOW w AS (PARTITION BY s.chat_id ORDER BY s.sampled_at);
COMMENT ON VIEW stats.v_drift IS 'Count samples: absolute mismatch and delta residual (D7, item 9).';

CREATE VIEW stats.v_gaps AS
SELECT g.gap_id, g.chat_id, c.title, lower(g.period) AS gap_from, upper(g.period) AS gap_to, g.reason, g.confirmed
FROM tg.gap g LEFT JOIN tg.chat c USING (chat_id);
COMMENT ON VIEW stats.v_gaps IS 'Periods with lost or doubtful events (D6).';

CREATE VIEW stats.v_join_requests_weekly AS
SELECT r.chat_id, date_trunc('week', r.requested_at AT TIME ZONE 'UTC')::date AS week,
       count(*) AS requests,
       count(*) FILTER (WHERE r.outcome = 'approved') AS approved,
       count(*) FILTER (WHERE r.outcome = 'declined') AS declined,
       count(*) FILTER (WHERE r.outcome = 'unknown') AS unknown,
       count(*) FILTER (WHERE r.outcome IS NULL) AS pending,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM r.resolved_at - r.requested_at) / 60)
         FILTER (WHERE r.outcome = 'approved') AS median_minutes_to_approve
FROM tg.join_request r
GROUP BY 1, 2;
COMMENT ON VIEW stats.v_join_requests_weekly IS 'Join-request funnel per chat and week (item 12).';

CREATE VIEW stats.v_campaign_summary AS SELECT * FROM stats.campaign_summary();
COMMENT ON VIEW stats.v_campaign_summary IS 'Giveaway report per campaign and chat, H = 48 h, as of now (item 4).';

CREATE VIEW stats.v_cohort_retention AS
SELECT m.chat_id, date_trunc('week', m.joined_at AT TIME ZONE 'UTC')::date AS cohort_week, h.days,
       count(*) AS cohort,
       count(*) FILTER (WHERE m.joined_at + h.days * interval '1 day' > now()) AS immature,
       count(*) FILTER (WHERE m.joined_at + h.days * interval '1 day' <= now() AND x.state = 'in') AS retained,
       count(*) FILTER (WHERE m.joined_at + h.days * interval '1 day' <= now() AND x.state = 'out') AS exited,
       count(*) FILTER (WHERE m.joined_at + h.days * interval '1 day' <= now() AND x.state = 'unknown') AS unknown
FROM tg.membership m
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
CROSS JOIN (VALUES (7), (30), (90)) h(days)
CROSS JOIN LATERAL (SELECT tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since,
                                       m.joined_at + h.days * interval '1 day') AS state) x
WHERE m.joined_at IS NOT NULL
GROUP BY 1, 2, 3;
COMMENT ON VIEW stats.v_cohort_retention IS 'Cohort retention at 7/30/90 days, mature cohorts only, unknown apart (item 7).';

CREATE VIEW stats.v_join_sources AS
SELECT m.chat_id, m.join_route, m.invite_link_id, l.name AS link_name, l.campaign_id,
       date_trunc('month', m.joined_at AT TIME ZONE 'UTC')::date AS month,
       count(*) AS joins,
       count(*) FILTER (WHERE m.joined_at + interval '7 days' <= now()) AS mature_7d,
       count(*) FILTER (WHERE m.joined_at + interval '7 days' <= now() AND x.state7 = 'in') AS retained_7d,
       count(*) FILTER (WHERE m.joined_at + interval '30 days' <= now()) AS mature_30d,
       count(*) FILTER (WHERE m.joined_at + interval '30 days' <= now() AND x.state30 = 'in') AS retained_30d
FROM tg.membership m
JOIN tg.tg_user u ON u.user_id = m.user_id AND NOT u.is_bot AND u.is_deleted IS NOT TRUE
LEFT JOIN tg.invite_link l ON l.invite_link_id = m.invite_link_id
CROSS JOIN LATERAL (SELECT
  tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, m.joined_at + interval '7 days') AS state7,
  tg.state_at(m.known_from, m.left_after, m.left_before, m.end_source, m.suspect_since, m.joined_at + interval '30 days') AS state30) x
WHERE m.joined_at IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6;
COMMENT ON VIEW stats.v_join_sources IS 'Joins and 7/30-day retention per join source and month (item 8).';

------------------------------------------------------------------------------------------------------------
-- Grants
------------------------------------------------------------------------------------------------------------
-- This file runs as the tenant login (owner of the database and of public; NOSUPERUSER, NOCREATEDB,
-- NOCREATEROLE), so it creates no role. The optional roles bi_reader and collect_writer are created by an
-- admin with admin.sql. Their grants are applied here when the roles already exist, and by tg.apply_grants()
-- (called at the end of admin.sql) when they are created later. Either order works.

CREATE FUNCTION tg.apply_grants() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_done text[] := '{}';
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader') THEN
    -- BI: the stats schema only. Views run with their owner's rights, so bi_reader never reads tg directly.
    GRANT USAGE ON SCHEMA stats TO bi_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA stats TO bi_reader;
    GRANT EXECUTE ON FUNCTION stats.campaign_summary(numeric, timestamptz, bigint) TO bi_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA stats GRANT SELECT ON TABLES TO bi_reader;
    v_done := v_done || 'bi_reader'::text;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collect_writer') THEN
    -- The collect script (snapshot contract, design.md 5): add a missing chat, write a snapshot and its
    -- members, mark it finished. Nothing else.
    GRANT USAGE ON SCHEMA tg TO collect_writer;
    GRANT SELECT (chat_id) ON tg.chat TO collect_writer;
    GRANT INSERT (chat_id, chat_type, title, username) ON tg.chat TO collect_writer;
    GRANT SELECT (snapshot_id, chat_id, finished_at) ON tg.snapshot TO collect_writer;
    GRANT INSERT (chat_id, started_at, method, reported_at_start) ON tg.snapshot TO collect_writer;
    GRANT UPDATE (finished_at, reported_at_end, fetched, complete) ON tg.snapshot TO collect_writer;
    GRANT INSERT ON tg.snapshot_member TO collect_writer;
    v_done := v_done || 'collect_writer'::text;
  END IF;
  RETURN 'grants applied for: ' || coalesce(nullif(array_to_string(v_done, ', '), ''), 'no role yet');
END $$;
REVOKE EXECUTE ON FUNCTION tg.apply_grants() FROM PUBLIC;

SELECT tg.apply_grants();

COMMIT;
