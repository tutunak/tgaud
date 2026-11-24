-- 007_ingest.sql (ingest task): what only ingest uses. Additive only; runs as the tenant login, the runner owns
-- the transaction.

------------------------------------------------------------------------------------------------------------
-- getChat metadata queue
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.chat ADD COLUMN metadata_due_at timestamptz;
COMMENT ON COLUMN tg.chat.metadata_due_at IS
  'When ingest should (re)fetch getChat metadata (type, title, username, linked_chat_id, join_by_request, '
  'has_hidden_members) for this chat; NULL = nothing pending. Set when a chat is first seen or becomes tracked '
  'and for every tracked chat at startup; the update that set it commits without waiting for the fetch. '
  'Ingest fetches due chats between polls and retries a failed fetch with backoff until it succeeds.';

ALTER TABLE tg.chat ADD COLUMN metadata_attempts integer NOT NULL DEFAULT 0 CHECK (metadata_attempts >= 0);
COMMENT ON COLUMN tg.chat.metadata_attempts IS
  'Failed getChat attempts since the last success (the backoff of metadata_due_at); 0 after a success.';

CREATE INDEX chat_metadata_due ON tg.chat (metadata_due_at) WHERE metadata_due_at IS NOT NULL;

------------------------------------------------------------------------------------------------------------
-- Rights check queue
------------------------------------------------------------------------------------------------------------

ALTER TABLE tg.chat ADD COLUMN rights_due_at timestamptz;
COMMENT ON COLUMN tg.chat.rights_due_at IS
  'When ingest should check the bot''s rights here (getChatMember(chat, bot)); NULL = nothing pending. Set for '
  'every tracked chat at startup and for a chat first seen; a failed check is retried with backoff, and until a '
  'check or a newer my_chat_member resolves it the chat carries a suspected not_admin gap (confirmed false). '
  'Never set for the old group of a migration.';

ALTER TABLE tg.chat ADD COLUMN rights_attempts integer NOT NULL DEFAULT 0 CHECK (rights_attempts >= 0);
COMMENT ON COLUMN tg.chat.rights_attempts IS
  'Failed rights checks since the last success (the backoff of rights_due_at); 0 after a success.';

ALTER TABLE tg.chat ADD COLUMN rights_failed_at timestamptz;
COMMENT ON COLUMN tg.chat.rights_failed_at IS
  'The first failed rights check since the last success; NULL = none pending. A my_chat_member dated before it '
  'does not cancel the retry (it says nothing about the state at the failure); a successful check or a newer '
  'event clears it.';

CREATE INDEX chat_rights_due ON tg.chat (rights_due_at) WHERE rights_due_at IS NOT NULL;
