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
