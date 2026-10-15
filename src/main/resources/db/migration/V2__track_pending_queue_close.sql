ALTER TABLE gates ADD COLUMN queue_close_pending BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE gates SET queue_close_pending = TRUE WHERE status = 'CLOSED';

CREATE INDEX gates_pending_queue_close ON gates (id) WHERE queue_close_pending = TRUE;
