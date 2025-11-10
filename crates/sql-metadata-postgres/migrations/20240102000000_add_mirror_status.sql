-- Migration: Add mirror status tracking table
-- Tracks the mirroring progress for each committed version.
-- Enables reconciliation to repair failed mirrors and monitor lag.

CREATE TABLE IF NOT EXISTS dl_mirror_status (
    table_id UUID NOT NULL,
    version BIGINT NOT NULL,
    
    -- Status: PENDING (in progress), SUCCESS (complete), FAILED (needs retry)
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')),
    
    -- Track partial failures (JSON written but checkpoint failed)
    json_written BOOLEAN NOT NULL DEFAULT FALSE,
    checkpoint_written BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Error details for debugging
    error_message TEXT,
    
    -- Retry tracking
    retry_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    
    -- Lifecycle
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Primary key ensures one entry per version
    PRIMARY KEY (table_id, version),
    
    -- Foreign key to ensure referential integrity
    FOREIGN KEY (table_id) REFERENCES dl_tables(table_id) ON DELETE CASCADE
);

-- Index for finding failed/pending mirrors (used by reconciler)
CREATE INDEX idx_dl_mirror_status_failed 
    ON dl_mirror_status(table_id, version) 
    WHERE status IN ('FAILED', 'PENDING');

-- Index for lag detection (find versions not yet successfully mirrored)
CREATE INDEX idx_dl_mirror_status_pending_by_time
    ON dl_mirror_status(table_id, last_attempt_at DESC) 
    WHERE status != 'SUCCESS';

-- Index for metrics queries (find oldest pending/failed per table)
CREATE INDEX idx_dl_mirror_status_latest_attempt
    ON dl_mirror_status(table_id, status, last_attempt_at DESC);
