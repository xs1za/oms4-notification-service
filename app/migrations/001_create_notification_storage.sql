CREATE TABLE IF NOT EXISTS notification_templates (
    template_code TEXT NOT NULL,
    channel TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (template_code, channel, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS notification_templates_one_active_idx
    ON notification_templates (template_code, channel)
    WHERE active;

CREATE TABLE IF NOT EXISTS notification_deliveries (
    notification_id UUID PRIMARY KEY,
    recipient TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel = 'email'),
    template_code TEXT NOT NULL,
    template_version INTEGER NOT NULL CHECK (template_version > 0),
    payload JSONB NOT NULL,
    template_variables JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('queued', 'sent', 'failed', 'retry', 'dlq')),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    error_message TEXT,
    correlation_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS notification_deliveries_status_updated_idx
    ON notification_deliveries (status, updated_at);

CREATE INDEX IF NOT EXISTS notification_deliveries_recipient_idx
    ON notification_deliveries (recipient);
