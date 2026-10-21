import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.settings import settings


MIGRATION_PATH = Path(__file__).with_name("migrations") / "001_create_notification_storage.sql"
DELIVERY_STATUSES = {"queued", "sent", "failed", "retry", "dlq"}
DELIVERY_TRANSITIONS = {
    "queued": {"sent", "failed", "retry"},
    "retry": {"sent", "failed", "retry", "dlq"},
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def json_value(value: Any) -> Jsonb:
    return Jsonb(json.loads(json.dumps(value, default=str)))


def _template(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "templateCode": row["template_code"],
        "channel": row["channel"],
        "subject": row["subject"],
        "body": row["body"],
        "version": row["version"],
        "active": row["active"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def _delivery(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "notificationId": str(row["notification_id"]),
        "recipient": row["recipient"],
        "channel": row["channel"],
        "templateCode": row["template_code"],
        "templateVersion": row["template_version"],
        "payload": row["payload"],
        "templateVariables": row["template_variables"],
        "status": row["status"],
        "retryCount": row["retry_count"],
        "errorMessage": row["error_message"],
        "correlationId": row["correlation_id"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
        "sentAt": row["sent_at"],
    }


class NotificationStorage:
    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(settings.database_url, row_factory=dict_row) as connection:
            yield connection

    def migrate(self) -> None:
        with self.connection() as connection:
            connection.execute(MIGRATION_PATH.read_text(encoding="utf-8"))

    def check(self) -> None:
        with self.connection() as connection:
            connection.execute("SELECT 1")

    def upsert_active_template(self, template_code: str, subject: str, body: str) -> dict[str, Any]:
        now = utcnow()
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM notification_templates
                WHERE template_code = %s AND channel = 'email' AND active
                ORDER BY version DESC LIMIT 1
                """,
                (template_code,),
            ).fetchone()
            if row:
                if row["subject"] == subject and row["body"] == body:
                    return _template(row)
                next_version = row["version"] + 1
                connection.execute(
                    """
                    UPDATE notification_templates
                    SET active = FALSE, updated_at = %s
                    WHERE template_code = %s AND channel = 'email' AND active
                    """,
                    (now, template_code),
                )
                row = connection.execute(
                    """
                    INSERT INTO notification_templates
                        (template_code, channel, version, subject, body, active, created_at, updated_at)
                    VALUES (%s, 'email', %s, %s, %s, TRUE, %s, %s)
                    RETURNING *
                    """,
                    (template_code, next_version, subject, body, now, now),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    INSERT INTO notification_templates
                        (template_code, channel, version, subject, body, active, created_at, updated_at)
                    VALUES (%s, 'email', 1, %s, %s, TRUE, %s, %s)
                    RETURNING *
                    """,
                    (template_code, subject, body, now, now),
                ).fetchone()
            return _template(row)

    def get_active_template(self, template_code: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM notification_templates
                WHERE template_code = %s AND channel = 'email' AND active
                ORDER BY version DESC LIMIT 1
                """,
                (template_code,),
            ).fetchone()
            return _template(row) if row else None

    def create_delivery(
        self,
        notification_id: str,
        recipient: str,
        template: dict[str, Any],
        payload: dict[str, Any],
        correlation_id: str,
    ) -> tuple[dict[str, Any], bool]:
        now = utcnow()
        with self.connection() as connection:
            try:
                row = connection.execute(
                    """
                    INSERT INTO notification_deliveries (
                        notification_id, recipient, channel, template_code, template_version,
                        payload, template_variables, status, retry_count, correlation_id,
                        created_at, updated_at
                    ) VALUES (%s, %s, 'email', %s, %s, %s, %s, 'queued', 0, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        notification_id,
                        recipient,
                        template["templateCode"],
                        template["version"],
                        json_value(payload),
                        json_value({}),
                        correlation_id,
                        now,
                        now,
                    ),
                ).fetchone()
            except UniqueViolation:
                connection.rollback()
                row = connection.execute(
                    "SELECT * FROM notification_deliveries WHERE notification_id = %s",
                    (notification_id,),
                ).fetchone()
                if row is None:
                    raise
                return _delivery(row), False
            return _delivery(row), True

    def get_delivery(self, notification_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM notification_deliveries WHERE notification_id = %s",
                (notification_id,),
            ).fetchone()
            return _delivery(row) if row else None

    def update_delivery(
        self,
        notification_id: str,
        status: str,
        error_message: str | None = None,
        retry_count: int | None = None,
    ) -> dict[str, Any] | None:
        if status not in DELIVERY_STATUSES:
            raise ValueError(f"Unsupported delivery status: {status}")
        now = utcnow()
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM notification_deliveries WHERE notification_id = %s FOR UPDATE",
                (notification_id,),
            ).fetchone()
            if row is None:
                return None
            if status not in DELIVERY_TRANSITIONS.get(row["status"], set()):
                return _delivery(row)
            next_retry_count = row["retry_count"] if retry_count is None else retry_count
            row = connection.execute(
                """
                UPDATE notification_deliveries
                SET status = %s, retry_count = %s, error_message = %s,
                    sent_at = CASE WHEN %s = 'sent' THEN %s ELSE sent_at END,
                    updated_at = %s
                WHERE notification_id = %s
                RETURNING *
                """,
                (status, next_retry_count, error_message, status, now, now, notification_id),
            ).fetchone()
            return _delivery(row)


notification_storage = NotificationStorage()
