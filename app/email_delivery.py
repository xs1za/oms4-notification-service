import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from uuid import uuid4

from app.settings import settings


def send_email_message(to: str, subject: str, body: str, notification_id: str | None = None) -> dict:
    notification_id = notification_id or str(uuid4())
    message = EmailMessage()
    message["From"] = settings.email_from
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body)

    status = "sent"
    error = None
    permanent = False
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
            if settings.smtp_username:
                smtp.login(settings.smtp_username, settings.smtp_password)
            smtp.send_message(message)
    except (smtplib.SMTPAuthenticationError, smtplib.SMTPDataError, smtplib.SMTPRecipientsRefused) as exc:
        status = "failed"
        error = str(exc)
        permanent = True
    except Exception as exc:  # noqa: BLE001 - worker retries transient SMTP failures.
        status = "failed"
        error = str(exc)
        permanent = False

    event = {
        "notification_id": notification_id,
        "channel": "email",
        "to": to,
        "status": status,
        "error": error,
        "permanent": permanent,
        "created_at": datetime.now(timezone.utc),
    }
    return event
