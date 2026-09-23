import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from app.healthcheck.router import router as healthcheck_router
from app.kafka import publish_event
from app.settings import settings

app = FastAPI(title="OMS4 Notification Service", version="0.1.0", root_path=settings.root_path)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8088", "http://127.0.0.1:8088"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(healthcheck_router)


class EmailNotification(BaseModel):
    to: EmailStr
    subject: str
    body: str


@app.post("/notifications/email")
def send_email(payload: EmailNotification) -> dict:
    notification_id = str(uuid4())
    message = EmailMessage()
    message["From"] = settings.email_from
    message["To"] = payload.to
    message["Subject"] = payload.subject
    message.set_content(payload.body)

    status = "sent"
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
            if settings.smtp_username:
                smtp.login(settings.smtp_username, settings.smtp_password)
            smtp.send_message(message)
    except Exception:
        status = "failed"

    event = {"notification_id": notification_id, "channel": "email", "to": payload.to, "status": status, "created_at": datetime.now(timezone.utc)}
    publish_event("notification.email_status", event)
    return event
