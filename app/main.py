from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from app.healthcheck.router import router as healthcheck_router
from app.kafka import publish_event
from app.rabbitmq import enqueue_email_command
from app.settings import settings

app = FastAPI(title="OMS4 Notification Service", version="0.2.0", root_path=settings.root_path)
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
    command_id = str(uuid4())
    command = {
        "commandId": command_id,
        "commandType": "email.send",
        "correlationId": f"corr_{uuid4().hex}",
        "createdAt": datetime.now(timezone.utc),
        "payload": {"to": payload.to, "subject": payload.subject, "body": payload.body},
    }
    try:
        enqueue_email_command(command)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Email command queue is unavailable") from exc
    event = {"notification_id": command_id, "channel": "email", "to": payload.to, "status": "queued", "created_at": datetime.now(timezone.utc)}
    publish_event("notification.email_status", event)
    return event
