import logging
from datetime import datetime, timezone
from threading import Thread
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from app.healthcheck.router import router as healthcheck_router
from app.kafka import consume_events, publish_event
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

logger = logging.getLogger(__name__)
processed_shift_status_events: set[str] = set()
shift_status_notification_decisions: list[dict] = []


class EmailNotification(BaseModel):
    to: EmailStr
    subject: str
    body: str


def shift_status_requires_notification(event: dict) -> bool:
    return event.get("new_status") in {"A30_CHOICE", "A40_EXECUTION", "A80_CLOSED"}


def handle_shift_status_changed(event: dict) -> None:
    if event.get("event_type") != "operations.shift.status_changed":
        raise ValueError("Unsupported event_type")
    if event.get("schema_version") != 1:
        raise ValueError("Unsupported schema_version")
    event_id = event.get("event_id")
    shift_id = event.get("shift_id")
    if not event_id or not shift_id:
        raise ValueError("event_id and shift_id are required")
    if event_id in processed_shift_status_events:
        logger.info("Skipping duplicate shift status event", extra={"event_id": event_id, "shift_id": shift_id})
        return
    decision = {
        "event_id": event_id,
        "correlation_id": event.get("correlation_id"),
        "shift_id": shift_id,
        "previous_status": event.get("previous_status"),
        "new_status": event.get("new_status"),
        "reason": event.get("reason"),
        "notification_required": shift_status_requires_notification(event),
        "decided_at": datetime.now(timezone.utc),
    }
    shift_status_notification_decisions.append(decision)
    processed_shift_status_events.add(event_id)
    logger.info("Processed shift status notification decision", extra={"event_id": event_id, "shift_id": shift_id})


def start_shift_status_consumer() -> None:
    consume_events("operations.shift.status_changed", settings.kafka_shift_status_group_id, handle_shift_status_changed)


@app.on_event("startup")
def start_consumers() -> None:
    Thread(target=start_shift_status_consumer, daemon=True).start()


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
