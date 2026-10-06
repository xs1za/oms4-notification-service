import logging
from datetime import datetime, timezone
from threading import Thread
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from app.healthcheck.router import router as healthcheck_router
from app.kafka import consume_events, publish_event
from app.logging_config import configure_logging
from app.problem_events import (
    EventProcessingError,
    enqueue_manual_reprocess,
    get_problem_event,
    list_problem_events,
    mark_manual_status,
    register_problem_event,
)
from app.rabbitmq import enqueue_email_command
from app.settings import settings

configure_logging()

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


class ManualProblemEventAction(BaseModel):
    comment: str


def require_operations_role(x_operational_role: str | None) -> None:
    if x_operational_role != "operations":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Operations role is required")


def shift_status_requires_notification(event: dict) -> bool:
    return event.get("new_status") in {"A30_CHOICE", "A40_EXECUTION", "A80_CLOSED"}


def process_shift_status_changed_event(event: dict) -> None:
    if event.get("event_type") != "operations.shift.status_changed":
        raise EventProcessingError("contract", "unsupported_event_type", "Unsupported event_type")
    if event.get("schema_version") != 1:
        raise EventProcessingError("contract", "unsupported_schema_version", "Unsupported schema_version")
    event_id = event.get("event_id")
    shift_id = event.get("shift_id")
    if not event_id or not shift_id:
        raise EventProcessingError("contract", "required_field_missing", "event_id and shift_id are required")
    if event.get("external_store_id") == "missing":
        raise EventProcessingError("business", "store_not_found", "Store is not registered in platform")
    if event.get("reason") == "force_technical_error":
        raise EventProcessingError("technical", "temporary_dependency_error", "Temporary dependency error")
    if event_id in processed_shift_status_events:
        logger.info(
            "Skipping duplicate shift status event",
            extra={"event_id": event_id, "correlation_id": event.get("correlation_id"), "shift_id": shift_id},
        )
        return
    notification_required = shift_status_requires_notification(event)
    decision = {
        "event_id": event_id,
        "correlation_id": event.get("correlation_id"),
        "shift_id": shift_id,
        "previous_status": event.get("previous_status"),
        "new_status": event.get("new_status"),
        "reason": event.get("reason"),
        "notification_required": notification_required,
        "decided_at": datetime.now(timezone.utc),
    }
    shift_status_notification_decisions.append(decision)
    processed_shift_status_events.add(event_id)
    logger.info(
        "Processed shift status notification decision",
        extra={
            "event_id": event_id,
            "correlation_id": event.get("correlation_id"),
            "shift_id": shift_id,
            "notification_required": notification_required,
        },
    )


def handle_shift_status_changed(event: dict, metadata: dict | None = None) -> None:
    try:
        process_shift_status_changed_event(event)
    except EventProcessingError as exc:
        register_problem_event(event, exc, settings.service_name, metadata)


def start_shift_status_consumer() -> None:
    consume_events("operations.shift.status_changed", settings.kafka_shift_status_group_id, handle_shift_status_changed)


@app.on_event("startup")
def start_consumers() -> None:
    Thread(target=start_shift_status_consumer, daemon=True).start()


@app.get("/admin/problem-events")
def get_problem_events(
    x_operational_role: str | None = Header(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    error_code: str | None = None,
    error_type: str | None = None,
    shift_id: str | None = None,
    event_type: str | None = None,
) -> list[dict]:
    require_operations_role(x_operational_role)
    items = list_problem_events()
    if status_filter:
        items = [item for item in items if item.get("status") == status_filter]
    if error_code:
        items = [item for item in items if item.get("error_code") == error_code]
    if error_type:
        items = [item for item in items if item.get("error_type") == error_type]
    if shift_id:
        items = [item for item in items if item.get("shift_id") == shift_id]
    if event_type:
        items = [item for item in items if item.get("event_type") == event_type]
    return items


@app.get("/admin/problem-events/{problem_event_id}")
def get_problem_event_detail(problem_event_id: str, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    item = get_problem_event(problem_event_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return item


@app.post("/admin/problem-events/{problem_event_id}/reprocess")
def reprocess_problem_event_api(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return enqueue_manual_reprocess(problem_event_id, "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/ignore")
def ignore_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "ignored", "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/manual-review")
def manual_review_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "manual_review", "operations", payload.comment)


@app.post("/admin/problem-events/{problem_event_id}/dlq")
def dlq_problem_event(problem_event_id: str, payload: ManualProblemEventAction, x_operational_role: str | None = Header(default=None)) -> dict:
    require_operations_role(x_operational_role)
    if get_problem_event(problem_event_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Problem event not found")
    return mark_manual_status(problem_event_id, "dlq", "operations", payload.comment)


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
