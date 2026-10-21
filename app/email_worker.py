import json
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika

from app.email_delivery import send_email_message
from app.kafka import publish_event
from app.logging_config import configure_logging
from app.rabbitmq import EMAIL_DLQ_ROUTING_KEY, EMAIL_RETRY_ROUTING_KEY, create_connection, declare_email_topology
from app.settings import settings
from app.notification_storage import notification_storage

configure_logging()
logger = logging.getLogger(__name__)


def _publish_retry(channel: pika.channel.Channel, command: dict, retry_count: int) -> None:
    channel.basic_publish(
        exchange=settings.rabbitmq_exchange,
        routing_key=EMAIL_RETRY_ROUTING_KEY,
        body=json.dumps(command, default=str).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2, headers={"retry_count": retry_count}),
    )


def _publish_dlq(channel: pika.channel.Channel, command: dict, error: str, retry_count: int) -> None:
    command = command | {"lastError": error, "failedAt": datetime.now(timezone.utc), "retryCount": retry_count}
    channel.basic_publish(
        exchange=settings.rabbitmq_dlx_exchange,
        routing_key=EMAIL_DLQ_ROUTING_KEY,
        body=json.dumps(command, default=str).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def _publish_status(command: dict, status: str, error: str | None = None) -> None:
    publish_event(
        "notification.email_status",
        {
            "notification_id": command.get("commandId", str(uuid4())),
            "channel": "email",
            "to": command.get("payload", {}).get("to"),
            "status": status,
            "error": error,
            "created_at": datetime.now(timezone.utc),
        },
    )


def process_message(channel: pika.channel.Channel, method, properties: pika.BasicProperties, body: bytes) -> None:
    retry_count = int((properties.headers or {}).get("retry_count", 0))
    try:
        command = json.loads(body.decode("utf-8"))
        payload = command["payload"]
        result = send_email_message(
            to=payload["to"],
            subject=payload["subject"],
            body=payload["body"],
            notification_id=command.get("commandId"),
        )
        if result["status"] != "sent":
            if result.get("permanent"):
                notification_storage.update_delivery(command.get("commandId"), "failed", result.get("error"))
                _publish_status(command, "failed", result.get("error"))
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            raise RuntimeError(result.get("error") or "SMTP delivery failed")
        notification_storage.update_delivery(command.get("commandId"), "sent")
        _publish_status(command, "sent")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:  # noqa: BLE001 - command worker must isolate message failures.
        logger.exception("Failed to process email command")
        command = json.loads(body.decode("utf-8")) if body else {}
        retry_count += 1
        if retry_count <= settings.email_max_retries:
            notification_storage.update_delivery(command.get("commandId"), "retry", str(exc), retry_count)
            _publish_retry(channel, command, retry_count)
            _publish_status(command, "retry", str(exc))
        else:
            notification_storage.update_delivery(command.get("commandId"), "dlq", str(exc), retry_count)
            _publish_dlq(channel, command, str(exc), retry_count)
            _publish_status(command, "dlq", str(exc))
        channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    while True:
        try:
            with create_connection() as connection:
                channel = connection.channel()
                declare_email_topology(channel)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=settings.email_send_queue, on_message_callback=process_message)
                logger.info("OMS4 email worker consuming queue %s", settings.email_send_queue)
                channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.exception("RabbitMQ is unavailable; retrying worker connection")
            time.sleep(5)


if __name__ == "__main__":
    main()
