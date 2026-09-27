import json
from typing import Any

import pika

from app.settings import settings

EMAIL_SEND_ROUTING_KEY = "email.send"
EMAIL_RETRY_ROUTING_KEY = "email.send.retry"
EMAIL_DLQ_ROUTING_KEY = "email.send.dlq"


def create_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))


def declare_email_topology(channel: pika.channel.Channel) -> None:
    channel.exchange_declare(exchange=settings.rabbitmq_exchange, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=settings.rabbitmq_dlx_exchange, exchange_type="direct", durable=True)
    channel.queue_declare(queue=settings.email_send_queue, durable=True)
    channel.queue_bind(queue=settings.email_send_queue, exchange=settings.rabbitmq_exchange, routing_key=EMAIL_SEND_ROUTING_KEY)

    channel.queue_declare(
        queue=settings.email_retry_queue,
        durable=True,
        arguments={
            "x-message-ttl": settings.email_retry_delay_ms,
            "x-dead-letter-exchange": settings.rabbitmq_exchange,
            "x-dead-letter-routing-key": EMAIL_SEND_ROUTING_KEY,
        },
    )
    channel.queue_bind(queue=settings.email_retry_queue, exchange=settings.rabbitmq_exchange, routing_key=EMAIL_RETRY_ROUTING_KEY)

    channel.queue_declare(queue=settings.email_dlq_queue, durable=True)
    channel.queue_bind(queue=settings.email_dlq_queue, exchange=settings.rabbitmq_dlx_exchange, routing_key=EMAIL_DLQ_ROUTING_KEY)


def enqueue_email_command(command: dict[str, Any]) -> None:
    with create_connection() as connection:
        channel = connection.channel()
        declare_email_topology(channel)
        channel.basic_publish(
            exchange=settings.rabbitmq_exchange,
            routing_key=EMAIL_SEND_ROUTING_KEY,
            body=json.dumps(command, default=str).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2, headers={"retry_count": 0}),
        )
