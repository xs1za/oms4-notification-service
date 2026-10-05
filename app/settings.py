from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS4"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    kafka_shift_status_group_id: str = "oms4.shift-status-notifications"
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_username: str = ""
    smtp_password: str = ""
    email_from: str = "noreply@example.local"
    rabbitmq_url: str = "amqp://oms:oms@rabbitmq.oms.svc.cluster.local:5672/%2F"
    rabbitmq_exchange: str = "oms.commands"
    rabbitmq_dlx_exchange: str = "oms.commands.dlx"
    email_send_queue: str = "oms4.email.send"
    email_retry_queue: str = "oms4.email.send.retry"
    email_dlq_queue: str = "oms4.email.send.dlq"
    email_retry_delay_ms: int = 30000
    email_max_retries: int = 3


settings = Settings()
