from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS4"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_username: str = ""
    smtp_password: str = ""
    email_from: str = "noreply@example.local"


settings = Settings()
