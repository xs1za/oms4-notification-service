# OMS4 Notification Service

`OMS4` - микросервис информирования пользователей системы. Начальная реализация поддерживает один канал: email.

## Функции

- Прием запроса на отправку email.
- Постановка команды `email.send` в RabbitMQ.
- Асинхронная отправка email через lightweight worker без Celery.
- Retry и DLQ для неуспешной доставки.
- Публикация статусов уведомлений в Kafka.
- Базовый health check.

## Технологии

- Python 3.11
- FastAPI
- Uvicorn
- SMTP через стандартный модуль `smtplib`
- confluent-kafka
- RabbitMQ через `pika`

## API

### Health check

```http
GET /health
```

### Отправить email

```http
POST /notifications/email
Content-Type: application/json
```

Тело запроса:

```json
{
  "to": "user@example.com",
  "subject": "OMS notification",
  "body": "Test email notification from OMS4."
}
```

Ответ при успешной постановке в очередь:

```json
{
  "notification_id": "uuid",
  "channel": "email",
  "to": "user@example.com",
  "status": "queued",
  "created_at": "2026-01-20T09:00:00Z"
}
```

Если RabbitMQ недоступен, API вернет `503`. Если SMTP недоступен, worker выполнит retry и затем отправит команду в DLQ `oms4.email.send.dlq`.

## Kafka events

- `notification.email_status` - статус email: `queued`, `sent`, `failed`.

Consumer:

- `operations.shift.status_changed` - изменение статуса смены; `OMS4` принимает решение о необходимости уведомления и обеспечивает идемпотентность по `event_id`.

## RabbitMQ queues

- `oms4.email.send` - основная очередь команд на отправку email.
- `oms4.email.send.retry` - retry-очередь с TTL и возвратом в основную очередь.
- `oms4.email.send.dlq` - dead-letter queue после исчерпания retry.

## Переменные окружения

| Переменная | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `SERVICE_NAME` | `OMS4` | Имя сервиса |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.oms.svc.cluster.local:9092` | Kafka bootstrap servers |
| `KAFKA_SHIFT_STATUS_GROUP_ID` | `oms4.shift-status-notifications` | Consumer group для `operations.shift.status_changed` |
| `SMTP_HOST` | `localhost` | SMTP host |
| `SMTP_PORT` | `1025` | SMTP port |
| `SMTP_USERNAME` | пусто | SMTP пользователь |
| `SMTP_PASSWORD` | пусто | SMTP пароль |
| `EMAIL_FROM` | `noreply@example.local` | Отправитель писем |
| `RABBITMQ_URL` | `amqp://oms:oms@rabbitmq.oms.svc.cluster.local:5672/%2F` | RabbitMQ connection URL |
| `RABBITMQ_EXCHANGE` | `oms.commands` | Exchange команд |
| `RABBITMQ_DLX_EXCHANGE` | `oms.commands.dlx` | Exchange DLQ |
| `EMAIL_SEND_QUEUE` | `oms4.email.send` | Основная email queue |
| `EMAIL_RETRY_QUEUE` | `oms4.email.send.retry` | Retry queue |
| `EMAIL_DLQ_QUEUE` | `oms4.email.send.dlq` | DLQ queue |
| `EMAIL_RETRY_DELAY_MS` | `30000` | Задержка retry |
| `EMAIL_MAX_RETRIES` | `3` | Максимум retry перед DLQ |

## Локальный запуск

Для локального тестирования удобно поднять MailHog или аналогичный SMTP-сервер.

```bash
docker run --rm -p 1025:1025 -p 8025:8025 mailhog/mailhog
```

Запуск сервиса:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
set SMTP_HOST=localhost
set SMTP_PORT=1025
uvicorn app.main:app --reload --host 0.0.0.0 --port 8004
```

Встроенный Swagger UI FastAPI для OMS4:

```text
http://localhost:8004/docs
```

Это Swagger только текущего сервиса OMS4. Общая Swagger UI страница для единой спецификации `platform/contracts/openapi_oms_microservices.json` запускается отдельно из корня проекта и открывается без `/docs`:

```powershell
docker run --rm `
  --name oms-swagger-ui `
  -p 8088:8080 `
  -e SWAGGER_JSON=/spec/platform/contracts/openapi_oms_microservices.json `
  -v "D:/ProjectsDocker/extrawork:/spec" `
  swaggerapi/swagger-ui:v5.17.14
```

```text
http://localhost:8088
```

MailHog UI, если MailHog запущен локально командой выше:

```text
http://localhost:8025
```

Через Ingress сервис доступен без port-forward. Встроенный Swagger UI OMS4:

```text
http://oms.local/oms4/docs
```

## Docker

```bash
docker build -t oms4:latest .
docker run --rm -p 8004:8000 -e SMTP_HOST=host.docker.internal -e SMTP_PORT=1025 oms4:latest
```

## Kubernetes

```bash
kubectl apply -f ../platform/k8s/namespace.yaml
kubectl apply -f ../platform/k8s/rabbitmq-dev.yaml
copy k8s\secret.example.yaml k8s\secret.yaml
kubectl apply -f k8s/
kubectl -n oms port-forward svc/oms4 8004:80
```

После port-forward встроенный Swagger UI OMS4 доступен по адресу:

```text
http://localhost:8004/docs
```
