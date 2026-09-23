# OMS4 Notification Service

`OMS4` - микросервис информирования пользователей системы. Начальная реализация поддерживает один канал: email.

## Функции

- Прием запроса на отправку email.
- Отправка email через SMTP.
- Публикация результата отправки в Kafka.
- Базовый health check.

## Технологии

- Python 3.11
- FastAPI
- Uvicorn
- SMTP через стандартный модуль `smtplib`
- confluent-kafka

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

Ответ при успешной постановке/попытке отправки:

```json
{
  "notification_id": "uuid",
  "channel": "email",
  "to": "user@example.com",
  "status": "sent",
  "created_at": "2026-01-20T09:00:00Z"
}
```

Если SMTP недоступен, сервис вернет `status: failed` и опубликует соответствующее событие.

## Kafka events

- `notification.email_status` - результат отправки email.

## Переменные окружения

| Переменная | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `SERVICE_NAME` | `OMS4` | Имя сервиса |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.oms.svc.cluster.local:9092` | Kafka bootstrap servers |
| `SMTP_HOST` | `localhost` | SMTP host |
| `SMTP_PORT` | `1025` | SMTP port |
| `SMTP_USERNAME` | пусто | SMTP пользователь |
| `SMTP_PASSWORD` | пусто | SMTP пароль |
| `EMAIL_FROM` | `noreply@example.local` | Отправитель писем |

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

Браузером можно открыть `GET /health`; отправку email нужно вызывать как `POST` из Swagger UI, Postman или `curl`:

```text
GET  http://oms.local/oms4/health
POST http://oms.local/oms4/notifications/email
```

## Docker

```bash
docker build -t oms4:latest .
docker run --rm -p 8004:8000 -e SMTP_HOST=host.docker.internal -e SMTP_PORT=1025 oms4:latest
```

## Kubernetes

```bash
kubectl apply -f ../platform/k8s/namespace.yaml
copy k8s\secret.example.yaml k8s\secret.yaml
kubectl apply -f k8s/
kubectl -n oms port-forward svc/oms4 8004:80
```

После port-forward встроенный Swagger UI OMS4 доступен по адресу:

```text
http://localhost:8004/docs
```

## Дальнейшее развитие

- Kafka использовать для событий уведомлений. Для очереди команд `send_email` с retry/DLQ лучше рассмотреть RabbitMQ/Celery.
- Добавить шаблоны писем.
- Добавить каналы SMS, push, Telegram.
- Добавить retry policy и dead-letter topic.
- Добавить авторизацию через `OMS1`.
