# OMS4 Backlog

- Добавить постоянное хранилище notification/email templates и статусов доставки.
- Добавить авторизацию через `OMS1` для API отправки уведомлений и внутренних команд.
- Добавить обработчики событий `OMS5` для отправки шаблонных писем по offer/assignment/timesheet сценариям.
- Добавить мониторинг RabbitMQ queues, retry и DLQ с процедурой ручного requeue.
- Добавить MVP email template codes: `shift_offer`, `shift_offer_accepted`, `shift_offer_lost`, `shift_assignment_confirmed`, `shift_assignment_cancelled`, `shift_absence_review_required`, `shift_reminder`, `timesheet_required`, `report_ready`.
- Добавить каналы SMS, push и Telegram после стабилизации email-канала.
