# Спортзал — практические №5–8, вариант 3, RabbitMQ

Это **учебная техническая реализация** по предметной модели практик №1–4. В исходном отчёте определены шесть агрегатов и четыре ограниченных контекста; на их базе подготовлены шесть отдельных API-сервисов. Пользователь уточнил выбор **RabbitMQ вместо ранее указанного Kafka** для асинхронных событий. Синхронные проверки через REST оставлены, поскольку проектирование REST API — отдельная практика №6.

## Практическая №5: индивидуальный вариант 3

По таблице методички: очередь `auto_delete=True`, обменник `fanout`, задержка обработки `random.randint(1, 5)` секунд. Для этой демонстрации очередь НЕ является устойчивой: при прекращении работы всех потребителей после хотя бы одного подключения она удаляется. Не путать с рабочими очередями доменных событий, где используются устойчивые очереди и `topic` для фильтрации событий (это отдельная проектная настройка RabbitMQ).

Запуск из корня проекта после `docker compose up --build -d`:

```bash
# Linux/macOS
export AMQP_URL='amqp://gym:gym_dev_only@localhost:5672/'
# Windows PowerShell
$env:AMQP_URL='amqp://gym:gym_dev_only@localhost:5672/'
```

Терминал №1 (запустить **сначала**, ожидает сообщение):

```bash
python -m rabbitmq.exercise --mode exchange --variant 3 --role consumer --queue-name ikbo-66-23_zuev
```

Терминал №2 (после старта потребителя):

```bash
python -m rabbitmq.exercise --mode exchange --variant 3 --role producer --queue-name ikbo-66-23_zuev --message 'Абонемент оформлен'
```

Для fanout ключ маршрутизации не влияет на выбор очередей; обменник рассылает сообщение во все привязанные очереди. Потребитель выводит текст и задержку 1–5 секунд и подтверждает обработку. После завершения единственного подписчика очередь автоматически удаляется. Для демонстрации нескольких потребителей запускайте их с **разными** именами очередей, пока они подключены к одному обменнику `gym_exercise_3`.

По условию имя очереди: `<группа>_<фамилия>`. Использовано `ikbo-66-23_zuev` из исходного отчёта. Если вариант оформляет другой участник, замените фамилию.

Режим `simple` в `rabbitmq/exercise.py` дополнительно показывает свойства `exclusive`, `durable` и `auto_delete`, но индивидуальным является вариант 3.

## Асинхронные события основного проекта — RabbitMQ

Для настоящего асинхронного обмена выбран **RabbitMQ**, а не Kafka. Учебная часть variant 3 и доменные сообщения используют один экземпляр RabbitMQ, но разные обменники:

| Объект | Название | Настройка |
|---|---|---|
| Практика №5 | `gym_exercise_3` | `fanout`; автоудаляемая очередь |
| Доменные события | `gym.domain.events` | `topic`, `durable=True` |
| Очереди рабочих сервисов | `gym.subscriptions.payment-confirmed`, `gym.payments.subscription-terminated`, `gym.reviews.training-finished`, `gym.tours.tour-confirmed` | `durable=True`, persistent delivery, manual ack |
| Ошибочные сообщения | `gym.failed.events` | dead-letter очередь после `nack(requeue=False)` |

События: `payment.confirmed` → SubscriptionService; `subscription.terminated` → PaymentService (модельный возврат денег); `training.finished` → ReviewService (локальная проекция оконченных занятий); `tour.confirmed` → событие для менеджера (только журнал; дата без указания менеджером **не выдумывается**).

Все JSON-поля, названия exchange и queue в `rabbitmq/event_catalog.md` — **проектные форматы для практик 5–8**, а не утверждённые ранее спецификации. Дополнительно `CreateSubscription` принимает необязательную пару `payment_id`/`expires_at`, чтобы при подтверждении платежа обработчик имел достаточно данных для автоматической активации. **Срок абонемента самостоятельно не задаётся:** его должен предоставить вызывающий клиент.

**Ограничения:** изменение данных и отправка события пока не атомарны (нет transactional outbox); если публикация не удалась после commit, HTTP-вызов может завершиться ошибкой при уже сохранённом изменении. Повторяющиеся сообщения обрабатываются идемпотентно для реализованных переходов. Пропущенное событие не восстанавливается автоматически. `gym.failed.events` требует ручного разбора. Реальный денежный перевод не выполняется.

## Практическая №6: REST API

REST нужен только для синхронных запросов, например проверки пользователя перед бронированием. На каждом сервисе от 2 до 5 конечных точек. OpenAPI 3 в `docs/openapi/` для каждого сервиса. Адреса Swagger UI:

- UserService: `http://localhost:8001/docs`
- TrainingService: `http://localhost:8002/docs`
- ReviewService: `http://localhost:8003/docs`
- SubscriptionService: `http://localhost:8004/docs`
- PaymentService: `http://localhost:8005/docs`
- TourService: `http://localhost:8006/docs`

## Практическая №7: запуск

```bash
docker compose up --build -d
docker compose ps
docker compose logs --follow payments_events subscriptions_events reviews_events tours_events
```

В составе Docker Compose: шесть FastAPI-сервисов, четыре отдельных процесса-потребителя RabbitMQ, PostgreSQL (шесть логических баз), Redis (кеш расписания) и RabbitMQ с веб-панелью `http://localhost:15672` (локально `gym` / `gym_dev_only`, только для разработки).

Пример последовательности бизнес-операций через HTTP и RabbitMQ:

1. Создать платёж через `POST http://localhost:8005/payments` (тестовый статус `initiated`) и получить `payment_id`.
2. Создать абонемент через `POST http://localhost:8004/subscriptions` с `user_id`, `payment_id` и `expires_at` (срок задаёт вызывающий клиент, т.к. в №1–4 тарифов нет).
3. Подтвердить платёж `POST /payments/{payment_id}:confirm`. PaymentService публикует `payment.confirmed`; `subscriptions_events` активирует соответствующий абонемент.
4. Расторгнуть абонемент через `PATCH /subscriptions/{id}` с `{"action":"terminate"}`. SubscriptionService публикует `subscription.terminated`; `payments_events` изменит статус тестового платежа на `refunded`.

Межсервисные события асинхронны: после HTTP-ответа обновление второго сервиса может появиться не сразу. Это проектная демонстрация, не финансовая система.

Docker в текущей среде подготовки недоступен. **Не утверждается**, что контейнеры или реальные очереди уже запускались.

## Практическая №8: тестирование

```bash
python -m venv .venv
# активируйте окружение
pip install -r requirements.txt
python -m pytest -q
```

Проверяются unit и component тесты с SQLite и моками HTTP/RabbitMQ. Проверена структура variant 3, вычисление задержек, публикация бизнес-событий из API, применение обработчиков событий и идемпотентность. Отдельный интеграционный тест с **реальным брокером RabbitMQ** не проводился — его требуется выполнить после старта Docker.

## Файлы

- `rabbitmq/exercise.py` — практическая №5, вариант 3.
- `rabbitmq/event_catalog.md` — события спортзала и контракты RabbitMQ.
- `common/events.py`, `rabbitmq/workers.py` — публикация и обработка доменных событий.
- `docs/openapi/*.json` — практика №6.
- `services/*`, `docker-compose.yml`, `init-databases.sql` — практика №7.
- `tests/*`, `docs/test_results.txt` — практика №8.
- `docs/source_constraints.md` — происхождение фактов и границы достоверности.
