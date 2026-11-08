# Проверка прототипа на компьютере исполнителя

1. В каталоге проекта выполнить `docker compose up --build -d`, затем `docker compose ps`; сохранить действительный скриншот запущенных сервисов.
2. Открыть RabbitMQ Management `http://localhost:15672` под `gym` / `gym_dev_only`. Проверить `gym.domain.events` и `gym_exercise_3` (второй появляется после запуска упражнения).
3. Для варианта **3** сначала запустить `python -m rabbitmq.exercise --mode exchange --role consumer --variant 3 --queue-name ikbo-66-23_zuev`, затем в другом терминале аналогичный `producer` с `--message 'Тест'`. Сохранить реальные логи `sleep=1..5s`. Убедиться, что очередь `ikbo-66-23_zuev` удаляется при отключении подписчика.
4. Проверить таблицу Exchanges: учебный `gym_exercise_3` — `fanout`, доменный `gym.domain.events` — `topic`.
5. Проверить Queues: `gym.subscriptions.payment-confirmed`, `gym.payments.subscription-terminated`, `gym.reviews.training-finished`, `gym.tours.tour-confirmed` (устойчивые очереди), `gym.failed.events`.
6. Отправить запросы из README для создания платежа, создания связанного абонемента и подтверждения платежа. После обработки `payment.confirmed` через RabbitMQ проверить статус абонемента через GET.
7. Расторгнуть активный абонемент. Проверить в PaymentService, что `subscription.terminated` привёл к `refunded`.
8. Завершить тренировку, проверить, что появился `training.finished`, затем создать отзыв.
9. Выполнить `python -m pytest -q` и сохранить собственный вывод.
10. Реальный запуск Docker и AMQP не воспроизводился при подготовке архива: не выдавать пример вывода команды за скриншот фактического запуска.
