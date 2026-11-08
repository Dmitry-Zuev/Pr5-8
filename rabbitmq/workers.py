"""Run one RabbitMQ event consumer per service.

Usage: python -m rabbitmq.workers subscriptions|payments|reviews|tours
No messages are acknowledged before their DB effects have committed.
"""
import json
import logging
import os
import sys
from time import sleep

from sqlalchemy import select
from common.events import DOMAIN_EXCHANGE, declare_topology
from common.runtime import make_sessions

WORKERS = {
    "subscriptions": ("payment.confirmed", "gym.subscriptions.payment-confirmed"),
    "payments": ("subscription.terminated", "gym.payments.subscription-terminated"),
    "reviews": ("training.finished", "gym.reviews.training-finished"),
    "tours": ("tour.confirmed", "gym.tours.tour-confirmed"),
}


def apply_event(worker, envelope, sessions):
    """Apply a domain event; repeated events are safe for these state transitions."""
    event_type = envelope["type"]
    data = envelope["data"]
    expected = WORKERS[worker][0]
    if event_type != expected:
        raise ValueError(f"Unexpected event {event_type!r}, expected {expected!r}")
    with sessions() as db:
        if worker == "subscriptions":
            from services.subscriptions.main import Subscription
            payment_id = data["payment_id"]
            user_id = data["user_id"]
            matches = db.scalars(select(Subscription).where(
                Subscription.payment_id == payment_id,
                Subscription.user_id == user_id,
                Subscription.status == "selected",
            )).all()
            for sub in matches:
                # The expiry date is unknown in works 1–4: never invent one.
                if sub.expires_at is not None:
                    sub.status = "active"
            db.commit()
            return len([s for s in matches if s.status == "active"])
        if worker == "payments":
            from services.payments.main import Payment
            payment_id = data.get("payment_id")
            if not payment_id:
                return 0  # Terminated subscription had no confirmed payment.
            payment = db.get(Payment, payment_id)
            if payment is None:
                raise ValueError(f"Payment {payment_id} does not exist")
            if payment.user_id != data["user_id"]:
                raise ValueError("Payment/user mismatch")
            if payment.status == "confirmed":
                payment.status = "refunded"
                db.commit()
                return 1
            return 0  # Already refunded / another state; idempotent.
        if worker == "reviews":
            from services.reviews.main import CompletedTraining
            training_id = data["training_id"]
            if db.get(CompletedTraining, training_id) is None:
                db.add(CompletedTraining(training_id=training_id))
                db.commit()
                return 1
            return 0
        if worker == "tours":
            # Only a manager can pick the date. There is no confirmed manager
            # notification service in prior works; write a log, not a fake plan.
            logging.info("Экскурсия %s подтверждена; ожидает планирования менеджером", data["tour_id"])
            return 1
    raise ValueError("Unknown worker")


def run_worker(worker):
    if worker not in WORKERS:
        raise ValueError(f"Worker must be one of {list(WORKERS)}")
    amqp_url = os.getenv("AMQP_URL")
    if not amqp_url:
        raise RuntimeError("AMQP_URL must be set for a broker worker")
    import pika
    from services.subscriptions.main import Subscription
    from services.payments.main import Payment
    from services.reviews.main import CompletedTraining
    # Ensure consumer-owned tables are initialized even if API starts later.
    tables = {"subscriptions": [Subscription.__table__], "payments": [Payment.__table__],
              "reviews": [CompletedTraining.__table__], "tours": []}[worker]
    engine, sessions = make_sessions()
    try:
        if tables:
            from common.runtime import Base
            Base.metadata.create_all(engine, tables=tables)
        key, queue = WORKERS[worker]
        while True:
            try:
                connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
                try:
                    channel = connection.channel()
                    declare_topology(channel)
                    channel.basic_qos(prefetch_count=1)
                    logging.info("RabbitMQ %s listening: %s -> %s", worker, key, queue)
                    def consume(ch, method, props, body):
                        try:
                            envelope = json.loads(body.decode("utf-8"))
                            apply_event(worker, envelope, sessions)
                        except Exception:
                            logging.exception("Failed event %s", worker)
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        else:
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                    channel.basic_consume(queue=queue, on_message_callback=consume, auto_ack=False)
                    channel.start_consuming()
                finally:
                    if connection.is_open:
                        connection.close()
            except KeyboardInterrupt:
                return
            except Exception:
                logging.exception("RabbitMQ worker disconnected; reconnecting")
                sleep(3)
    finally:
        engine.dispose()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m rabbitmq.workers subscriptions|payments|reviews|tours")
    run_worker(sys.argv[1])
