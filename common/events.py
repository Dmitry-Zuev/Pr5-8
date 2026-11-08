"""RabbitMQ domain-event publisher for the educational prototype.

An absent AMQP_URL disables event dispatch only in isolated local/SQLite tests.
In Docker Compose AMQP_URL is always defined. The write + publish operation
is NOT transactional; production would require a transactional outbox.
"""
import json
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

DOMAIN_EXCHANGE = "gym.domain.events"
FAILED_EXCHANGE = "gym.domain.failed"
EVENT_ROUTES = {
    "payment.confirmed": "gym.subscriptions.payment-confirmed",
    "subscription.terminated": "gym.payments.subscription-terminated",
    "training.finished": "gym.reviews.training-finished",
    "tour.confirmed": "gym.tours.tour-confirmed",
}


def declare_topology(channel):
    """Create durable bindings before any publisher emits the first event."""
    channel.exchange_declare(exchange=DOMAIN_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=FAILED_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue="gym.failed.events", durable=True)
    channel.queue_bind(queue="gym.failed.events", exchange=FAILED_EXCHANGE)
    for event_type, queue in EVENT_ROUTES.items():
        channel.queue_declare(queue=queue, durable=True,
                              arguments={"x-dead-letter-exchange": FAILED_EXCHANGE})
        channel.queue_bind(queue=queue, exchange=DOMAIN_EXCHANGE, routing_key=event_type)



def make_envelope(event_type, data):
    return {"event_id": str(uuid4()), "type": event_type,
            "occurred_at": datetime.now(timezone.utc).isoformat(), "data": data}


def publish_event(event_type, data):
    """Publish one persistent JSON event. Return False only if AMQP is disabled."""
    amqp_url = os.getenv("AMQP_URL")
    if not amqp_url:
        logging.getLogger(__name__).info("AMQP_URL unset; local test: event %s not published", event_type)
        return False
    import pika
    envelope = make_envelope(event_type, data)
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=DOMAIN_EXCHANGE, routing_key=event_type,
            body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
    finally:
        connection.close()
    return True
