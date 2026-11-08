"""Practical 5: RabbitMQ exercise. Variant 3 (ticket ends in 3): auto-delete/fanout/random 1–5 seconds.
No credentials for university training server are included in this project.
"""
import argparse
import os
import random
import time

VARIANTS = {
    1: {"queue": "auto_delete", "exchange": "topic", "delay": "dots"},
    2: {"queue": "durable", "exchange": "direct", "delay": "length"},
    3: {"queue": "auto_delete", "exchange": "fanout", "delay": "random"},
    4: {"queue": "durable", "exchange": "direct", "delay": "numeric"},
}

def get_delay(variant, message):
    if variant == 1:
        return message.count(".")
    if variant == 2:
        return len(message)
    if variant == 3:
        return random.randint(1, 5)
    if variant == 4:
        try: return max(0, int(message))
        except ValueError as exc: raise ValueError("Вариант 4 требует число в теле сообщения") from exc
    raise ValueError("Номер варианта 1..4")

def get_connection():
    import pika
    url = os.getenv("AMQP_URL")
    if not url: raise RuntimeError("Требуется AMQP_URL (пример: amqp://gym:...@localhost:5672/)")
    return pika.BlockingConnection(pika.URLParameters(url))

def simple_queue(name, kind, message):
    """Same-channel sender/receiver handles exclusive correctly."""
    import pika
    conn = get_connection()
    try:
        ch = conn.channel()
        opts = {"exclusive": kind == "exclusive", "auto_delete": kind == "auto_delete", "durable": kind == "durable"}
        ch.queue_declare(queue=name, **opts)
        ch.basic_publish(exchange="", routing_key=name, body=message,
                         properties=pika.BasicProperties(delivery_mode=2 if kind == "durable" else 1))
        method, properties, body = ch.basic_get(queue=name, auto_ack=False)
        if not method: raise RuntimeError("Сообщение не получено")
        print(f"RECEIVED {body.decode('utf-8')}")
        ch.basic_ack(method.delivery_tag)
        if kind == "auto_delete":
            ch.basic_consume(queue=name, on_message_callback=lambda *_: None)
            ch.basic_cancel(ch.consumer_tags[0])
    finally:
        conn.close()

def exchange_demo(name, variant, role, message):
    """For transient auto_delete queues start consumer before producer."""
    import pika
    conf = VARIANTS[variant]
    queue_name = name
    exchange_name = f"gym_exercise_{variant}"
    durable = conf["queue"] == "durable"
    conn = get_connection()
    try:
        ch = conn.channel()
        ch.exchange_declare(exchange=exchange_name, exchange_type=conf["exchange"], durable=durable)
        key = "gym.payment.confirmed" if conf["exchange"] == "topic" else "gym.payment"
        # For variant 3 the auto-deleted queue is owned and declared ONLY by
        # the consumer. Producer only publishes to fanout; start consumer first.
        if role == "consumer":
            ch.queue_declare(queue=queue_name, durable=durable, auto_delete=not durable)
            ch.queue_bind(queue=queue_name, exchange=exchange_name,
                          routing_key=("gym.#" if conf["exchange"] == "topic" else key))
        if role == "producer":
            ch.basic_publish(exchange=exchange_name, routing_key=key, body=message,
                             properties=pika.BasicProperties(delivery_mode=2 if durable else 1))
            print(f"SENT: {message}; exchange={conf['exchange']}; queue={queue_name}")
        else:
            print("Waiting for one message...")
            for method, properties, body in ch.consume(queue=queue_name, inactivity_timeout=20, auto_ack=False):
                if method is None: raise TimeoutError("За 20 секунд сообщение не получено")
                msg = body.decode("utf-8")
                delay = get_delay(variant, msg)
                print(f"RECEIVED {msg!r}; sleep={delay}s")
                time.sleep(delay)
                ch.basic_ack(method.delivery_tag)
                ch.cancel(); break
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-name", required=True, help="Формат: <группа>_<фамилия>")
    parser.add_argument("--mode", choices=["simple", "exchange"], required=True)
    parser.add_argument("--queue-kind", choices=["exclusive", "durable", "auto_delete"])
    parser.add_argument("--variant", type=int, choices=range(1, 5), default=3)
    parser.add_argument("--role", choices=["producer", "consumer"])
    parser.add_argument("--message", default="..")
    args = parser.parse_args()
    if args.mode == "simple":
        if not args.queue_kind: parser.error("Для simple нужен --queue-kind")
        simple_queue(args.queue_name, args.queue_kind, args.message)
    else:
        if not args.variant or not args.role: parser.error("Для exchange нужны --variant и --role")
        exchange_demo(args.queue_name, args.variant, args.role, args.message)
if __name__ == "__main__":
    main()
