import argparse
import os
import signal

from kafka import KafkaConsumer, KafkaProducer
import logging
import requests
import umsgpack
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

get_count = 0
post_count = 0
no_more_batches = False
counter_lock = threading.Lock()

stop_event = threading.Event()

def poll_batches(server_url, bench_id, kafka_bootstrap="kafka:9092", topic="raw-batch", interval=2, verbose=False):
    logger = logging.getLogger("poll_batches")

    global get_count, no_more_batches

    session = requests.Session()

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: v
    )

    logger.info("Polling batch thread started")
    while not stop_event.is_set():
        try:
            response = session.get(f"{server_url}/api/next_batch/{bench_id}")
            if response.status_code == 404:
                logger.info("No more batches available.")
                with counter_lock:
                    no_more_batches = True
                break

            response.raise_for_status()

            response_content = response.content

            if verbose:
                batch = umsgpack.unpackb(response_content)

                print_id = batch["print_id"]
                tile_id = batch["tile_id"]
                batch_id = batch["batch_id"]
                layer = batch["layer"]

                print(" B")

                logger.info(f"[BATCH {batch_id}] Received from server | print={print_id} | layer={layer} | tile={tile_id}")
            else:
                logger.info(f"Received next batch from server.")

            # Here we encode response_content as a string to simplify in a Flink job retrieving this message
            producer.send(topic, response_content)
            logger.info(f"Sent batch to Kafka topic '{topic}'")

            # The solution here describes a possible strategy for middleware messages but is not practical.
            # To keep middleware light way, it was preferred to send response.content directly,
            # without any deserialization and serialization in some other format
            #
            # batch = umsgpack.unpackb(response.content)
            #
            # print_id = batch["print_id"]
            # tile_id = batch["tile_id"]
            # batch_id = batch["batch_id"]
            # layer = batch["layer"]
            #
            # logger.info(f"Received batch - printId: {print_id}, tileId: {tile_id}, batchId: {batch_id}, layer: {layer}")
            # producer.send(topic, batch)
            # logger.info(f"Sent batch to Kafka topic '{topic}' - printId: {print_id}, tileId: {tile_id}, batchId: {batch_id}, layer: {layer}")

            with counter_lock:
                get_count += 1

        except Exception as e:
            logger.error(f"Polling error: {e}")
        time.sleep(interval)


def consume_results(kafka_bootstrap="kafka:9092", server_url=None, topic="results"):
    logger = logging.getLogger("consume_results")

    global post_count

    session = requests.Session()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="middleware-consumer",
        value_deserializer=lambda m: umsgpack.unpackb(m)
    )

    for message in consumer:
        if stop_event.is_set():
            break
        try:
            data = message.value
            logger.info(f"Consumed result: {data}")
            resp = session.post(
                f"{server_url}/api/result/{data['q']}/{data['bench_id']}/{data['batch_id']}",
                json=data["result"]
            )
            resp.raise_for_status()
            logger.info(f"Posted result for batch {data['batch_id']}")

            with counter_lock:
                post_count += 1

        except Exception as e:
            logger.error(f"Result processing error: {e}")

def consume_q1(kafka_bootstrap="kafka:9092", server_url=None, topic="saturated-pixels"):
    logger = logging.getLogger("consume_q1")

    global post_count

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="middleware-consumer",
        value_deserializer=lambda m: m
    )

    for message in consumer:
        if stop_event.is_set():
            break
        try:
            data = message
            logger.info(f"Consumed result: {data}")

        except Exception as e:
            logger.error(f"Result processing error: {e}")

def consume_q2(kafka_bootstrap="kafka:9092", server_url=None, topic="saturated-rank"):
    logger = logging.getLogger("consume_q2")

    global post_count

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="middleware-consumer",
        value_deserializer=lambda m: m
    )

    for message in consumer:
        if stop_event.is_set():
            break
        try:
            data = message
            logger.info(f"Consumed result: {data}")

        except Exception as e:
            logger.error(f"Result processing error: {e}")

def create_and_start_benchmark(server_url, limit=None):
    session = requests.Session()
    logger.info("Creating benchmark")
    response = session.post(
        f"{server_url}/api/create",
        json={"apitoken": "polimi-deib", "name": "middleware-run", "test": True, "max_batches": limit}
    )
    response.raise_for_status()
    bench_id = response.json()
    logger.info(f"Created benchmark with ID: {bench_id}")

    logger.info(f"Starting benchmark {bench_id}")
    start_resp = session.post(f"{server_url}/api/start/{bench_id}")
    start_resp.raise_for_status()
    return bench_id

def watch_and_end(server_url, bench_id, check_interval=1):
    global get_count, post_count, no_more_batches
    session = requests.Session()
    logger.info("Starting watcher thread to send /end")

    while not stop_event.is_set():
        with counter_lock:
            if no_more_batches and get_count == post_count:
                break
        time.sleep(check_interval)

    try:
        logger.info("Sending /end request")
        resp = session.post(f"{server_url}/api/end/{bench_id}")
        resp.raise_for_status()
        logger.info("Benchmark ended successfully.")
    except Exception as e:
        logger.error(f"Error sending /end: {e}")

def handle_shutdown(signum, frame):
    logger.info("Shutdown signal received. Stopping threads...")
    stop_event.set()

def main():
    env_server_url = os.getenv("CHALLENGER_URL")
    env_kafka_url = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    env_limit =  int(os.getenv("LIMIT")) if os.getenv("LIMIT") is not None and os.getenv("LIMIT").isdigit() else None
    env_interval =  int(os.getenv("INTERVAL")) if os.getenv("INTERVAL") is not None and os.getenv("INTERVAL").isdigit() else 2
    env_verbose = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", default=env_server_url, help="Local Challenger URL")
    parser.add_argument("--kafka", default=env_kafka_url, help="Kafka bootstrap server")
    parser.add_argument("--limit", type=int, default=env_limit, help="Max number of batches (optional)")
    parser.add_argument("--interval", type=int, default=env_interval, help="Interval between checks (optional)")
    parser.add_argument("--verbose", action="store_true", default=env_verbose, help="Enable verbose (debug) logging")
    args = parser.parse_args()

    if not args.server_url:
        parser.error("Missing --server-url and no CHALLENGER_URL environment variable provided.")

    # Trap SIGINT and SIGTERM for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Step 1: Create and start a benchmark
    bench_id = create_and_start_benchmark(args.server_url, limit=args.limit)

    # Step 2: Start threads
    batch_thread = threading.Thread(target=poll_batches, args=(args.server_url, bench_id, args.kafka), kwargs={"interval": args.interval, "verbose": args.verbose})
    kafka_thread = threading.Thread(target=consume_results, args=(args.kafka, args.server_url))
    q1_thread = threading.Thread(target=consume_q1, args=(args.kafka, args.server_url))
    q2_thread = threading.Thread(target=consume_q2, args=(args.kafka, args.server_url))
    watcher_thread = threading.Thread(target=watch_and_end, args=(args.server_url, bench_id))

    batch_thread.start()
    kafka_thread.start()
    q1_thread.start()
    q2_thread.start()
    watcher_thread.start()

    # Step 3: Wait threads
    batch_thread.join()
    kafka_thread.join()
    q1_thread.join()
    q2_thread.join()
    watcher_thread.join()

    logger.info("Middleware exited cleanly.")

if __name__ == "__main__":
    main()
