import argparse
import csv
import json
import logging
import os
import redis
import requests
import signal
import threading
import time
import umsgpack

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.makedirs(os.path.dirname("results/"), exist_ok=True)

get_count = 0
post_count = 0
no_more_batches = False
counter_lock = threading.Lock()

stop_event = threading.Event()

def poll_batches(
    server_url,
    bench_id,
    kafka_bootstrap="kafka:9092",
    topic="raw-batch",
    interval=1,
    burst_size=1,
    verbose=False
):
    logger = logging.getLogger("poll_batches")
    global get_count, no_more_batches

    session = requests.Session()
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: v
    )

    logger.info("Polling batch thread started")
    while not stop_event.is_set():
        for i in range(burst_size):
            try:
                response = session.get(f"{server_url}/api/next_batch/{bench_id}")
                if response.status_code == 404:
                    logger.info("No more batches available.")
                    with counter_lock:
                        no_more_batches = True
                    return

                response.raise_for_status()
                response_content = response.content

                if verbose:
                    batch = umsgpack.unpackb(response_content)
                    print_id = batch["print_id"]
                    tile_id = batch["tile_id"]
                    batch_id = batch["batch_id"]
                    layer = batch["layer"]
                    logger.info(f"[BATCH {batch_id}] Received from server | print={print_id} | layer={layer} | tile={tile_id}")
                else:
                    logger.info(f"Received next batch from server.")

                producer.send(topic, response_content)
                logger.info(f"Sent batch to Kafka topic '{topic}'")

                with counter_lock:
                    get_count += 1

            except Exception as e:
                logger.error(f"Polling error: {e}")
                break

        time.sleep(interval)


def consume_results(
    kafka_bootstrap="kafka:9092",
    server_url=None,
    topic="results"
):
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

def consume_q1_redis(
    redis_host='redis',
    redis_port=6379,
    channel='saturated-pixels',
    csv_path='results/saturated-pixels.csv',
    verbose=False
):
    logger = logging.getLogger("consume_q1_redis")

    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    logger.info(f"[consume_q1_redis] Subscribed to Redis channel: {channel}")

    with open(csv_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['seq_id', 'print_id', 'tile_id', 'saturated'])

    buffer = {}  # seq_id → (print_id, tile_id, saturated)
    next_expected_id = 0

    def flush_buffer():
        nonlocal next_expected_id
        with open(csv_path, mode='a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            while next_expected_id in buffer:
                print_id, tile_id, saturated = buffer.pop(next_expected_id)
                writer.writerow([next_expected_id, print_id, tile_id, saturated])

                if verbose:
                    logger.info(f"[consume_q1_redis] Written seq_id={next_expected_id}")

                next_expected_id += 1

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = message['data']
                parts = data.split(',')

                if len(parts) != 4:
                    logger.warning(f"[consume_q1_redis] Unexpected CSV format: {data}")
                    continue

                seq_id, print_id, tile_id, saturated_count = parts
                seq_id = int(seq_id)
                tile_id = int(tile_id)
                saturated_count = int(saturated_count)

                if verbose:
                    logger.info(f"[consume_q1_redis] Received: seq_id={seq_id}, print_id={print_id}, tile_id={tile_id}, saturated={saturated_count}")

                if seq_id not in buffer:
                    buffer[seq_id] = (print_id, tile_id, saturated_count)

                flush_buffer()

            except Exception as e:
                logger.error(f"[consume_q1_redis] Error parsing message: {e}")

        if stop_event.is_set():
            logger.info("[consume_q1_redis] Stop event received. Exiting.")

            break

def consume_q2_redis(
    redis_host='redis',
    redis_port=6379,
    db=0,
    channel='saturated-rank',
    csv_path='results/saturated-rank.csv',
    verbose=False
):
    logger = logging.getLogger("consume_q2_redis")

    r = redis.Redis(host=redis_host, port=redis_port, db=db, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    logger.info(f"[consume_q2_redis] Subscribed to Redis channel: {channel}")

    with open(csv_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'seq_id', 'print_id', 'tile_id',
            'p_1', 'dp_1', 'p_2', 'dp_2', 'p_3', 'dp_3', 'p_4', 'dp_4', 'p_5', 'dp_5'
        ])

    buffer = {}
    next_expected_id = 32

    def flush_buffer():
        nonlocal next_expected_id
        with open(csv_path, mode='a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            while next_expected_id in buffer:
                writer.writerow(buffer.pop(next_expected_id))

                if verbose:
                    logger.info(f"[consume_q2_redis] Written seq_id={next_expected_id}")

                next_expected_id += 1

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = message['data']
                parts = data.split(',')

                if len(parts) != 13:
                    logger.warning(f"[consume_q2_redis] Unexpected CSV format (got {len(parts)} fields): {data}")
                    continue

                seq_id = int(parts[0])
                print_id = parts[1]
                tile_id = int(parts[2])
                p1, dp1 = parts[3], float(parts[4])
                p2, dp2 = parts[5], float(parts[6])
                p3, dp3 = parts[7], float(parts[8])
                p4, dp4 = parts[9], float(parts[10])
                p5, dp5 = parts[11], float(parts[12])

                row = [
                    seq_id, print_id, tile_id,
                    p1, dp1, p2, dp2, p3, dp3, p4, dp4, p5, dp5
                ]

                buffer[seq_id] = row

                if verbose:
                    logger.info(f"[consume_q2_redis] Parsed and buffered seq_id={seq_id}")

                flush_buffer()

            except Exception as e:
                logger.error(f"[consume_q2_redis] Error processing message from Redis: {e}")

        if stop_event.is_set():
            logger.info("[consume_q2_redis] Stop event received. Exiting.")

            break

def consume_q3_redis(
    redis_host='redis',
    redis_port=6379,
    db=0,
    channel='centroids',
    server_url=None,
    bench_id=None,
    csv_path='results/centroids.csv',
    verbose=False
):
    logger = logging.getLogger("consume_q3_redis")

    global post_count

    r = redis.Redis(host=redis_host, port=redis_port, db=db, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    logger.info(f"[consume_q3_redis] Subscribed to Redis channel: {channel}")

    session = requests.Session()

    with open(csv_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['seq_id', 'print_id', 'tile_id', 'saturated', 'centroids'])

    buffer = {}
    next_expected_id = 32

    def flush_buffer():
        global post_count

        nonlocal next_expected_id
        with open(csv_path, mode='a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            while next_expected_id in buffer:
                row = buffer.pop(next_expected_id)

                writer.writerow([next_expected_id] + row)

                if verbose:
                    logger.info(f"[consume_q3_redis] Written seq_id={next_expected_id}")

                with counter_lock:
                    post_count += 1

                next_expected_id += 1

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = message['data']
                parts = data.split(',',4)

                if len(parts) != 5:
                    logger.warning(f"[consume_q3_redis] Unexpected CSV format: {data}")
                    continue

                seq_id = int(parts[0])
                print_id = parts[1]
                tile_id = int(parts[2])
                saturated = int(parts[3])
                centroids = parts[4]

                # Fai la POST subito (non ordinata)
                if server_url:
                    try:
                        centroids_json = json.loads(centroids)
                        payload = {
                            "print_id": print_id,
                            "tile_id": int(tile_id),
                            "saturated": int(saturated),
                            "centroids": centroids_json
                        }
                        resp = session.post(
                            f"{server_url}/api/result/0/{bench_id}/{seq_id}",
                            json=payload
                        )
                        resp.raise_for_status()
                        logger.info(f"[consume_q3_redis] Posted result seq_id={seq_id} — Response: {resp.text}")
                    except Exception as e:
                        logger.error(f"[consume_q3_redis] Failed to post seq_id={seq_id}: {e}")

                row = [print_id, tile_id, saturated, centroids]
                buffer[seq_id] = row

                if verbose:
                    logger.info(f"[consume_q3_redis] Buffered seq_id={seq_id}")

                flush_buffer()

                with counter_lock:
                    post_count += 1

            except Exception as e:
                logger.error(f"[consume_q3_redis] Error processing message from Redis: {e}")

        if stop_event.is_set():
            logger.info("[consume_q3_redis] Stop event received. Exiting.")

            break

def create_and_start_benchmark(server_url, limit=None):
    session = requests.Session()
    logger.info("Creating benchmark")

    if limit:
        response = session.post(
            f"{server_url}/api/create",
            json={"apitoken": "ralisin", "name": "middleware-run", "test": True, "max_batches": limit}
        )
    else:
        response = session.post(
            f"{server_url}/api/create",
            json={"apitoken": "ralisin", "name": "middleware-run", "test": True}
        )
    response.raise_for_status()
    bench_id = response.json()
    logger.info(f"Created benchmark with ID: {bench_id}")

    logger.info(f"Starting benchmark {bench_id}")
    start_resp = session.post(f"{server_url}/api/start/{bench_id}")
    start_resp.raise_for_status()
    return bench_id

def watch_and_end(server_url, bench_id, limit = None, check_interval=3):
    global get_count, post_count, no_more_batches
    session = requests.Session()
    logger.info("Starting watcher thread to send/end")

    while not stop_event.is_set():
        with counter_lock:
            if limit:
                if get_count == limit and post_count == limit - (16 * 2):
                    break
            if no_more_batches and get_count == 225 * 16 and post_count == (225 - 2) * 16:
                break
        time.sleep(check_interval)

    try:
        logger.info("Sending /end request")
        resp = session.post(f"{server_url}/api/end/{bench_id}")
        resp.raise_for_status()

        logger.info(f"Completed bench {bench_id}")
        print(f"Result: {resp.text}")
    except Exception as e:
        logger.error(f"Error sending /end: {e}")

def handle_shutdown(signum, frame):
    logger.info("Shutdown signal received. Stopping threads...")
    stop_event.set()

def main():
    env_server_url = os.getenv("CHALLENGER_URL")
    env_kafka_url = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    env_limit =  int(os.getenv("LIMIT")) if os.getenv("LIMIT") is not None and os.getenv("LIMIT").isdigit() else None
    env_burst_size =  int(os.getenv("BURST_SIZE")) if os.getenv("BURST_SIZE") is not None and os.getenv("BURST_SIZE").isdigit() else 1
    env_interval =  int(os.getenv("INTERVAL")) if os.getenv("INTERVAL") is not None and os.getenv("INTERVAL").isdigit() else 2
    env_verbose = os.getenv("VERBOSE", "false").lower() in ("1", "true", "yes")

    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", default=env_server_url, help="Local Challenger URL")
    parser.add_argument("--kafka", default=env_kafka_url, help="Kafka bootstrap server")
    parser.add_argument("--limit", type=int, default=env_limit, help="Max number of batches (optional)")
    parser.add_argument("--burst-size", type=int, default=env_burst_size, help="Max number of batches sent before an interval time (optional)")
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
    batch_thread = threading.Thread(target=poll_batches, args=(args.server_url, bench_id, args.kafka), kwargs={"burst_size": 16, "interval": args.interval, "verbose": args.verbose})
    # kafka_thread = threading.Thread(target=consume_results, args=(args.kafka, args.server_url))
    q1_thread = threading.Thread(target=consume_q1_redis, kwargs={"verbose": args.verbose})
    q2_thread = threading.Thread(target=consume_q2_redis, kwargs={"verbose": args.verbose})
    q3_thread = threading.Thread(target=consume_q3_redis, kwargs={"bench_id": bench_id, "server_url": args.server_url, "verbose": args.verbose})
    watcher_thread = threading.Thread(target=watch_and_end, args=(args.server_url, bench_id, args.limit))

    batch_thread.start()
    # kafka_thread.start()
    q1_thread.start()
    q2_thread.start()
    q3_thread.start()
    watcher_thread.start()

    # Step 3: Wait threads
    batch_thread.join()
    # kafka_thread.join()
    q1_thread.join()
    q2_thread.join()
    q3_thread.join()
    watcher_thread.join()

    logger.info("Middleware exited cleanly.")

if __name__ == "__main__":
    main()
