# Real-time monitoring of defects in Laser Powder Bed Fusion

### Architecture
```plaintext
      ┌────────────────────┐
      │   Local Challenger │   <-- HTTP REST API
      │ (Evaluation Server)│
      └──────────┬─────────┘
                 ↑  POST /create
                 ↑  POST /start/{bench-id}
                 ↑  GET  /next_batch/{bench-id}
                 ↑  POST /result/{q}/{bench-id}/{batch-id}
                 ↑  POST /end/{bench-id}
                 ↑  ... other APIs (/get_result, /plot, /history)
                 │
┌────────────────┴───────────────────┐
│         Python Middleware          │  <-- Kafka Producer of topic "raw-batch"
│          Result Consumer           │  <-- Kafka Consumer of topic "results"
│                                    │      + HTTP POST /result → Local Challenger
│    Produces / Consumes to Kafka    │ 
└────────────────┬───────────────────┘
                 ↑ consumes from Kafka topic "results"
                 ↓ produces to Kafka topic "raw-batch"
      ┌──────────┴───────────┐
      │     Kafka Broker     │  <-- central message bus
      │     - "raw-batch"    │
      │     - "results"      │
      └──────────┬───────────┘
                 ↑ produces to Kafka topic "results"
                 ↓ consumes from Kafka topic "raw-batch"
        ┌────────┴────────┐
        │    Flink Job    │  <-- Kafka Consumer of "raw-batch"
        │                 │      Kafka Producer of "results"
        └─────────────────┘
```

| Component               | Role                                                                                                                               |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| **Local Challenger**    | Provides data via HTTP API and receives benchmark results via HTTP POST `/result/...`                                              |
| **Python Orchestrator** | Calls Local Challenger APIs, produces raw batches to Kafka topic `raw-batch`, consumes results from `results`, and posts them back |
| **Kafka**               | Central asynchronous and scalable message broker with topics `raw-batch` and `results`                                             |
| **Flink Job**           | Consumes raw batches from Kafka topic `raw-batch`, processes them, and produces results to Kafka topic `results`                   |

## Util commands

### Read kafka topic

Raw-batch
```shell
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw-batch --from-beginning
```

Results
```shell
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic results --from-beginning
```

Saturated pixels
```shell
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic saturated-pixels --from-beginning
```

Saturated rank
```shell
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic saturated-rank --from-beginning
```

### Start flink task

```shell
  flink run -pyFiles /opt/Q1.py,/opt/Q2.py,/opt/Q3.py -py /opt/flink_job.py
```

```shell
  flink run -p2 -pyFiles /opt/Q1.py,/opt/Q2.py,/opt/Q3.py -py /opt/flink_job.py
```

```shell
  flink run -p4 -pyFiles /opt/Q1.py,/opt/Q2.py,/opt/Q3.py -py /opt/flink_job.py
```

> ### Docker commands short-cut to read Kafka topic
> Raw-batch  
> ```bash
> docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw-batch --from-beginning
> ```
> Results
> ```shell
> docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic results --from-beginning
> ```
> 
> Saturated pixels
> ```shell
> docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic saturated-pixels --from-beginning
> ```
> 
> Saturated rank
> ```shell
> docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic saturated-rank --from-beginning
> ```