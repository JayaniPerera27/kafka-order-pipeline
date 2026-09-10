# Kafka Order Pipeline

A Kafka-based system that produces and consumes order messages using **Avro serialization**, with **real-time aggregation**, **retry logic**, and a **Dead Letter Queue (DLQ)** for permanently failed messages.

Built for the Big Data Analytics take-home assignment.


## Features

- **Avro Serialization** - Order messages are serialized/deserialized using Apache Avro with Confluent Schema Registry.
- **Real-time Aggregation** - Consumer maintains a running average of order prices as messages arrive.
- **Retry Logic** - Failed message processing is retried up to 3 times with backoff before being considered a permanent failure.
- **Dead Letter Queue (DLQ)** - Messages that fail after all retries are routed to a separate `orders-dlq` topic instead of being dropped.
- **Dockerized Infrastructure** - Kafka and Schema Registry run via Docker Compose, no manual installation required.
---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Java 21 |
| Build Tool | Maven |
| Messaging | Apache Kafka |
| Serialization | Apache Avro |
| Schema Management | Confluent Schema Registry |
| Infrastructure | Docker & Docker Compose |
 
---

## Order Message Schema (`order.avsc`)

| Field | Type | Description |
|---|---|---|
| `orderId` | string | Unique identifier for the order |
| `product` | string | Name of the purchased item |
| `price` | float | Price of the product |

`````json
{
  "namespace": "com.university.avro",
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
`````
 
---

## Project Structure

`````
kafka-order-pipeline/
├── docker-compose.yml          # Kafka + Schema Registry setup
├── pom.xml                     # Maven build configuration
├── schema/
│   └── order.avsc              # Avro schema definition
├── src/main/java/com/university/
│   ├── avro/
│   │   └── Order.java          # Auto-generated Avro POJO
│   ├── OrderProducer.java      # Kafka producer
│   └── OrderConsumer.java      # Kafka consumer (aggregation + retry + DLQ)
└── README.md
`````
 
---

## Architecture / Flow

`````
OrderProducer → (Avro serialize) → "orders" topic
                                        │
                                        ▼
                                 OrderConsumer
                                        │
                         ┌──────────────┴──────────────┐
                         ▼                              ▼
                  Success → update                Fail (retry x3)
                  running average                        │
                                                           ▼
                                                  "orders-dlq" topic
`````
 
---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Java 21 (JDK)
- IntelliJ IDEA (or any IDE with Maven support)
---

## Setup & Running Instructions

### 1. Clone the repository

`````bash
git clone https://github.com/JayaniPerera27/kafka-order-pipeline.git
cd kafka-order-pipeline
`````

### 2. Start Kafka and Schema Registry

`````bash
docker compose up -d
`````

Verify both containers are running:

`````bash
docker ps
`````

### 3. Create Kafka topics

`````bash
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
`````

Confirm topics were created:

`````bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
`````

### 4. Build the project (generates Avro POJO from schema)

In IntelliJ, use the Maven panel → `Lifecycle` → `compile`
(or from a terminal with Maven installed: `mvn clean compile`)

### 5. Run the Consumer

Run `OrderConsumer.java` first - it will start listening on the `orders` topic.

### 6. Run the Producer

Run `OrderProducer.java` - it sends 20 sample order messages with random products and prices to the `orders` topic.

### 7. Observe the Consumer output

The consumer will:
- Process each message and print the running average
- Randomly simulate temporary failures (~20% of messages) to demonstrate retry logic
- Send permanently failed messages (after 3 failed retries) to the `orders-dlq` topic
---

## Verifying the Dead Letter Queue

To check messages that landed in the DLQ:

`````bash
docker exec -it kafka kafka-console-consumer --topic orders-dlq --bootstrap-server localhost:9092 --from-beginning --property print.key=true
`````
 
---

## Resetting for a Clean Demo Run

To reset the consumer offset so the consumer only picks up new messages (without deleting topics):

`````bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group order-consumer-group --reset-offsets --to-latest --topic orders --execute
`````

To fully reset all data (topics, messages, offsets):

`````bash
docker compose down -v
docker compose up -d
````//then re-create the topics as in Step 3
 
---
 
## Stopping the Environment
 
```bash
docker compose down
```
 
---
 
## Demo Video
 
A live demonstration of this system (producer + consumer running, real-time aggregation, retry logic, and DLQ handling) is available here:
 
`[Add video link here]`
 
---
 
## Author
 
M.A. Jayani Chamodi Perera
Computer Engineering, University of Ruhuna
 