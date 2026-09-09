package com.university;

import com.university.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    private static final String TOPIC = "orders";
    private static final String DLQ_TOPIC = "orders-dlq";
    private static final int MAX_RETRIES = 3;

    // Running average tracking
    private static long messageCount = 0;
    private static double totalPrice = 0.0;

    public static void main(String[] args) {

        // Consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put("schema.registry.url", "http://localhost:8081");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("specific.avro.reader", "true");

        // Producer for DLQ (re-use Avro serializer to push failed messages)
        Properties dlqProducerProps = new Properties();
        dlqProducerProps.put("bootstrap.servers", "localhost:9092");
        dlqProducerProps.put("key.serializer", StringSerializer.class.getName());
        dlqProducerProps.put("value.serializer", KafkaAvroSerializer.class.getName());
        dlqProducerProps.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, Order> dlqProducer = new KafkaProducer<>(dlqProducerProps);

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Consumer started. Listening to topic: " + TOPIC);

        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Order> record : records) {
                    processWithRetry(record, dlqProducer);
                }
            }
        } finally {
            consumer.close();
            dlqProducer.close();
        }
    }

    // Processes a message with retry logic, sends to DLQ if all retries fail
    private static void processWithRetry(ConsumerRecord<String, Order> record, KafkaProducer<String, Order> dlqProducer) {
        int attempt = 0;
        boolean success = false;

        while (attempt < MAX_RETRIES && !success) {
            attempt++;
            try {
                processOrder(record.value());
                success = true;
            } catch (Exception e) {
                System.out.printf("Attempt %d failed for order %s: %s%n",
                        attempt, record.key(), e.getMessage());

                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(1000L * attempt); // simple backoff
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        // All retries exhausted - send to DLQ
        if (!success) {
            System.out.printf("Order %s permanently failed. Sending to DLQ.%n", record.key());
            ProducerRecord<String, Order> dlqRecord =
                    new ProducerRecord<>(DLQ_TOPIC, record.key(), record.value());
            dlqProducer.send(dlqRecord);
            dlqProducer.flush();
        }
    }
    private static void processOrder(Order order) {

        // --- Simulated temporary failure (demo purpose) ---
        // Uncomment below to test retry logic on a random subset of messages:
         if (Math.random() < 0.2) throw new RuntimeException("Simulated temporary failure");

        messageCount++;
        totalPrice += order.getPrice();
        double runningAverage = totalPrice / messageCount;

        System.out.printf("Processed order %s | product=%s, price=%.2f | Running Average: %.2f (count=%d)%n",
                order.getOrderId(), order.getProduct(), order.getPrice(), runningAverage, messageCount);
    }
}