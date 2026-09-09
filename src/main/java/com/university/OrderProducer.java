package com.university;

import com.university.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class OrderProducer {

    private static final String TOPIC = "orders";
    private static final String[] PRODUCTS = {"Item1", "Item2", "Item3", "Item4", "Item5"};

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        // Producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Random random = new Random();

        // Send 20 sample order messages
        for (int i = 1; i <= 20; i++) {
            String orderId = String.valueOf(1000 + i);
            String product = PRODUCTS[random.nextInt(PRODUCTS.length)];
            float price = 10 + random.nextFloat() * 490; // price between 10 and 500

            Order order = Order.newBuilder()
                    .setOrderId(orderId)
                    .setProduct(product)
                    .setPrice(price)
                    .build();

            ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, orderId, order);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent order: %s | product=%s, price=%.2f | partition=%d, offset=%d%n",
                    orderId, product, price, metadata.partition(), metadata.offset());

            Thread.sleep(500); // small delay between messages
        }

        producer.flush();
        producer.close();
        System.out.println("Finished sending all orders.");
    }
}