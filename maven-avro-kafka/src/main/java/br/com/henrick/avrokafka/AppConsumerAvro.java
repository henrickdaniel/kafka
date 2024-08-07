package br.com.henrick.avrokafka;

import br.com.henrick.avro.Sale;
import br.com.henrick.avrokafka.config.ConfigKafka;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AppConsumerAvro {

    public static void main(String[] args) throws IOException {
        consume("mytopic");
    }

    public static void consume (String topic) throws IOException {
        Properties config = ConfigKafka.readConfig();
        // sets the group ID, offset and message deserializers
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // creates a new consumer instance and subscribes to messages from the topic
        KafkaConsumer<String, Sale> consumer = new KafkaConsumer<>(config);
        consumer.listTopics();
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            // polls the consumer for new messages and prints them
            ConsumerRecords<String, Sale> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Sale> record : records) {
                System.out.println(String.format("Consumed message from topic %s key = %s value = %s", topic, record.key(), record.value()));
            }
        }
    }

}
