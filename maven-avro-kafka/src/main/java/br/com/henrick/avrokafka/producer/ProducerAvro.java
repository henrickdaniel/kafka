package br.com.henrick.avrokafka.producer;

import br.com.henrick.avro.Sale;
import br.com.henrick.avrokafka.config.ConfigKafka;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class ProducerAvro {

    private static final Properties config;

    static {
        try {
            config = ConfigKafka.readConfig();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void produce(String topic, String key, String value) throws IOException {
        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        Sale sale = new Sale();
        sale.setSaleId("123456");
        sale.setStatus("registered");
        sale.setCostumerId("91185670106");

        // creates a new producer instance and sends a sample message to the topic

        Producer<String, Sale> producer = new KafkaProducer<>(config);
        org.apache.kafka.clients.producer.ProducerRecord<String, Sale> producerRecord = new ProducerRecord<>("mytopic", null, sale);

        producer.send(producerRecord);

        System.out.println(
                String.format(
                        "Produced message to topic = %s key = %s value = %s", topic, key, value
                )
        );
        producer.close();
    }

}
