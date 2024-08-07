package br.com.henrick.avrokafka.producer;

import br.com.henrick.avrokafka.config.ConfigKafka;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class Producer {

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
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creates a new producer instance and sends a sample message to the topic
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(config);
        producer.metrics();
        producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    log.error("Error sending message to topic: {}, erro.message: {}, e.cause: {}", topic, e.getMessage(), e.getCause());
                }else{
                    log.info("Posted to topic: {} with message: {}", recordMetadata.topic(), value);
                }
            }
        });

        System.out.println(
                String.format(
                        "Produced message to topic = %s key = %s value = %s", topic, key, value
                )
        );
        producer.close();
    }

}
