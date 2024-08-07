package br.com.henrick.avrokafka;

import br.com.henrick.avrokafka.producer.ProducerAvro;

import java.io.IOException;

public class AppProduceAvro {

    public static void main(String[] args) throws IOException {
        ProducerAvro.produce("mytopic", null, "123 testing");
    }

}
