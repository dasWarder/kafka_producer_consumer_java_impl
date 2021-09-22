package by.itechart.babichev.kafka.example.producer.impl;

import by.itechart.babichev.kafka.example.producer.AbstractProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BaseProducer extends AbstractProducer {

    private final KafkaProducer<String, String> producer = super.getProducer();

    @Override
    public void sendMessage(String message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, message);
        producer.send(record);
    }

    @Override
    public void closeProducer() {
        producer.close();
    }
}
