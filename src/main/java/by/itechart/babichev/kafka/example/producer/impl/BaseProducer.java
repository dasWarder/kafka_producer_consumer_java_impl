package by.itechart.babichev.kafka.example.producer.impl;

import by.itechart.babichev.kafka.example.producer.AbstractProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseProducer extends AbstractProducer {

    private static final Logger logger = LoggerFactory.getLogger(BaseProducer.class);

    public BaseProducer() {
        super();
    }

    public BaseProducer(String topic) {
        super(topic);
    }

    @Override
    public void sendMessage(String message) {

        logger.info("Send a producer message");

        ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, message);
        PRODUCER.send(record);
    }
}
