package by.itechart.babichev.kafka.example.producer.impl;

import by.itechart.babichev.kafka.example.producer.AbstractProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class BaseProducerWithKeys extends AbstractProducer {

    private static final AtomicLong ID = new AtomicLong(1);

    private static final Logger logger = LoggerFactory.getLogger(BaseProducerWithKeys.class);

    public BaseProducerWithKeys() {
        super();
    }

    public BaseProducerWithKeys(String topic) {
        super(topic);
    }

    @Override
    public void sendMessage(String message) {

        String id = String.valueOf(ID.getAndIncrement());

        logger.info("Key = {}", id);

        ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, id, message);
        PRODUCER.send(record, (rm, e) -> {

            if (e == null) {
                logger.info("Record metadata received. Topic = {}, partition = {}, offset = {}, timestamp = {}",
                        rm.topic(), rm.partition(), rm.offset(), rm.timestamp());
            } else {
                logger.error("The exception = {} with a message = {} occurred", e.getClass().getSimpleName(), e.getMessage());
            }
        });
    }
}
