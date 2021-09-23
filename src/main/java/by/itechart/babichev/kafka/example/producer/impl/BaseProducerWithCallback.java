package by.itechart.babichev.kafka.example.producer.impl;

import by.itechart.babichev.kafka.example.producer.AbstractProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseProducerWithCallback extends AbstractProducer {

    private static final Logger logger = LoggerFactory.getLogger(BaseProducerWithCallback.class);

    public BaseProducerWithCallback() {
        super();
    }

    public BaseProducerWithCallback(String topic) {
        super(topic);
    }

    @Override
    public void sendMessage(String message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, message);
        PRODUCER.send(record, (rm, e) -> {

            if (e != null) {
                logger.error("The error = {} with occurred", e.getMessage());
            } else {
                logger.info("Record metadata received. Topic = {}, partition = {}, offset = {}, timestamp = {}",
                        rm.topic(), rm.partition(), rm.offset(), rm.timestamp());
            }
        });
    }
}
