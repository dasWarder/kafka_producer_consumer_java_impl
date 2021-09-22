package by.itechart.babichev.kafka.example.producer.impl;

import by.itechart.babichev.kafka.example.producer.AbstractProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseProducerWithCallback extends AbstractProducer {

    private final Logger logger = LoggerFactory.getLogger(BaseProducerWithCallback.class);

    private final KafkaProducer<String, String> producer = super.getProducer();

    @Override
    public void sendMessage(String message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, message);

        producer.send(record, (recordMetadata, e) -> {

            if (e != null) {

                logger.error("The error = {} with occurred", e.getMessage());

            } else {

                logger.info("Record metadata received. Topic = {}, partition = {}, offset = {}, timestamp = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());

            }
        });
    }

    @Override
    public void closeProducer() {
        producer.close();
    }
}
