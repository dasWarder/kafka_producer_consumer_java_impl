package by.itechart.babichev.kafka.example.consumer.impl;

import by.itechart.babichev.kafka.example.consumer.AbstractConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class BaseConsumer extends AbstractConsumer {

    private static final Duration DURATION = Duration.ofMillis(100);

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    public BaseConsumer() {
        super();
    }

    public BaseConsumer(String... topics) {
        super(topics);
    }

    public BaseConsumer(String group) {
        super(group);
    }

    public BaseConsumer(String group, String... topics) {
        super(group, topics);
    }

    @Override
    public void pollData() {

        ConsumerRecords<String, String> records = CONSUMER.poll(DURATION);

        for (ConsumerRecord<String, String> record : records) {

            logger.info("Consumer: {}. Key = {}, value = {}, partition = {}, offset = {}",
                    CONSUMER.toString(), record.key(), record.value(), record.partition(), record.offset());
            System.out.println(record.value());
        }
    }
}
