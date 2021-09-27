package by.itechart.babichev.kafka.example.consumer.impl;

import by.itechart.babichev.kafka.example.consumer.AbstractConsumer;
import by.itechart.babichev.kafka.example.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignSeekConsumer implements Consumer {

    private static final String AUTO_OFFSET = "earliest";

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    private static final Duration DURATION = Duration.ofMillis(100);

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private static final String STRING_DESERIALIZER = StringDeserializer.class.getName();

    private static String topic = "firstTopic";

    private static int partition = 0;

    private static long offset = 1;

    private static long numberOfMessageToRead = 5;

    private KafkaConsumer<String, String> consumer;

    public AssignSeekConsumer() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET);

        this.consumer = new KafkaConsumer<>(properties);
    }

    public AssignSeekConsumer(String topic) {
        this();
        this.topic = topic;
    }

    public AssignSeekConsumer(String topic, int partition, long offsetReadFrom) {
        this(topic);
        this.partition = partition;
        this.offset = offsetReadFrom;
    }

    public AssignSeekConsumer(String topic, int partition, long offsetReadFrom, long numberOfMessageToRead) {
        this(topic, partition, offsetReadFrom);
        this.numberOfMessageToRead = numberOfMessageToRead;
    }

    @Override
    public void pollData() {
        TopicPartition partition = new TopicPartition(this.topic, this.partition);
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition, offset);


        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(DURATION);

            int count = 0;

            for (ConsumerRecord<String, String> record : records) {


                logger.info("Consumer: {}. Key = {}, value = {}, partition = {}, offset = {}",
                        consumer.toString(), record.key(), record.value(), record.partition(), record.offset());
                System.out.println(record.value());
                count++;

                if(count >= numberOfMessageToRead) {
                    break;
                }
            }
        }
    }

    @Override
    public void closeConsumer() {
        consumer.close();
    }

    @Override
    public void interruptConsumer() {
        consumer.wakeup();
    }
}
