package by.itechart.babichev.kafka.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public abstract class AbstractConsumer implements Consumer {

    protected static final String GROUP_ID = "test_1";

    protected static final String AUTO_OFFSET = "earliest";

    protected static final String BASE_TOPIC = "firstTopic";

    protected static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    protected static final String STRING_DESERIALIZER = StringDeserializer.class.getName();

    protected static KafkaConsumer<String, String> CONSUMER;

    public AbstractConsumer(String group) {

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET);

        CONSUMER = new KafkaConsumer<>(properties);

        this.subscribeTopics(BASE_TOPIC);
    }

    public AbstractConsumer() {
       this(GROUP_ID);
    }

    public AbstractConsumer(String... topics) {
        this();
        this.subscribeTopics(topics);
    }

    public AbstractConsumer(String group, String... topics) {
        this(group);
        this.subscribeTopics(topics);
    }

    @Override
    public void interruptConsumer() {
        CONSUMER.wakeup();
    }

    @Override
    public void closeConsumer() {
        CONSUMER.close();
    }

    @Override
    public abstract void pollData();


    private void subscribeTopics(String... topics) {

        List<String> topicsList = Arrays.stream(topics).collect(Collectors.toUnmodifiableList());

        CONSUMER.subscribe(topicsList);
    }
}
