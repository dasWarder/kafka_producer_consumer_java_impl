package by.itechart.babichev.kafka.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public abstract class AbstractProducer implements Producer {

    protected static String BASE_TOPIC = "firstTopic";

    protected static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    protected static final String STRING_SERIALIZER = StringSerializer.class.getName();

    protected static KafkaProducer<String, String> PRODUCER;

    public AbstractProducer() {

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);

        PRODUCER = new KafkaProducer<>(properties);
    }

    public AbstractProducer(String topic) {
        this();
        this.BASE_TOPIC = topic;
    }

    @Override
    public abstract void sendMessage(String message);

    @Override
    public void closeProducer() {
        PRODUCER.close();
    }
}
