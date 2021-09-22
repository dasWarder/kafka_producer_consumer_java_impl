package by.itechart.babichev.kafka.example.producer;

import by.itechart.babichev.kafka.example.util.ConsoleReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public abstract class AbstractProducer implements Producer {

    protected static final String BASE_TOPIC = "firstTopic";

    protected static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    @Override
    public abstract void sendMessage(String message);

    protected KafkaProducer getProducer() {

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }

    @Override
    public abstract void closeProducer();
}
