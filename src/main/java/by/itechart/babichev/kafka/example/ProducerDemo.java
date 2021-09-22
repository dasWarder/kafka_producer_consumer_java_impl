package by.itechart.babichev.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
public class ProducerDemo {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final String BASE_TOPIC = "firstTopic";

    private static final ConsoleReader CONSOLE_READER = new ConsoleReader();

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        while(true) {

            String data = CONSOLE_READER.readData();

            if(data.equals("exit")) {
                break;
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(BASE_TOPIC, data);
            producer.send(record);
        }

        producer.close();
    }
}
