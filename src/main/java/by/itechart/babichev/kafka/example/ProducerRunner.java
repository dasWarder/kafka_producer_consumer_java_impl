package by.itechart.babichev.kafka.example;

import by.itechart.babichev.kafka.example.producer.impl.BaseProducer;
import by.itechart.babichev.kafka.example.producer.Producer;
import by.itechart.babichev.kafka.example.producer.impl.BaseProducerWithCallback;
import by.itechart.babichev.kafka.example.util.ConsoleReader;

import java.io.IOException;

public class ProducerRunner {

    private static final Producer producer = new BaseProducerWithCallback();

    protected static final ConsoleReader CONSOLE_READER = new ConsoleReader();

    public static void main(String[] args) throws IOException {


        while (true) {

            String data = CONSOLE_READER.readData();

            if(data.equals("exit")) {
                break;
            }
            
            producer.sendMessage(data);
        }

        producer.closeProducer();
        CONSOLE_READER.closeReader();
    }
}
