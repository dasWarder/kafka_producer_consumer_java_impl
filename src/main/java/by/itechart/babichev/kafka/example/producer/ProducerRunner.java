package by.itechart.babichev.kafka.example.producer;

import by.itechart.babichev.kafka.example.producer.impl.BaseProducerWithKeys;
import by.itechart.babichev.kafka.example.util.ConsoleReader;

import java.io.IOException;

public class ProducerRunner {

    private static final Producer producer = new BaseProducerWithKeys();

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
