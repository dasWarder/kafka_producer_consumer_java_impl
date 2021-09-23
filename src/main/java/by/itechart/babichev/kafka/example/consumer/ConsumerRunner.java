package by.itechart.babichev.kafka.example.consumer;

import by.itechart.babichev.kafka.example.consumer.impl.BaseConsumer;

public class ConsumerRunner {

    private static final BaseConsumer consumer = new BaseConsumer();

    public static void main(String[] args) {

        while(true) {
            consumer.pollData();
        }
    }
}
