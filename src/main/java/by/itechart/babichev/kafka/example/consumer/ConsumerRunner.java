package by.itechart.babichev.kafka.example.consumer;

import by.itechart.babichev.kafka.example.consumer.impl.AssignSeekConsumer;

public class ConsumerRunner {

    private static final Consumer CONSUMER = new AssignSeekConsumer
            ("firstTopic", 0, 70, 3);

    public static void main(String[] args) {
        CONSUMER.pollData();
    }
}
