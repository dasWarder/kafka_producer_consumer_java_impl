package by.itechart.babichev.kafka.example.consumer;

public interface Consumer {

    void pollData();

    void closeConsumer();
}
