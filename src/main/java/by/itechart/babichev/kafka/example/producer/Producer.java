package by.itechart.babichev.kafka.example.producer;

public interface Producer {

    void sendMessage(String message);

    void closeProducer();
}
