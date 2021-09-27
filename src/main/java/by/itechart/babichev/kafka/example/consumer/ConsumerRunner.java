package by.itechart.babichev.kafka.example.consumer;

import by.itechart.babichev.kafka.example.consumer.impl.BaseConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    private static final BaseConsumer CONSUMER = new BaseConsumer();

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    public static void main(String[] args) {

        LOGGER.info("Create a enw consumer thread");
        ConsumerThread consumer= new ConsumerThread(LATCH, CONSUMER);

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOGGER.info("Caught shutdown");
            consumer.shoutDown();
            try {
                LATCH.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has exited");
        }));

    }
}
