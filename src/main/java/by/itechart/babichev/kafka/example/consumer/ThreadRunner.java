package by.itechart.babichev.kafka.example.consumer;

import by.itechart.babichev.kafka.example.consumer.impl.BaseConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThreadRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    private static final BaseConsumer CONSUMER = new BaseConsumer();

    private static final CountDownLatch LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException {

        LOGGER.info("Create a new consumer's threads");
        TestThread thread = new TestThread(new ConsumerThread(LATCH, CONSUMER));
        thread.start();
        LATCH.await();
        thread.interrupt();
    }
}
