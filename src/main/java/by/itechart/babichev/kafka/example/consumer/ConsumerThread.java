package by.itechart.babichev.kafka.example.consumer;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private CountDownLatch countDownLatch;

    private Consumer consumer;

    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    public ConsumerThread(CountDownLatch countDownLatch, Consumer consumer) {
        this.countDownLatch = countDownLatch;
        this.consumer = consumer;
    }

    @Override
    public void run() {

        try {
            while (true) {
                consumer.pollData();
            }
        } catch (WakeupException e) {
            logger.info("Received shut down signal");
        } finally {
            consumer.closeConsumer();
            countDownLatch.countDown();
        }
    }

    public void shutDown() {
        consumer.interruptConsumer();
    }
}
