package by.itechart.babichev.kafka.example.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestThread extends Thread {

    private ConsumerThread runner;

    private Logger log = LoggerFactory.getLogger(TestThread.class);

    public TestThread(ConsumerThread runner) {
        this.runner = runner;
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public void interrupt() {
        log.info("Caught shutDown");
        runner.shutDown();
    }
}
