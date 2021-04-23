package com.gs.photo.workflow.consumers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.gs.photo.workflow.recinhbase.consumers.ConsumerForRecordHbaseImage;

// @SpringBootTest(classes = { ConsumerForRecordHbaseImage.class })
public class TestConsumer {

    // @Autowired
    protected ConsumerForRecordHbaseImage consumer;

    // @Test
    public void test() {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 1; i++) {
            executor.execute(() -> { this.consumer.processIncomingMessages(); });
        }
        synchronized (this) {

            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

}
