package com.gs.photo.workflow.consumers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ConsumerForRecordHbaseImage.class })
public class TestConsumer {

	@Autowired
	protected ConsumerForRecordHbaseImage consumer;

	@Test
	public void test() {
		ExecutorService executor = Executors.newFixedThreadPool(
			3);
		for (int i = 0; i < 1; i++) {
			executor.execute(
				() -> {
					consumer.recordIncomingMessageInHbase();
				});
		}
		synchronized (this) {

			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

}
