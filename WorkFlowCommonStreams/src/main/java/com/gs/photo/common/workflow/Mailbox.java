package com.gs.photo.common.workflow;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mailbox<T> {
    protected static final Logger LOGGER         = LoggerFactory.getLogger(Mailbox.class);
    T                             value;
    protected ReadWriteLock       lock           = new ReentrantReadWriteLock();
    protected CountDownLatch      countDownLatch = new CountDownLatch(1);

    public void post(T v) {
        this.lock.writeLock()
            .lock();
        try {
            this.value = v;
            Mailbox.LOGGER.info("Post event {} ", v);
            this.countDownLatch.countDown();
        } finally {
            this.lock.writeLock()
                .unlock();
        }

    }

    public T read() throws InterruptedException {
        T retValue = null;
        do {
            try {
                this.lock.readLock()
                    .lock();
                retValue = this.value;
            } finally {
                this.lock.readLock()
                    .unlock();
            }
            if (retValue == null) {
                this.countDownLatch.await();
            }
            Mailbox.LOGGER.info("Read event {} ", this.value);
            this.lock.writeLock()
                .lock();
            try {
                retValue = this.value;
                this.value = null;
                this.countDownLatch = new CountDownLatch(1);
            } finally {
                this.lock.writeLock()
                    .unlock();
            }
        } while (retValue == null);
        return retValue;
    }

}