package com.gs.photo.workflow.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IMonitor;
import com.gs.photo.workflow.daos.ICacheNodeDAO;
import com.gs.photo.workflow.daos.IEventDAO;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

@Component
public class BeanMonitor implements IMonitor {
    @Autowired
    protected Consumer<String, WfEvents> consumerOfWfEventsWithStringKey;
    @Autowired
    protected Producer<String, String>   producerForPublishingOnStringTopic;
    @Autowired
    protected IBeanTaskExecutor          beanTaskExecutor;
    @Value("${topic.topicEvent}")
    private String                       topicEvent;
    @Value("${topic.topicProcessedFile}")
    private String                       topicProcessedFile;
    @Autowired
    protected IEventDAO                  eventDAO;
    @Autowired
    protected ICacheNodeDAO              cacheNodeDAO;

    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processInputFile()); }

    protected void processInputFile() {
        this.consumerOfWfEventsWithStringKey.subscribe(Collections.singleton(this.topicEvent));
        Iterable<ConsumerRecord<String, WfEvents>> iterable = () -> new Iterator<ConsumerRecord<String, WfEvents>>() {
            Iterator<ConsumerRecord<String, WfEvents>> records;

            @Override
            public boolean hasNext() {
                if ((this.records == null) || !this.records.hasNext()) {
                    if (this.records != null) {
                        BeanMonitor.this.consumerOfWfEventsWithStringKey.commitSync();
                    }
                    ConsumerRecords<String, WfEvents> nextRecords;
                    do {
                        nextRecords = BeanMonitor.this.consumerOfWfEventsWithStringKey.poll(Duration.ofMillis(250));
                    } while (nextRecords.isEmpty());
                    this.records = nextRecords.iterator();
                }
                return true;
            }

            @Override
            public ConsumerRecord<String, WfEvents> next() { return this.records.next(); }
        };
        StreamSupport.stream(iterable.spliterator(), true)
            .peek((rec) -> this.eventDAO.addOrCreate(rec.value()))
            .parallel()
            .peek((rec) -> this.cacheNodeDAO.addOrCreate(rec.value()))
            .parallel()
            .flatMap(
                (evts) -> evts.value()
                    .getEvents()
                    .stream())
            .filter((evt) -> this.cacheNodeDAO.allEventsAreReceivedForAnImage(evt.getImgId()))
            .forEach((evt) -> this.publishImagecompleted(evt));

    }

    private void publishImagecompleted(WfEvent evt) {
        this.producerForPublishingOnStringTopic
            .send(new ProducerRecord<String, String>(this.topicProcessedFile, evt.getImgId()));
    }

}
