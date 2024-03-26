package com.gs.photo.workflow.recinhbase.business;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Component;

import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;

@Component
public class ProcessKafkaEvent<R extends WfEvent, T extends HbaseData> implements IProcessKafkaEvent<R, T> {

    protected Map<String, IPersistRecordsInDatabase<R, T>> persistRecordsInDatabaseMap;

    @Override
    public CompletableFuture<Collection<R>> asyncProcess(Class<T> cl, Collection<T> collection) {
        return this.persistRecordsInDatabaseMap.get(cl.getSimpleName())
            .persist(collection);
    }

    public ProcessKafkaEvent(Map<String, IPersistRecordsInDatabase<R, T>> persistRecordsInDatabaseMap) {
        this.persistRecordsInDatabaseMap = persistRecordsInDatabaseMap;
    }

}
