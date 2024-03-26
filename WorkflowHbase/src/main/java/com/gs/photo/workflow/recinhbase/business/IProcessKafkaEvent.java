package com.gs.photo.workflow.recinhbase.business;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;

public interface IProcessKafkaEvent<R extends WfEvent, T extends HbaseData> {
    public CompletableFuture<Collection<R>> asyncProcess(Class<T> cl, Collection<T> collection);

}
