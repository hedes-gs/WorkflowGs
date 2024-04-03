package com.gs.instrumentation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.micrometer.core.instrument.Timer;

public class ChainedContext {

    protected ChainedContext       parent;
    protected Thread               currentThread;
    protected Map<Object, Object>  globalContext;
    protected Map<String, Integer> nbOfSentObjectsPerTopic;
    protected Set<Timer>           currentTimers;
    protected List<ChainedContext> children;

    public Thread getCurrentThread() { return this.currentThread; }

    public void setCurrentThread(Thread currentThread) { this.currentThread = currentThread; }

    public Set<Timer> getCurrentTimers() { return this.currentTimers; }

    public void setCurrentTimers(Set<Timer> currentTimers) { this.currentTimers = currentTimers; }

    public List<ChainedContext> getChildren() { return this.children; }

    public void setChildren(List<ChainedContext> children) { this.children = children; }

    public Map<String, Integer> getNbOfSentObjectsPerTopic() { return this.nbOfSentObjectsPerTopic; }

    public void setNbOfSentObjectsPerTopic(Map<String, Integer> nbOfSentObjectsPerTopic) {
        this.nbOfSentObjectsPerTopic = nbOfSentObjectsPerTopic;
    }

    public Map<Object, Object> getGlobalContext() { return this.globalContext; }

    public void addKafkaContext(Map<?, ?> kafkaContext) {
        this.globalContext.put("CONTEXT-KAFKA-" + Thread.currentThread(), kafkaContext);
        System.out.println(Thread.currentThread() + " - addKafkaContext " + this.globalContext);

    }

    public void deleteKafkaContext() {
        ChainedContext currentChainedContext = this;
        while (true) {
            if (this.globalContext.get("CONTEXT-KAFKA-" + currentChainedContext.currentThread) != null) {
                this.globalContext.remove("CONTEXT-KAFKA-" + currentChainedContext.currentThread);
            }
            currentChainedContext = currentChainedContext.parent;
            if (currentChainedContext == null) {
                break;
            }
        }
    }

    public Map<?, ?> getKafkaContext() {
        ChainedContext currentChainedContext = this;
        while (true) {
            final String key = "CONTEXT-KAFKA-" + currentChainedContext.currentThread;
            if (this.globalContext.get(key) != null) { return (Map<?, ?>) this.globalContext.get(key); }
            currentChainedContext = currentChainedContext.parent;
            if (currentChainedContext == null) {
                break;
            }
        }
        System.out.println(Thread.currentThread() + " - unable to find a kafka context " + this.globalContext);
        return Collections.EMPTY_MAP;
    }

    public ChainedContext(
        Thread currentThread,
        Map<Object, Object> globalContext,
        ChainedContext parent
    ) {
        super();
        this.currentThread = currentThread;
        this.nbOfSentObjectsPerTopic = new HashMap<>();
        this.currentTimers = new HashSet<Timer>();
        this.children = new ArrayList<ChainedContext>();
        this.globalContext = globalContext;
        this.parent = parent;
    }

}
