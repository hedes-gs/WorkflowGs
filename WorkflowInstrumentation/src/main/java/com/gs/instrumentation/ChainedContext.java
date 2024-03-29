package com.gs.instrumentation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.micrometer.core.instrument.Timer;

public class ChainedContext {
    protected Thread               currentThread;
    protected Set<Timer>           currentTimers;
    protected List<ChainedContext> children;

    public Thread getCurrentThread() { return this.currentThread; }

    public void setCurrentThread(Thread currentThread) { this.currentThread = currentThread; }

    public Set<Timer> getCurrentTimers() { return this.currentTimers; }

    public void setCurrentTimers(Set<Timer> currentTimers) { this.currentTimers = currentTimers; }

    public List<ChainedContext> getChildren() { return this.children; }

    public void setChildren(List<ChainedContext> children) { this.children = children; }

    public ChainedContext(Thread currentThread) {
        super();
        this.currentThread = currentThread;
        this.currentTimers = new HashSet<Timer>();
        this.children = new ArrayList<ChainedContext>();
    }

}
