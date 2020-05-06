package com.gs.photo.workflow;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class TimeMeasurement implements Closeable {
    public static interface Handler { public void onClose(List<Step> steps); }

    public static class Step implements Comparable<Step> {
        protected long   initialTime;
        protected long   creationDate;
        protected float  duration;
        protected String name;

        @Override
        public String toString() { return "[ " + this.name + " : " + this.duration + " sec]"; }

        public Step computeDuration() {
            this.duration = (this.creationDate - this.initialTime) / 1000.0f;
            return this;
        }

        public Step(
            long initialTime,
            long creationDate,
            String name
        ) {
            super();
            this.initialTime = initialTime;
            this.creationDate = creationDate;
            this.name = name;
        }

        @Override
        public int compareTo(Step o) { return Long.compare(this.creationDate, o.creationDate); }

    }

    public void addStep(String name) {
        TimeMeasurement.times.get()
            .put(this.id, new Step(this.initialTime, System.currentTimeMillis(), name));
    }

    public TimeMeasurement(
        String id,
        Handler handler,
        long initialTime
    ) {
        this.initialTime = initialTime;
        this.id = id;
        this.handler = handler;
        this.addStep("CREATE");
    }

    public static TimeMeasurement of(String id, Handler handler, long initialTime) {
        return new TimeMeasurement(id, handler, initialTime);
    }

    protected static ThreadLocal<Multimap<String, Step>> times = ThreadLocal.withInitial(() -> TreeMultimap.create());
    protected Handler                                    handler;
    protected String                                     id;
    protected long                                       initialTime;

    @Override
    public void close() throws IOException {
        this.addStep("END");
        this.handler.onClose(
            TimeMeasurement.times.get()
                .get(this.id)
                .stream()
                .map((t) -> t.computeDuration())
                .collect(Collectors.toList()));
        TimeMeasurement.times.remove();
    }

}
