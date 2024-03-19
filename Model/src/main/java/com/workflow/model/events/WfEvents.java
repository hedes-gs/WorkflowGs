package com.workflow.model.events;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

import org.apache.avro.reflect.Nullable;

import com.nurkiewicz.typeof.TypeOf;
import com.workflow.model.HbaseData;

public class WfEvents extends HbaseData implements Serializable {

    private static final long             serialVersionUID = 1L;

    @Nullable
    protected String                      producer;

    protected Collection<WfEventInitial>  initialEvents    = new ArrayList<>();
    protected Collection<WfEventCopy>     copyEvents       = new ArrayList<>();
    protected Collection<WfEventProduced> producedEvents   = new ArrayList<>();
    protected Collection<WfEventFinal>    finalEvents      = new ArrayList<>();
    protected Collection<WfEventRecorded> recordedEvents   = new ArrayList<>();

    public String getProducer() { return this.producer; }

    private WfEvents(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.producer = builder.producer;
        this.addEvents(builder.events);
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder2 = new StringBuilder();
        builder2.append("WfEvents [producer=");
        builder2.append(this.producer);
        builder2.append(", initialEvents=");
        builder2.append(this.initialEvents != null ? this.toString(this.initialEvents, maxLen) : null);
        builder2.append(", copyEvents=");
        builder2.append(this.copyEvents != null ? this.toString(this.copyEvents, maxLen) : null);
        builder2.append(", producedEvents=");
        builder2.append(this.producedEvents != null ? this.toString(this.producedEvents, maxLen) : null);
        builder2.append(", finalEvents=");
        builder2.append(this.finalEvents != null ? this.toString(this.finalEvents, maxLen) : null);
        builder2.append(", recordedEvents=");
        builder2.append(this.recordedEvents != null ? this.toString(this.recordedEvents, maxLen) : null);
        builder2.append("]");
        return builder2.toString();
    }

    private String toString(Collection<?> collection, int maxLen) {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("[");
        int i = 0;
        for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && (i < maxLen); i++) {
            if (i > 0) {
                builder2.append(", ");
            }
            builder2.append(iterator.next());
        }
        builder2.append("]");
        return builder2.toString();
    }

    public WfEvents() { super(null,
        0); }

    private void dispatch(WfEvent c) {
        TypeOf.whenTypeOf(c)
            .is(WfEventInitial.class)
            .then((e) -> this.initialEvents.add(e))
            .is(WfEventCopy.class)
            .then((e) -> this.copyEvents.add(e))
            .is(WfEventProduced.class)
            .then((e) -> this.producedEvents.add(e))
            .is(WfEventFinal.class)
            .then((e) -> this.finalEvents.add(e))
            .is(WfEventRecorded.class)
            .then((e) -> this.recordedEvents.add(e));
    }

    public Collection<WfEvent> getEvents() {
        Collection<WfEvent> retValue = new ArrayList<>();
        retValue.addAll(this.initialEvents);
        retValue.addAll(this.copyEvents);
        retValue.addAll(this.producedEvents);
        retValue.addAll(this.finalEvents);
        retValue.addAll(this.recordedEvents);
        return Collections.unmodifiableCollection(retValue);
    }

    public <T extends WfEvent> WfEvents addEvent(T wfe) {
        this.dispatch(wfe);
        return this;
    }

    public WfEvents addEvents(Collection<? extends WfEvent> wfe) {
        wfe.forEach((w) -> this.dispatch(w));
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(
            this.copyEvents,
            this.finalEvents,
            this.initialEvents,
            this.producedEvents,
            this.producer,
            this.recordedEvents);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        WfEvents other = (WfEvents) obj;
        return Objects.equals(this.copyEvents, other.copyEvents) && Objects.equals(this.finalEvents, other.finalEvents)
            && Objects.equals(this.initialEvents, other.initialEvents)
            && Objects.equals(this.producedEvents, other.producedEvents)
            && Objects.equals(this.producer, other.producer)
            && Objects.equals(this.recordedEvents, other.recordedEvents);
    }

    /**
     * Creates builder to build {@link WfEvents}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEvents}.
     */
    public static final class Builder {
        private long                        dataCreationDate;
        private String                      dataId;
        private String                      producer;
        private Collection<WfEventInitial>  initialEvents  = Collections.emptyList();
        private Collection<WfEventCopy>     copyEvents     = Collections.emptyList();
        private Collection<WfEventProduced> producedEvents = Collections.emptyList();
        private Collection<WfEventFinal>    finalEvents    = Collections.emptyList();
        private Collection<WfEventRecorded> recordedEvents = Collections.emptyList();
        private Collection<WfEvent>         events         = Collections.emptyList();

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         *
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         *
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for producer parameter.
         *
         * @param producer
         *            field to set
         * @return builder
         */
        public Builder withProducer(String producer) {
            this.producer = producer;
            return this;
        }

        /**
         * Builder method for initialEvents parameter.
         *
         * @param initialEvents
         *            field to set
         * @return builder
         */
        public Builder withInitialEvents(Collection<WfEventInitial> initialEvents) {
            this.initialEvents = initialEvents;
            return this;
        }

        /**
         * Builder method for copyEvents parameter.
         *
         * @param copyEvents
         *            field to set
         * @return builder
         */
        public Builder withCopyEvents(Collection<WfEventCopy> copyEvents) {
            this.copyEvents = copyEvents;
            return this;
        }

        /**
         * Builder method for producedEvents parameter.
         *
         * @param producedEvents
         *            field to set
         * @return builder
         */
        public Builder withProducedEvents(Collection<WfEventProduced> producedEvents) {
            this.producedEvents = producedEvents;
            return this;
        }

        /**
         * Builder method for finalEvents parameter.
         *
         * @param finalEvents
         *            field to set
         * @return builder
         */
        public Builder withFinalEvents(Collection<WfEventFinal> finalEvents) {
            this.finalEvents = finalEvents;
            return this;
        }

        /**
         * Builder method for recordedEvents parameter.
         *
         * @param recordedEvents
         *            field to set
         * @return builder
         */
        public Builder withRecordedEvents(Collection<WfEventRecorded> recordedEvents) {
            this.recordedEvents = recordedEvents;
            return this;
        }

        public Builder withEvents(Collection<WfEvent> events) {
            this.events = events;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public WfEvents build() { return new WfEvents(this); }
    }

}
