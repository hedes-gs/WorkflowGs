package com.gs.photo.workflow;

import javax.annotation.Generated;

import com.workflow.model.events.WfEvent;

public class GraphNodeLink<T extends WfEvent, U extends WfEvent> {
    protected GraphNode<T> source;
    protected GraphNode<U> target;

    @Generated("SparkTools")
    private GraphNodeLink(Builder<T, U> builder) {
        this.source = builder.previous;
        this.target = builder.next;
    }

    /**
     * Creates builder to build {@link GraphNodeLink}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static <T extends WfEvent, U extends WfEvent> Builder<T, U> builder() { return new Builder<>(); }

    /**
     * Builder to build {@link GraphNodeLink}.
     */
    @Generated("SparkTools")
    public static final class Builder<T extends WfEvent, U extends WfEvent> {
        private GraphNode<T> previous;
        private GraphNode<U> next;

        private Builder() {}

        /**
         * Builder method for previous parameter.
         *
         * @param previous
         *            field to set
         * @return builder
         */
        public Builder<T, U> withPrevious(GraphNode<T> previous) {
            this.previous = previous;
            return this;
        }

        /**
         * Builder method for next parameter.
         *
         * @param next
         *            field to set
         * @return builder
         */
        public Builder<T, U> withNext(GraphNode<U> next) {
            this.next = next;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public GraphNodeLink<T, U> build() { return new GraphNodeLink<T, U>(this); }
    }

}