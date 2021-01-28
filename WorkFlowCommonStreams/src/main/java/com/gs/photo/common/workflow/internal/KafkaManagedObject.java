package com.gs.photo.common.workflow.internal;

public class KafkaManagedObject {

    protected int  partition;
    protected long kafkaOffset;

    public int getPartition() { return this.partition; }

    public long getKafkaOffset() { return this.kafkaOffset; }

}
