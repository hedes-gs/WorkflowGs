package com.gs.photo.common.workflow;

public interface IKafkaStreamProperties extends IKafkaProperties {
    public String getKafkaStreamDir();

    public int getCommitIntervalIms();

    public int getCacheMaxBytesBuffering();

    public int getNbOfThreads();

    public String getStoreName();

    public boolean isCleanupRequired();
}
