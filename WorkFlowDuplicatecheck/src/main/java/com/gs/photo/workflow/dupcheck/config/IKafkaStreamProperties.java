package com.gs.photo.workflow.dupcheck.config;

import com.gs.photo.common.workflow.IKafkaProperties;

public interface IKafkaStreamProperties extends IKafkaProperties {
    public String getKafkaStreamDir();

    public int getCommitIntervalIms();

    public int getCacheMaxBytesBuffering();

    public int getNbOfThreads();

    public String getStoreName();

    public boolean isCleanupRequired();
}
