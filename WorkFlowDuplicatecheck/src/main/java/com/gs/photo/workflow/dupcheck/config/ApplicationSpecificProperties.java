package com.gs.photo.workflow.dupcheck.config;

import com.gs.photo.common.workflow.AbstractApplicationKafkaProperties;

public class ApplicationSpecificProperties extends AbstractApplicationKafkaProperties
    implements IKafkaStreamProperties {

    protected String  kafkaStreamDir;
    protected int     commitIntervalIms;
    protected String  storeName;
    protected int     cacheMaxBytesBuffering;
    protected int     nbOfThreads;
    protected boolean cleanupRequired;

    @Override
    public String getKafkaStreamDir() { return this.kafkaStreamDir; }

    public void setKafkaStreamDir(String kafkaStreamDir) { this.kafkaStreamDir = kafkaStreamDir; }

    @Override
    public int getCommitIntervalIms() { return this.commitIntervalIms; }

    public void setCommitIntervalIms(int commitIntervalIms) { this.commitIntervalIms = commitIntervalIms; }

    @Override
    public int getCacheMaxBytesBuffering() { return this.cacheMaxBytesBuffering; }

    public void setCacheMaxBytesBuffering(int cacheMaxBytesBuffering) {
        this.cacheMaxBytesBuffering = cacheMaxBytesBuffering;
    }

    @Override
    public int getNbOfThreads() { return this.nbOfThreads; }

    public void setNbOfThreads(int nbOfThreads) { this.nbOfThreads = nbOfThreads; }

    @Override
    public String getStoreName() { return this.storeName; }

    public void setStoreName(String storeName) { this.storeName = storeName; }

    @Override
    public boolean isCleanupRequired() { return this.cleanupRequired; }

    public void setCleanupRequired(boolean cleanupRequired) { this.cleanupRequired = cleanupRequired; }

}
