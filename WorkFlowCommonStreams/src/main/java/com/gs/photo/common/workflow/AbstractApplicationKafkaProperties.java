package com.gs.photo.common.workflow;

import java.util.List;
import java.util.Map;

public abstract class AbstractApplicationKafkaProperties implements IKafkaProperties {

    protected int                                  metaDataMaxAgeInMs;
    protected String                               applicationId;
    protected List<String>                         BootStrapServers;
    protected Topics                               topics;
    protected String                               securityKerberosName;
    protected Map<String, KafkaConsumerProperties> consumersType;
    protected Map<String, KafkaProducerProperties> producersType;

    @Override
    public int getMetaDataMaxAgeInMs() { return this.metaDataMaxAgeInMs; }

    public void setMetaDataMaxAgeInMs(int metaDataMaxAge) { this.metaDataMaxAgeInMs = metaDataMaxAge; }

    @Override
    public String getApplicationId() { return this.applicationId; }

    public void setApplicationId(String applicationId) { this.applicationId = applicationId; }

    @Override
    public List<String> getBootStrapServers() { return this.BootStrapServers; }

    public void setBootStrapServers(List<String> bootStrapServers) { this.BootStrapServers = bootStrapServers; }

    @Override
    public Topics getTopics() { return this.topics; }

    public void setTopics(Topics topics) { this.topics = topics; }

    @Override
    public String getSecurityKerberosName() { return this.securityKerberosName; }

    public void setSecurityKerberosName(String securityKerberosName) {
        this.securityKerberosName = securityKerberosName;
    }

    @Override
    public Map<String, KafkaConsumerProperties> getConsumersType() { return this.consumersType; }

    public void setConsumersType(Map<String, KafkaConsumerProperties> consumersType) {
        this.consumersType = consumersType;
    }

    @Override
    public Map<String, KafkaProducerProperties> getProducersType() { return this.producersType; }

    public void setProducersType(Map<String, KafkaProducerProperties> producersType) {
        this.producersType = producersType;
    }

}
