package com.gs.photo.common.workflow;

import java.util.List;
import java.util.Map;

public interface IKafkaProperties {

    public record Topics(
        String topicDupFilteredFile,
        String topicExif,
        String topicThumb,
        String topicEvent,
        String topicImportEvent,
        String topicComponentStatus,
        String topicScanFile,
        String topicScannedFilesChild,
        String topicHashKeyOutput,
        String topicTransformedThumb,
        String pathNameTopic
    ) {}

    public int getMetaDataMaxAgeInMs();

    public String getApplicationId();

    public List<String> getBootStrapServers();

    public Map<String, KafkaConsumerProperties> getConsumersType();

    public Map<String, KafkaProducerProperties> getProducersType();

    public Topics getTopics();

    public String getSecurityKerberosName();

}