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
        String pathNameTopic,
        String finalTopic,
        String inputImageNameTopic,
        String scanOutput,
        String scanOutputChildParent,
        String topicCopyOtherfile,
        String topicCountOfImagesPerDate,
        String topicDuplicateKeyImageFound,
        String topicExifImageDataToPersist,
        String topicExifSizeOfImageStream,
        String topicFileHashkey,
        String topicFullyProcessedImage,
        String topicImageDataToPersist,
        String topicImageDate,
        String topicLocalFileCopy,
        String topicProcessedFile,
        String topicScannedFiles
    ) {}

    public int getMetaDataMaxAgeInMs();

    public String getApplicationId();

    public List<String> getBootStrapServers();

    public Map<String, KafkaConsumerProperties> getConsumersType();

    public Map<String, KafkaProducerProperties> getProducersType();

    public Topics getTopics();

    public String getSecurityKerberosName();

}