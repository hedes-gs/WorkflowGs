package com.gs.photo.common.workflow;

public record KafkaConsumerProperties(
    int batchSizeForParallelProcessingIncomingRecords,
    int consumerFetchMaxBytes,
    int retryBackoffMaxRequestSize,
    int reconnectBackoffMs,
    int retryBackoffMs,
    int heartbeatIntervallMs,
    int sessionTimeoutMs,
    String groupId,
    String instanceGroupId,
    int fetchMaxBytes,
    int maxPollRecords,
    int sessionTimeout,
    int fetchMaxWaitMs,
    int maxPollIntervallMs
) {

}