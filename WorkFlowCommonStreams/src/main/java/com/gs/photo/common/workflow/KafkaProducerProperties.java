package com.gs.photo.common.workflow;

public record KafkaProducerProperties(
    int maxBlockMsConfig,
    int maxRequestSize,
    int maxBatchSize,
    int lingerInMillis,
    String transactionId,
    int transactionTimeout
) {}