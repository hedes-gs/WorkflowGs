package com.gs.photo.common.workflow;

import java.util.Map;

public interface IApplicationProperties {

    public Map<String, String> getTopics();

    public IKafkaProperties getKafkaProperties();

}
