package com.gs.photo.common.workflow;

import java.util.Map;

public interface IIgniteProperties {

    public String getDefaultCache();

    public Map<String, String> getCachesPerclasses();
}
