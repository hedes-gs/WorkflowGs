package com.gs.photo.workflow.copyfiles.config;

public class SpecificApplicationProperties implements ISpecificApplicationProperties {
    protected String repository;
    protected int    maxNumberOfFilesInAFolder;

    public String getRepository() { return this.repository; }

    public void setRepository(String repository) { this.repository = repository; }

    public int getMaxNumberOfFilesInAFolder() { return this.maxNumberOfFilesInAFolder; }

    public void setMaxNumberOfFilesInAFolder(int maxNumberOfFilesInAFolder) {
        this.maxNumberOfFilesInAFolder = maxNumberOfFilesInAFolder;
    }

}
