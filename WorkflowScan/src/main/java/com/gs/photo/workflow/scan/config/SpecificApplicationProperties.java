package com.gs.photo.workflow.scan.config;

import java.util.List;

public class SpecificApplicationProperties {
    protected List<String> scannedFolder;
    protected List<String> fileExtensions;
    protected int          heartBeatTime;

    public List<String> getScannedFolder() { return this.scannedFolder; }

    public void setScannedFolder(List<String> scannedFolder) { this.scannedFolder = scannedFolder; }

    public List<String> getFileExtensions() { return this.fileExtensions; }

    public void setFileExtensions(List<String> fileExtensions) { this.fileExtensions = fileExtensions; }

    public int getHeartBeatTime() { return this.heartBeatTime; }

    public void setHeartBeatTime(int heartBeatTime) { this.heartBeatTime = heartBeatTime; }

}
