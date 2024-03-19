package com.gs.photo.workflow;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;

@Component
public class BeanPollFolder {

    protected static final Logger LOGGER = Logger.getLogger(BeanPollFolder.class);
    @Value("${polledFolders}")
    protected String              polledFolders;

    @Autowired
    protected IBeanProcessImage   beanProcessImage;
    @Autowired
    protected IBeanTaskExecutor   beanTaskExecutor;

    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> { this.poll(); }); }

    public void poll() {
        try {
            BeanPollFolder.LOGGER.info("Starting to poll " + this.polledFolders);

            File dirFile = new File(this.polledFolders);
            Set<String> setOfProcessedFiles = new HashSet<>();
            for (;;) {
                String[] listDir = dirFile.list();
                for (String element : listDir) {
                    String fileToProcess = this.polledFolders + "/" + element;
                    if (!setOfProcessedFiles.contains(fileToProcess)) {
                        this.process(fileToProcess);
                        setOfProcessedFiles.add(fileToProcess);
                    }
                }
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            BeanPollFolder.LOGGER.warn("Warning", e);
        }

    }

    private void process(final String child) {
        this.beanTaskExecutor.execute(() -> { this.beanProcessImage.process(child); });
    }
}
