package com.gs.photo.workflow.service;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.dao.IHbaseImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;

@Service
public class KafkaConsumerService {

    @Autowired
    protected FileSystem              hdfsFileSystem;
    @Autowired
    protected IHbaseImageThumbnailDAO hbaseImageThumbnailDAO;
    @Value("${wf.hdfs.rootPath}")
    protected String                  rootPath;
    protected static Logger           LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    protected static final String     folder = "G:\\photos-capture-one\\divers\\photos";

    @KafkaListener(topics = "${topic.topicCheckout}", containerFactory = "KafkaListenerContainerFactory")
    public void consume(@Payload(required = false) byte[] message) {
        final HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(message);
        KafkaConsumerService.LOGGER.info("Receive, {}", hbi);

        String importName = Strings.isEmpty(hbi.getImportName()) ? "DEFAULT_IMPORT" : hbi.getImportName();
        String key = hbi.getImageId();
        final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));
        try (
            FSDataInputStream fdsOs = this.hdfsFileSystem
                .open(this.build(folderWhereRecord, "/" + hbi.getImageId() + "-" + hbi.getImageName()))) {

            FileUtils.copyInputStreamToFile(fdsOs, new File(KafkaConsumerService.folder + "/" + hbi.getImageName()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private Path build(Path rootPath2, String key) { return Path.mergePaths(rootPath2, new Path(key)); }

}