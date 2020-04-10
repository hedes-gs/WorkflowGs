package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanArchive;
import com.gs.photo.workflow.IBeanTaskExecutor;

@Component
public class BeanArchive implements IBeanArchive {

    private static final int           BUFFER_SIZE = 4 * 1024 * 1024;

    @Autowired
    protected IBeanTaskExecutor        beanTaskExecutor;

    @Autowired
    @Qualifier("consumerForTransactionalCopyForTopicWithStringKey")
    protected Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey;

    @Value("${wf.hdfs.rootPath}")
    protected String                   rootPath;

    // @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processInputFile()); }

    private void processInputFile() {
        KafkaUtils.toStream(this.consumerForTransactionalCopyForTopicWithStringKey)
            .parallel()
            .forEach((rec) -> this.processRecord(rec.key(), rec.value()));
    }

    private void processRecord(String key, String value) {
        LocalDate localDate = LocalDate.now();
        try {
            Configuration configuration = new Configuration();
            FileSystem filesystem = FileSystem.get(configuration);
            final Path FolderWhereRecord = new Path(this.rootPath, localDate.toString());
            filesystem.mkdirs(FolderWhereRecord);

            FSDataOutputStream fdsOs = filesystem.create(this.build(FolderWhereRecord, value), true);
            FileUtils
                .copyRemoteToLocal(this.getCoordinates(value), this.getFile(value), fdsOs, BeanArchive.BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Path build(Path rootPath2, String value) { return Path.mergePaths(rootPath2, new Path(value)); }

    private String getFile(String value) { return value.substring(value.lastIndexOf("/") + 1); }

    private String getCoordinates(String value) { return value.substring(0, value.lastIndexOf("/")); }
}
