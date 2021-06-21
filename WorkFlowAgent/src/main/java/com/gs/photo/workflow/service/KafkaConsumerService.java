package com.gs.photo.workflow.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.KafkaConsumerConfig;
import com.gs.photo.workflow.dao.IHbaseImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;

@Service
public class KafkaConsumerService implements ConsumerSeekAware {

    @Autowired
    protected FileSystem                                   hdfsFileSystem;
    @Autowired
    protected IHbaseImageThumbnailDAO                      hbaseImageThumbnailDAO;
    @Value("${wf.hdfs.rootPath}")
    protected String                                       rootPath;
    protected static Logger                                LOGGER                  = LoggerFactory
        .getLogger(KafkaConsumerService.class);

    @Value("${wf.outputpath}")
    protected String                                       folder;

    @Value("${wf.watched.subdirPattern}")
    protected String                                       whatchedSubDirFolderPattern;

    @Value("${topic.topicUpdate}")
    protected String                                       topicUpdate;

    @Value("${group.id}")
    protected String                                       groupId;

    @Autowired
    protected IBeanTaskExecutor                            beanTaskExecutor;

    @Autowired
    protected KafkaTemplate<String, String>                kafkaTemplateForTopicUpdate;

    protected Set<HbaseImageThumbnail>                     exported                = ConcurrentHashMap.newKeySet();
    protected Set<File>                                    updatesToIgnore         = ConcurrentHashMap.newKeySet();
    protected NavigableMap<Long, List<java.nio.file.Path>> timeStampOfFiles        = new ConcurrentSkipListMap<>();
    protected NavigableMap<java.nio.file.Path, Long>       filesWithTheirTimeStamp = new ConcurrentSkipListMap<>();

    protected ReentrantLock                                lock                    = new ReentrantLock();

    @KafkaListener(topics = "${topic.topicCheckout}", containerFactory = "KafkaListenerContainerFactory")
    public void consume(@Payload(required = false) byte[] message) {
        final HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(message);
        KafkaConsumerService.LOGGER.info("Receive, {}", hbi);

        String importName = hbi.getImportName()
            .stream()
            .findAny()
            .orElse("DEFAULT_IMPORT");
        String key = hbi.getImageId();
        final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));

        this.exported.add(hbi);
        String mainImage = hbi.getImageId() + "-" + hbi.getImageName();

        try {
            final RemoteIterator<LocatedFileStatus> listFiles = this.hdfsFileSystem.listFiles(folderWhereRecord, true);
            while (listFiles.hasNext()) {
                LocatedFileStatus fileStatus = listFiles.next();
                if (mainImage.equals(
                    fileStatus.getPath()
                        .getName())) {
                    final File destFile = new File(this.folder + "/" + hbi.getImageName());
                    this.copyLocally(fileStatus.getPath(), destFile, false);
                }
            }
        } catch (IOException e) {
            KafkaConsumerService.LOGGER.error("Unexpecterd error  {}", ExceptionUtils.getStackTrace(e));
        }

    }

    @KafkaListener(topics = "${topic.topicUpdate}", containerFactory = "kafkaListenerContainerFactoryForTopicUpdate")
    public void updateImagesFile(
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String emettor,
        @Payload(required = false) String file
    ) {
        if (!emettor.startsWith(this.computeEmettorId())) {
            KafkaConsumerService.LOGGER.info("Receive update on file  {}", file);
            int pos = file.indexOf(this.whatchedSubDirFolderPattern);
            if (pos >= 0) {
                final Path hdfsSourceFile = new Path(file);
                String subDir = file.substring(pos);
                final File destFile = new File(this.folder, subDir);
                try {
                    this.updatesToIgnore.add(destFile);
                    if (this.hdfsFileSystem.getFileStatus(hdfsSourceFile)
                        .getModificationTime() > (destFile.lastModified() + 100)) {
                        this.copyLocally(hdfsSourceFile, destFile, true);
                    } else {
                        KafkaConsumerService.LOGGER.info("Not updateing locally file {} : dates are equals", file);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    this.updatesToIgnore.remove(destFile);
                }
            }
        }
    }

    protected String computeEmettorId() { return this.groupId + "-" + KafkaConsumerConfig.SEED_ID; }

    protected void copyLocally(final Path folderWhereRecord, final File destFile, boolean forceWrite) {
        if (forceWrite || !destFile.exists()) {
            try (
                FSDataInputStream fdsOs = this.hdfsFileSystem.open(folderWhereRecord)) {
                KafkaConsumerService.LOGGER.info("copying {} to {}", folderWhereRecord, destFile);
                FileUtils.copyInputStreamToFile(fdsOs, destFile);
            } catch (IOException e) {
                KafkaConsumerService.LOGGER
                    .error("Dest file {} already exists: not copying, {}", destFile, ExceptionUtils.getStackTrace(e));
            }
        } else {
            KafkaConsumerService.LOGGER.info("Dest file {} already exists: not copying", destFile);
        }
    }

    private Path build(Path rootPath2, String key) { return Path.mergePaths(rootPath2, new Path("/" + key)); }

    protected Stream<java.nio.file.Path> recordInHdfs(java.nio.file.Path file) {
        return this.exported.stream()
            .filter(
                (c) -> file.getFileName()
                    .toString()
                    .contains(c.getImageName()))
            .map((c) -> this.recordInHdfs(c, file))
            .filter((c) -> c != null);
    }

    protected java.nio.file.Path recordInHdfs(HbaseImageThumbnail hbi, java.nio.file.Path file) {
        if (!this.updatesToIgnore.contains(file.toFile())) {
            final Path hdfsDestdir = this.getHdfsDirectoryPath(hbi, file);
            try {
                if (!this.hdfsFileSystem.exists(hdfsDestdir)) {
                    this.hdfsFileSystem.mkdirs(hdfsDestdir);
                }
            } catch (Exception e) {
                KafkaConsumerService.LOGGER
                    .error("Error when recording {} : {} ", hdfsDestdir, ExceptionUtils.getStackTrace(e));
            }
            final Path hdfsDestFile = this.build(
                hdfsDestdir,
                file.getFileName()
                    .toString());
            KafkaConsumerService.LOGGER.info("Copying {} to  {} ", file, hdfsDestFile);
            try (
                FSDataOutputStream fdsOs = this.hdfsFileSystem.create(hdfsDestFile, true);) {
                FileUtils.copyFile(file.toFile(), fdsOs);
                return file;

            } catch (IOException e) {
                KafkaConsumerService.LOGGER
                    .error("ERROR when Copying {} to  {} : {}", file, hdfsDestFile, e.getMessage());
            }
        }
        return null;

    }

    protected Path getHdfsDirectoryPath(HbaseImageThumbnail hbi, java.nio.file.Path file) {
        String importName = hbi.getImportName()
            .stream()
            .findAny()
            .orElse("DEFAULT_IMPORT");
        String key = hbi.getImageId();

        final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));

        return this.build(
            folderWhereRecord,
            file.toAbsolutePath()
                .getParent()
                .toString()
                .substring(this.folder.length() + 1));
    }

    protected Path getHdfsPath(HbaseImageThumbnail hbi, java.nio.file.Path file) {
        String importName = hbi.getImportName()
            .stream()
            .findAny()
            .orElse("DEFAULT_IMPORT");
        String key = hbi.getImageId();

        final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));
        return this.build(
            folderWhereRecord,
            file.toAbsolutePath()
                .toString()
                .substring(this.folder.length() + 1));
    }

    protected void processMap() {
        long youngerFile = 0;
        try {
            while (true) {
                Thread.sleep(10000);
                do {
                    youngerFile = !this.timeStampOfFiles.isEmpty() ? this.timeStampOfFiles.firstKey() : 0;
                    KafkaConsumerService.LOGGER
                        .info("Younger file is {} - nb of elements {} ", youngerFile, this.timeStampOfFiles.size());
                    if ((youngerFile != 0) && ((System.currentTimeMillis() - youngerFile) > 10000)) {
                        KafkaConsumerService.LOGGER.info("Processing changes...");
                        try {
                            this.lock.lock();
                            if (!this.timeStampOfFiles.isEmpty()) {
                                KafkaConsumerService.LOGGER.info(
                                    "Detected changes on files, dates are {} - current time is {}",
                                    youngerFile,
                                    System.currentTimeMillis());
                                if ((System.currentTimeMillis() - youngerFile) > 10000) {
                                    this.timeStampOfFiles.get(youngerFile)
                                        .stream()
                                        .peek((c) -> KafkaConsumerService.LOGGER.info("Processing file {} ", c))
                                        .flatMap((c) -> this.recordInHdfs(c))
                                        .map((c) -> this.cleanEntry(c))
                                        .flatMap((c) -> this.toHdfsPath(c))
                                        .map((c) -> this.sendForUpdate(c))
                                        .forEach((c) -> KafkaConsumerService.LOGGER.info("End of action on {}", c));
                                    this.timeStampOfFiles.remove(youngerFile);

                                }
                            }

                        } finally {
                            this.lock.unlock();
                        }

                    }
                } while ((youngerFile != 0) && ((System.currentTimeMillis() - youngerFile) > 10000));
            }
        } catch (InterruptedException e) {
            KafkaConsumerService.LOGGER.info("Stopping...");
        } catch (Exception e) {
            KafkaConsumerService.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
        }
    }

    private java.nio.file.Path cleanEntry(java.nio.file.Path c) {
        this.filesWithTheirTimeStamp.remove(c);
        return c;
    }

    private Stream<String> toHdfsPath(java.nio.file.Path file) {
        return this.exported.stream()
            .filter(
                (c) -> file.getFileName()
                    .toString()
                    .contains(c.getImageName()))
            .map((c) -> this.getHdfsPath(c, file))
            .map((c) -> c.toString());

    }

    protected void listenForDirectoryEvents(java.nio.file.Path whatchedFolder) {
        KafkaConsumerService.LOGGER.info("Start listening {} ", whatchedFolder);
        try {
            WatchService watchService = FileSystems.getDefault()
                .newWatchService();
            whatchedFolder.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY);

            for (;;) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException x) {
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {

                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }
                    WatchEvent<java.nio.file.Path> ev = (WatchEvent<java.nio.file.Path>) event;
                    final java.nio.file.Path fileName = whatchedFolder.resolve(ev.context());
                    this.exported.stream()
                        .filter(
                            (c) -> fileName.getFileName()
                                .toString()
                                .contains(c.getImageName()))
                        .filter(
                            (c) -> !(fileName.getFileName()
                                .toString()
                                .toLowerCase()
                                .endsWith("tmp")
                                || fileName.getFileName()
                                    .toString()
                                    .toLowerCase()
                                    .endsWith("new")))
                        .map((c) -> fileName)
                        .map((c) -> this.recordFile(c))
                        .forEach((c) -> KafkaConsumerService.LOGGER.info("End of action on {}", fileName));
                }
                key.reset();
            }
        } catch (IOException e) {
            KafkaConsumerService.LOGGER.error("ERROR when listenForDirectoryEvents {}", e.getMessage());
        }
    }

    private Object recordFile(java.nio.file.Path fileName) {
        KafkaConsumerService.LOGGER.info("Update done on file {}", fileName);

        this.lock.lock();
        try {
            Long time = this.filesWithTheirTimeStamp.get(fileName);
            if (time != null) {
                List<java.nio.file.Path> listOfFileAtSpecifiedTime = this.timeStampOfFiles.get(time);
                if (listOfFileAtSpecifiedTime != null) {
                    listOfFileAtSpecifiedTime.remove(fileName);
                    if (listOfFileAtSpecifiedTime.isEmpty()) {
                        this.timeStampOfFiles.remove(time);
                    }
                }
            }
            final long lastModified = fileName.toFile()
                .lastModified();
            this.filesWithTheirTimeStamp.put(fileName, lastModified);
            List<java.nio.file.Path> listOfFileAtSpecifiedTime = this.timeStampOfFiles
                .computeIfAbsent(lastModified, (k) -> new LinkedList<>());
            listOfFileAtSpecifiedTime.add(fileName);
            KafkaConsumerService.LOGGER
                .info("At time {}, files are {} ", lastModified, this.timeStampOfFiles.get(lastModified));

        } finally {
            this.lock.unlock();
        }
        return fileName;
    }

    protected boolean sendForUpdate(String x) {
        KafkaConsumerService.LOGGER.info("Sending update on {}", x);
        this.doSend(x);
        return true;
    }

    protected ListenableFuture<SendResult<String, String>> doSend(String x) {
        return this.kafkaTemplateForTopicUpdate.send(this.topicUpdate, this.computeEmettorId(), x);
    }

    @PostConstruct
    public void init() throws IOException {

        try (
            Stream<java.nio.file.Path> stream = Files.walk(Paths.get(this.folder))) {
            stream.filter(file -> Files.isDirectory(file))
                .filter(
                    (x) -> x.toAbsolutePath()
                        .toString()
                        .contains(this.whatchedSubDirFolderPattern))
                .peek((c) -> KafkaConsumerService.LOGGER.info("Detected folder {}", c))
                .forEach((x) -> this.beanTaskExecutor.execute(() -> this.listenForDirectoryEvents(x)));
            this.beanTaskExecutor.execute(() -> this.processMap());
        } catch (Exception e) {
            KafkaConsumerService.LOGGER.error("ERROR when init {}", ExceptionUtils.getStackTrace(e));
        }

    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((t, o) -> callback.seekToBeginning(t.topic(), t.partition()));
    }
}