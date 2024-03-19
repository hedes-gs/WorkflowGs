package com.gs.photos.ws.services;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gs.photos.ws.repositories.IHbaseImageThumbnailDAO;
import com.workflow.model.dtos.ImageDto;

@Service
public class HFileServices implements IHFileServices {

    @Autowired
    protected FileSystem              hdfsFileSystem;

    @Autowired
    protected IHbaseImageThumbnailDAO hbaseImageThumbnailDAO;
    @Value("${wf.hdfs.rootPath}")
    protected String                  rootPath;
    protected static Logger           LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Override
    public void delete(ImageDto imageToDelete) {
        String importName = Strings.isEmpty(imageToDelete.getImportName()) ? "DEFAULT_IMPORT"
            : imageToDelete.getImportName();
        String key = imageToDelete.getImageId();

        final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));

        try {
            HFileServices.LOGGER.info("Starting to delete file {}", folderWhereRecord);
            if (this.hdfsFileSystem.exists(folderWhereRecord)) {
                HFileServices.LOGGER.info("Deleting file {}", folderWhereRecord);
                this.hdfsFileSystem.delete(folderWhereRecord, true);
            } else {
                HFileServices.LOGGER.warn("Unable to delete file {}", folderWhereRecord);
            }
        } catch (Exception e) {
            KafkaConsumerService.LOGGER
                .error("Error when deleting {} : {} ", folderWhereRecord, ExceptionUtils.getStackTrace(e));
        }
        KafkaConsumerService.LOGGER.info("End of deleting hdfs file {} ", folderWhereRecord);
    }

}
