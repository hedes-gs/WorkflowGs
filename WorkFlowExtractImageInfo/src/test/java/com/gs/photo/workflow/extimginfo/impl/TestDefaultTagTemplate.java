package com.gs.photo.workflow.extimginfo.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.exif.ExifServiceImpl;
import com.gs.photo.common.workflow.ports.IIgniteCacheFactory;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.workflow.extimginfo.ApplicationConfig;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.IOUtils;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyII;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyMM;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;
import com.workflow.model.files.FileToProcess;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ExifServiceImpl.class, ApplicationConfig.class })
public class TestDefaultTagTemplate {
    public static final int                   STREAM_HEAD = 0x00;
    private Logger                            LOGGER      = LogManager.getLogger(TestDefaultTagTemplate.class);

    protected FileChannelDataInput            fcdi;

    @MockBean
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    @Qualifier("consumerForTopicWithFileToProcessValue")
    @MockBean
    protected Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue;

    @Autowired
    @Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
    @MockBean
    protected Producer<String, Object>        producerForTransactionPublishingOnExifOrImageTopic;

    @MockBean
    protected IIgniteCacheFactory             igniteCacheFactory;

    @Autowired
    @MockBean
    protected IIgniteDAO                      iIgniteDAO;

    @Autowired
    protected ExifServiceImpl                 exifService;

    @Autowired
    protected IFileMetadataExtractor          beanFileMetadataExtractor;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        Path filePath = new File("src/test/resources/A900.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(2 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
    }

    @Test
    public void testConvertTagValueToTag() {}

    @Test
    public void testCreateSimpleTiffFields() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs(this.iIgniteDAO.get("1"), null)
            .get();
        IFD.tiffFieldsAsStream(allIfds.stream())
            .forEach((tif) -> {
                this.LOGGER.info(tif);
                if ((tif.getTiffField()
                    .getTagValue() & 0xffff) == 0xb027) {
                    this.LOGGER.info(
                        "-----> {} ",
                        new String(this.exifService.getSonyLens(
                            (int[]) tif.getTiffField()
                                .getData())));
                }
            });
    }

    private Collection<TiffField<?>> getAllTiffFields(Collection<IFD> allIfds) {
        final Collection<TiffField<?>> retValue = new ArrayList<>();
        allIfds.forEach((ifd) -> {
            retValue.addAll(ifd.getFields());
            retValue.addAll(this.getAllTiffFields(ifd.getAllChildren()));
        });
        return retValue;
    }

    private int readHeader(FileChannelDataInput rin) throws IOException {
        int offset = 0;
        rin.position(TestDefaultTagTemplate.STREAM_HEAD);
        short endian = rin.readShort();
        offset += 2;

        if (endian == IOUtils.BIG_ENDIAN) {
            rin.setReadStrategy(ReadStrategyMM.getInstance());
        } else if (endian == IOUtils.LITTLE_ENDIAN) {
            rin.setReadStrategy(ReadStrategyII.getInstance());
        } else {
            throw new RuntimeException("Invalid TIFF byte order");
        }
        rin.position(offset);
        short tiff_id = rin.readShort();
        offset += 2;

        if (tiff_id != 0x2a) { // "*" 42 decimal
            throw new RuntimeException("Invalid TIFF identifier");
        }

        rin.position(offset);
        offset = rin.readInt();

        return offset;
    }

}
