package com.gs.photos.workflow.metadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.ApplicationConfig;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.exif.ExifServiceImpl;
import com.gs.photo.workflow.impl.BeanFileMetadataExtractor;
import com.gs.photos.workflow.metadata.tiff.TiffField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        ExifServiceImpl.class, NameServiceTestConfiguration.class, BeanFileMetadataExtractor.class,
        ApplicationConfig.class })
public class TestBeanFileReadIFDs {
    private Logger                   LOGGER = LogManager.getLogger(TestBeanFileReadIFDs.class);

    @Autowired
    IIgniteDAO                       iIgniteDAO;

    @Autowired
    protected IFileMetadataExtractor beanFileMetadataExtractor;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
    }

    @Test
    public void shouldGet115TiffFieldWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");
        Assert.assertEquals(
            115,
            IFD.tiffFieldsAsStream(allIfds.stream())
                .count());
        Assert.assertEquals(115, IFD.getNbOfTiffFields(allIfds));
    }

    @Test
    public void shouldPrint90TiffFieldWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");
        IFD.tiffFieldsAsStream(allIfds.stream())
            .forEach((tif) -> this.LOGGER.info(tif));
    }

    @Test
    public void shouldGet2JpgFilesWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");
        Assert.assertEquals(
            2,
            IFD.ifdsAsStream(allIfds)
                .filter((ifd) -> ifd.imageIsPresent())
                .count());
        IFD.ifdsAsStream(allIfds)
            .filter((ifd) -> ifd.imageIsPresent())
            .map((ifd) -> ifd.getJpegImage())
            .forEach((img) -> {
                LocalDateTime currentTime = LocalDateTime.now();
                try (
                    FileOutputStream stream = new FileOutputStream(UUID.randomUUID() + "-" + currentTime.toString()
                        .replaceAll("\\:", "_") + ".jpg")) {
                    stream.write(img);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
    }

    @Test
    public void shouldGet2DateTimeExifAtPath0x2A_0x8769‬_WhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");

        long count = IFD.tiffFieldsAsStream(allIfds.stream())
            .filter(
                (t) -> (t.getTiffField()
                    .getTag()
                    .getValue() == (short) 0x9003)
                    && Objects.deepEquals(t.getPath(), new short[] { 0, (short) 0x8769 }))
            .count();
        Assert.assertEquals(1, count);

    }

    protected String buildIfdString(String path, IFD ifd, TiffField<?> t) {
        if (t.getTag()
            .getValue() == TiffTag.DATE_TIME_ORIGINAL.getValue()) {
            this.LOGGER.info("found " + t + "at path " + path + " ");
        }

        return path + ifd.toString() + "/" + t.toString();

    }

}
