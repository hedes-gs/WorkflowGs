package com.gs.photo.workflow.extimginfo.impl;

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

import com.gs.photo.common.workflow.IIgniteDAO;
import com.gs.photo.common.workflow.exif.ExifServiceImpl;
import com.gs.photo.workflow.extimginfo.ApplicationConfig;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.impl.BeanFileMetadataExtractor;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffTag;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        ExifServiceImpl.class, NameServiceTestConfiguration.class, BeanFileMetadataExtractor.class,
        ApplicationConfig.class })
public class TestBeanFileReadIFDs {
    private Logger                   LOGGER                     = LogManager.getLogger(TestBeanFileReadIFDs.class);

    public static final int          JOIN_WINDOW_TIME           = 86400;
    public static final short        EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short        EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short        EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short        EXIF_ORIENTATION           = (short) 0x0112;

    public static final short        EXIF_LENS                  = (short) 0xA434;
    public static final short        EXIF_FOCAL_LENS            = (short) 0x920A;
    public static final short        EXIF_SHIFT_EXPO            = (short) 0x9204;
    public static final short        EXIF_SPEED_ISO             = (short) 0x8827;
    public static final short        EXIF_APERTURE              = (short) 0x829D;
    public static final short        EXIF_SPEED                 = (short) 0x829A;
    public static final short        EXIF_COPYRIGHT             = (short) 0x8298;
    public static final short        EXIF_ARTIST                = (short) 0x13B;
    public static final short        EXIF_CAMERA_MODEL          = (short) 0x110;

    public static final short[]      EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

    public static final short[]      EXIF_LENS_PATH             = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_FOCAL_LENS_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_SHIFT_EXPO_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_SPEED_ISO_PATH        = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_APERTURE_PATH         = { (short) 0, (short) 0x8769 };
    public static final short[]      EXIF_SPEED_PATH            = { (short) 0, (short) 0x8769 };

    public static final short[]      EXIF_COPYRIGHT_PATH        = { (short) 0 };
    public static final short[]      EXIF_ARTIST_PATH           = { (short) 0 };
    public static final short[]      EXIF_CAMERA_MODEL_PATH     = { (short) 0 };
    public static final short[]      EXIF_ORIENTATION_PATH      = { (short) 0 };

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
        ByteBuffer bb = ByteBuffer.allocate(2 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
    }

    @Test
    public void shouldGet115TiffFieldWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1")
            .get();
        Assert.assertEquals(
            115,
            IFD.tiffFieldsAsStream(allIfds.stream())
                .count());
        Assert.assertEquals(115, IFD.getNbOfTiffFields(allIfds));
    }

    @Test
    public void shouldPrint90TiffFieldWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1")
            .get();
        IFD.tiffFieldsAsStream(allIfds.stream())
            .forEach((tif) -> this.LOGGER.info(tif));
    }

    @Test
    public void shouldFilter13TiffFieldsWhenSonyARWIsAnA7IIInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1")
            .get();
        IFD.tiffFieldsAsStream(allIfds.stream())
            .filter((t) -> this.isARecordedField(t))
            .forEach((tif) -> this.LOGGER.info(tif));
    }

    private boolean isARecordedField(TiffFieldAndPath t) {
        short currentTag = t.getTiffField()
            .getTag()
            .getValue();
        return ((t.getTiffField()
            .getTag()
            .getValue() == TestBeanFileReadIFDs.EXIF_CREATION_DATE_ID)
            && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_CREATION_DATE_ID_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_SIZE_WIDTH)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_WIDTH_HEIGHT_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_SIZE_HEIGHT)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_WIDTH_HEIGHT_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_ORIENTATION)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_ORIENTATION_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_LENS)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_LENS_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_FOCAL_LENS)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_FOCAL_LENS_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_SHIFT_EXPO)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_SHIFT_EXPO_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_SPEED_ISO)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_SPEED_ISO_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_APERTURE)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_APERTURE_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_SPEED)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_SPEED_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_COPYRIGHT)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_COPYRIGHT_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_ARTIST)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_ARTIST_PATH)))

            || ((currentTag == TestBeanFileReadIFDs.EXIF_CAMERA_MODEL)
                && (Objects.deepEquals(t.getPath(), TestBeanFileReadIFDs.EXIF_CAMERA_MODEL_PATH)));

    }

    @Test
    public void shouldGet2JpgFilesWhenSonyARWIsAnInputFile() {
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1")
            .get();
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
        Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1")
            .get();

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
