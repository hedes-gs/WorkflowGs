package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.WorkflowHbaseApplication;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.events.WfEvents;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = WorkflowHbaseApplication.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestImageGenericDao {

    @Autowired
    protected HbaseImageThumbnailDAO     hbaseImageThumbnailDAO;

    @MockBean
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents> producerForPublishingWfEvents;

    @Before
    public void init() { MockitoAnnotations.initMocks(this); }

    @After
    public void clean() {}

    @Test
    public void test001_shouldRecordInHbaseWithKey1ABCDEFVersion1() throws IOException {
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
    }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(short v) {
        HashSet<String> albums = new HashSet<>(Arrays.asList("album1", "album2"));
        HashSet<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));

        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion(v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(new byte[] { 0, 1, 2, 3, 4 })
            .withThumbName("Thumbnail_1.jpg")
            .withHeight(1024)
            .withWidth(768)
            .withAlbums(albums)
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("Alpha9")
            .withCopyright("gs")
            .withFocalLens(new int[] { 2, 3 })
            .withImportName("Mon import")
            .withIsoSpeed((short) 2)
            .withKeyWords(keywords)
            .withLens(new byte[] { 3, 4 })
            .withShiftExpo(new int[] { 4, 5 })
            .withSpeed(new int[] { 6, 7 })
            .build();
        return hbaseData;
    }

    @Test
    public void test002_shouldRecordInHbaseWithKey1ABCDEFVersion2() throws IOException {
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 2);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();
    }

    @Test
    public void test003_shouldHbaseDataVersion2EqualsToRecordedHbaseData() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 2)
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertEquals(this.buildVersionHbaseImageThumbnail((short) 2), hbaseData);
    }

    @Test
    public void test004_shouldHbaseDataVersion1EqualsToRecordedHbaseData() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 1)
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertEquals(this.buildVersionHbaseImageThumbnail((short) 1), hbaseData);
    }

    @Test
    public void test005_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndVersion1() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 1)
            .build();
        this.hbaseImageThumbnailDAO.delete(hbaseData);
    }

    @Test
    public void test006_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndVersion1() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 1)
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertNull(hbaseData);
    }

    @Test
    public void test005_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndVersion2() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 2)
            .build();
        this.hbaseImageThumbnailDAO.delete(hbaseData);
    }

    @Test
    public void test006_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndVersion2() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF")
            .withVersion((short) 2)
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertNull(hbaseData);
    }

    @Test
    public void test014_shouldRecordBulkOf1000Data() throws IOException {
        List<HbaseImageThumbnail> data = new ArrayList<>(1000);
        for (int k = 0; k < 10000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);
        this.hbaseImageThumbnailDAO.flush();
    }

    @Test
    public void test015_shouldReturn1000DataAfterBulkRecord() throws IOException {
        HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
        int nbOfDataFromHbase = 0;
        for (int k = 0; k < data.length; k++) {
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withCreationDate(1)
                .withImageId("ABCDEF")
                .withVersion((short) k)
                .build();
            hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
            Assert.assertNotNull(hbaseData);
            if (hbaseData != null) {
                nbOfDataFromHbase++;
            }
        }
        Assert.assertEquals(1000, nbOfDataFromHbase);

    }

    @Test
    public void test016_shouldDelete1000DataAfterBulkDelete() throws IOException {
        HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
        for (int k = 0; k < data.length; k++) {
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withCreationDate(1)
                .withImageId("ABCDEF")
                .withVersion((short) k)
                .build();
            data[k] = hbaseData;
        }
        this.hbaseImageThumbnailDAO.delete(data);
    }

    @Test
    public void test017_shouldReturn0DataAfterBulkDelete() throws IOException {
        HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
        int nbOfDataFromHbase = 0;
        for (int k = 0; k < data.length; k++) {
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withCreationDate(1)
                .withImageId("ABCDEF")
                .withVersion((short) k)
                .build();
            hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);

            if (hbaseData != null) {
                nbOfDataFromHbase++;
            }
            Assert.assertNull(hbaseData);
        }
        Assert.assertEquals(0, nbOfDataFromHbase);

    }

    @Test
    public void test018_shouldReturn1RecordWhenUsingFilter() throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail(
            LocalDateTime.now()
                .toInstant(ZoneOffset.ofTotalSeconds(0))
                .toEpochMilli(),
            (short) 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();

        List<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getThumbNailsByDate(
            LocalDateTime.now()
                .minusDays(2),
            LocalDateTime.now()
                .plusDays(2),
            0,
            0);
        Assert.assertEquals(1, scanValue.size());
        this.hbaseImageThumbnailDAO.delete(hbaseData);
        this.hbaseImageThumbnailDAO.flush();

    }

    @Test
    public void test019_shouldReturn1000WhenCountIsCalledAnd1000DataRecorded() throws Throwable {

        this.hbaseImageThumbnailDAO.truncate();
        List<HbaseImageThumbnail> data = new ArrayList<>(10000);
        for (int k = 0; k < 1000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);

        int count = this.hbaseImageThumbnailDAO.count();
        Assert.assertEquals(1000, count);
        this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    public void test020_shouldReturn1RecordWhenUsingFilterNextRow() throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        final long epochMilli = LocalDateTime.now()
            .toInstant(ZoneOffset.ofTotalSeconds(0))
            .toEpochMilli();
        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("0123456", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }

        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }
        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("9ABCDEF", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }

        this.hbaseImageThumbnailDAO.flush();
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) 1);
        List<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getNextThumbNailOf(hbaseData);
        Assert.assertEquals(1, scanValue.size());
        Assert.assertEquals(
            "9ABCDEF",
            scanValue.get(0)
                .getImageId()
                .trim());
        Assert.assertEquals(
            1,
            scanValue.get(0)
                .getVersion());

        this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    public void test020_shouldReturn1RecordWhenUsingFilterPreviousRow() throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        final long epochMilli = LocalDateTime.now()
            .toInstant(ZoneOffset.ofTotalSeconds(0))
            .toEpochMilli();
        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("0123456", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }

        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }
        for (int k = 0; k < 10; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("9ABCDEF", epochMilli, (short) k);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }

        this.hbaseImageThumbnailDAO.flush();

        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) 1);
        List<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getPreviousThumbNailOf(hbaseData);
        Assert.assertEquals(1, scanValue.size());
        Assert.assertEquals(
            "0123456",
            scanValue.get(0)
                .getImageId()
                .trim());
        Assert.assertEquals(
            1,
            scanValue.get(0)
                .getVersion());
        this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    public void test021_shouldReturnCorrectsKeyWordsWhenRecordsAreAssociated() throws IOException {
        try {
            Set<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));
            this.hbaseImageThumbnailDAO.truncate();
            final long epochMilli = LocalDateTime.now()
                .toInstant(ZoneOffset.ofTotalSeconds(0))
                .toEpochMilli();
            HbaseImageThumbnail hbaseDataRecordedData = this
                .buildVersionHbaseImageThumbnail("0123456", epochMilli, (short) 1);
            this.hbaseImageThumbnailDAO.put(hbaseDataRecordedData);
            this.hbaseImageThumbnailDAO.flush();

            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("0123456", epochMilli, (short) 1);

            HbaseImageThumbnail scanValue = this.hbaseImageThumbnailDAO.get(hbaseData);
            Assert.assertNotNull(scanValue);
            Assert.assertEquals(keywords, scanValue.getKeyWords());
        } finally {
            this.hbaseImageThumbnailDAO.truncate();
        }
    }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(long creationDate, short v) {
        HashSet<String> albums = new HashSet<>(Arrays.asList("album1", "album2"));
        HashSet<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(creationDate)
            .withImageId("ABCDEF")
            .withVersion(v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(new byte[] { 0, 1, 2, 3, 4 })
            .withThumbName("Thumbnail_1.jpg")
            .withHeight(1024)
            .withWidth(768)
            .withOriginalHeight(5000)
            .withOriginalHeight(7000)
            .withAlbums(albums)
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("Alpha9")
            .withCopyright("gs")
            .withFocalLens(new int[] { 2, 3 })
            .withImportName("Mon import")
            .withIsoSpeed((short) 2)
            .withKeyWords(keywords)
            .withLens(new byte[] { 3, 4 })
            .withShiftExpo(new int[] { 4, 5 })
            .withSpeed(new int[] { 6, 7 })
            .build();

        return hbaseData;
    }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(String id, long creationDate, short v) {
        HashSet<String> albums = new HashSet<>(Arrays.asList("album1", "album2"));
        HashSet<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(creationDate)
            .withImageId(id)
            .withVersion(v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(new byte[] { 0, 1, 2, 3, 4 })
            .withThumbName("Thumbnail_1.jpg")
            .withHeight(1024)
            .withWidth(768)
            .withOriginalHeight(5000)
            .withOriginalHeight(7000)
            .withAlbums(albums)
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("Alpha9")
            .withCopyright("gs")
            .withFocalLens(new int[] { 2, 3 })
            .withImportName("Mon import")
            .withIsoSpeed((short) 2)
            .withKeyWords(keywords)
            .withLens(new byte[] { 3, 4 })
            .withShiftExpo(new int[] { 4, 5 })
            .withSpeed(new int[] { 6, 7 })
            .build();
        return hbaseData;
    }

}
