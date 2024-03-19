package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.workflow.recinhbase.WorkflowHbaseApplication;
import com.gs.photo.workflow.recinhbase.dao.HbaseImageThumbnailDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfAlbumDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfKeywordsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.SizeAndJpegContent;
import com.workflow.model.events.WfEvents;

import reactor.core.publisher.Flux;

@SpringBootTest(classes = WorkflowHbaseApplication.class)
@TestMethodOrder(OrderAnnotation.class)
public class TestImageGenericDao {

    protected static Logger              LOGGER                              = LoggerFactory
        .getLogger(TestImageGenericDao.class);
    protected static final long          PAGE_SIZE                           = 1000L;
    protected static final String        TABLE_SOURCE                        = "image_thumbnail";
    protected static final String        TABLE_PAGE                          = "page_image_thumbnail";
    protected static final byte[]        TABLE_PAGE_DESC_COLUMN_FAMILY       = "max_min".getBytes();
    protected static final byte[]        TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER = "max".getBytes();
    protected static final byte[]        TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER = "min".getBytes();
    protected static final byte[]        TABLE_PAGE_LIST_COLUMN_FAMILY       = "list".getBytes();
    protected static final byte[]        TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes();
    protected static final byte[]        TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements".getBytes();

    @Autowired
    protected Connection                 connection;

    @Autowired
    protected HbaseImageThumbnailDAO     hbaseImageThumbnailDAO;

    @Autowired
    protected HbaseImagesOfAlbumDAO      hbaseAlbumDAO;

    @Autowired
    protected HbaseImagesOfKeywordsDAO   hbaseKeywordsDAO;

    @Autowired
    protected HbaseStatsDAO              hbaseStatsDAO;

    @MockBean
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents> producerForPublishingWfEvents;

    @BeforeEach
    public void init() { MockitoAnnotations.initMocks(this); }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(short v, int creationDate, int version) {
        HashSet<String> albums = new HashSet<>(Arrays.asList("album1", "album2"));
        HashSet<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));
        HashSet<String> persons = new HashSet<>();
        HashSet<Long> ratings = new HashSet<>();

        HashMap<Integer, SizeAndJpegContent> map = new HashMap<>();
        map.put(
            version,
            SizeAndJpegContent.builder()
                .withJpegContent(new byte[] { 0, 1, 2, 3, 4 })
                .withHeight(1024)
                .withWidth(768)
                .build());

        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withRegionSalt((short) 0x10)
            .withCreationDate(creationDate)
            .withImageId("ABCDEF_" + v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(map)
            .withThumbName("Thumbnail_1.jpg")
            .withHeight(1024)
            .withWidth(768)
            .withAlbums(albums)
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("Alpha9")
            .withCopyright("gs")
            .withFocalLens(new int[] { 2, 3 })
            .withImportName(new HashSet<>(Collections.singleton("Mon import")))
            .withIsoSpeed((short) 2)
            .withKeyWords(keywords)
            .withPersons(persons)
            .withLens(new byte[] { 3, 4 })
            .withShiftExpo(new int[] { 4, 5 })
            .withSpeed(new int[] { 6, 7 })
            .withRatings(ratings)
            .build();
        return hbaseData;
    }

    @Test
    @Order(1)
    public void test001_shouldRecordInHbaseWithKey1ABCDEFVersion1() throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        this.hbaseAlbumDAO.truncate();
        this.hbaseKeywordsDAO.truncate();
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 1, 1, 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
    }

    @Test
    @Order(2)
    public void test002_shouldRecordInHbaseWithKey1ABCDEFVersion2() throws IOException {
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 3, 1, 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        hbaseData = this.buildVersionHbaseImageThumbnail((short) 4, 1, 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();

        HbaseImageThumbnail hbaseDataGet = HbaseImageThumbnail.builder()
            .withRegionSalt((short) 0x10)
            .withCreationDate(1)
            .withImageId("ABCDEF_3")
            .build();

        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseDataGet);
        Assertions.assertEquals(this.buildVersionHbaseImageThumbnail((short) 3, 1, 1), hbaseData);

        hbaseDataGet = HbaseImageThumbnail.builder()
            .withRegionSalt((short) 0x10)
            .withCreationDate(1)
            .withImageId("ABCDEF_4")
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseDataGet);
        Assertions.assertEquals(this.buildVersionHbaseImageThumbnail((short) 4, 1, 1), hbaseData);
    }

    @Test
    @Order(3)
    public void test005_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEF1() throws IOException {
        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withRegionSalt((short) 0x10)
            .withCreationDate(1)
            .withImageId("ABCDEF_1")
            .build();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assertions.assertNotNull(hbaseData);
        hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(1)
            .withImageId("ABCDEF_1")
            .build();
        this.hbaseImageThumbnailDAO.delete(hbaseData);
        this.hbaseImageThumbnailDAO.flush();
        hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assertions.assertNull(hbaseData);
    }

    @Test
    @Order(4)
    public void test014_shouldRecordBulkOf1000Data() throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        this.preparePageTable();
        List<HbaseImageThumbnail> data = new ArrayList<>(1000);
        for (int k = 0; k < 1000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, 1, 1);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);
        this.hbaseImageThumbnailDAO.flush();
    }

    @Test
    @Order(5)
    public void test015_shouldReturn1000DataAfterBulkRecord() throws IOException {
        int nbOfDataFromHbase = 0;
        for (int k = 0; k < 1000; k++) {
            HashMap<Integer, byte[]> map = new HashMap<>();
            map.put(1, new byte[] { 0, 1, 2, 3, 4 });
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withRegionSalt((short) 0x10)
                .withCreationDate(1)
                .withImageId("ABCDEF_" + k)
                .build();
            hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
            Assertions.assertNotNull(hbaseData);
            if (hbaseData != null) {
                nbOfDataFromHbase++;
            }
        }
        Assertions.assertEquals(1000, nbOfDataFromHbase);

    }

    @Test
    @Order(6)
    public void test016_shouldDelete1000DataAfterBulkDelete() throws IOException {
        HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
        for (int k = 0; k < data.length; k++) {
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withRegionSalt((short) 0x10)
                .withCreationDate(1)
                .withImageId("ABCDEF_" + k)
                .build();
            data[k] = hbaseData;
        }
        this.hbaseImageThumbnailDAO.delete(data);
    }

    @Test
    @Order(7)
    public void test017_shouldReturn0DataAfterBulkDelete() throws IOException {
        HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
        int nbOfDataFromHbase = 0;
        for (int k = 0; k < data.length; k++) {
            HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                .withRegionSalt((short) 0x10)
                .withCreationDate(1)
                .withImageId("ABCDEF_" + k)
                .build();
            hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);

            if (hbaseData != null) {
                nbOfDataFromHbase++;
            }
            Assertions.assertNull(hbaseData);
        }
        Assertions.assertEquals(0, nbOfDataFromHbase);

    }

    @Test
    @Order(8)
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
        Assertions.assertEquals(1, scanValue.size());
        this.hbaseImageThumbnailDAO.delete(hbaseData);
        this.hbaseImageThumbnailDAO.flush();

    }

    @Test
    @Order(9)
    public void test019_shouldReturn1000WhenCountIsCalledAnd1000DataRecorded() throws Throwable {

        this.hbaseImageThumbnailDAO.truncate();
        List<HbaseImageThumbnail> data = new ArrayList<>(10000);
        for (int k = 0; k < 1000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, 1, 1);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);

        long count = this.hbaseImageThumbnailDAO.count();
        Assertions.assertEquals(1000, count);
        // this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    @Order(10)
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
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) 2);
        List<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getNextThumbNailOf(hbaseData);
        Assertions.assertEquals(1, scanValue.size());
        Assertions.assertEquals(
            "12345_3",
            scanValue.get(0)
                .getImageId()
                .trim());
        // this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    @Order(11)
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

        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail("12345", epochMilli, (short) 2);
        Flux<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getPreviousThumbNailsOf(hbaseData, false);
        HbaseImageThumbnail retValue = scanValue.sort((a, b) -> HbaseImageThumbnail.compareForSorting(b, a))
            .blockFirst();

        Assertions.assertNotNull(retValue);
        Assertions.assertEquals(
            "12345_1",
            retValue.getImageId()
                .trim());
    }

    @Test
    @Order(12)
    public void test021_shouldReturnCorrectsKeyWordsWhenRecordsAreAssociated() throws IOException {
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
        Assertions.assertNotNull(scanValue);
        Assertions.assertEquals(keywords, scanValue.getKeyWords());
    }

    @Test
    @Order(13)
    public void test023_shouldGetMaxElementToPageSizeMinElementTo0MaxElementToPageSizeMinusOneWhen1000ImagesAreRecorded()
        throws IOException {
        this.hbaseImageThumbnailDAO.truncate();
        final TableName pageTable = this.preparePageTable();
        for (int k = 0; k < 1000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, k, 1);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }
        this.hbaseImageThumbnailDAO.flush();
        try (
            Table table = this.connection.getTable(pageTable)) {
            Get get = new Get(TestImageGenericDao.convert(0L));
            Result res1 = table.get(get);
            long minValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER));
            long maxValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER));
            long nbOfElements = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS));
            Assertions.assertEquals(1000, nbOfElements);
            Assertions.assertEquals(0, minValue);
            Assertions.assertEquals(999, maxValue);
        }
    }

    @Test
    @Order(14)
    public void test024_shouldGet2PagesWhenRecordMoreThanPageSizeImages() throws Throwable {
        final TableName pageTable = this.preparePageTable();
        for (int k = 10; k < 1010; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, k, 1);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }
        this.hbaseImageThumbnailDAO.flush();
        HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 3, 3, 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        hbaseData = this.buildVersionHbaseImageThumbnail((short) 1, 1, 1);
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();

        try (
            AggregationClient ac = new AggregationClient(HBaseConfiguration.create())) {
            long nbOfElements = ac.rowCount(pageTable, new LongColumnInterpreter(), new Scan());
            Assertions.assertEquals(2, nbOfElements);
        }
        try (
            Table table = this.connection.getTable(pageTable)) {
            Get get = new Get(TestImageGenericDao.convert(0L));
            Result res1 = table.get(get);
            long nbOfElements = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS));
            long minValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER));
            long maxValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER));
            Assertions.assertEquals(1000, nbOfElements);
            Assertions.assertEquals(1, minValue);
            Assertions.assertEquals(1007, maxValue);
            Assertions.assertEquals(
                1000,
                res1.getFamilyMap(TestImageGenericDao.TABLE_PAGE_LIST_COLUMN_FAMILY)
                    .size());
        }

        try (
            Table table = this.connection.getTable(pageTable)) {
            Get get = new Get(TestImageGenericDao.convert(1L));
            Result res1 = table.get(get);
            long nbOfElements = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS));
            long minValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER));
            long maxValue = Bytes.toLong(
                res1.getValue(
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_FAMILY,
                    TestImageGenericDao.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER));
            Assertions.assertEquals(2, nbOfElements);
            Assertions.assertEquals(1008, minValue);
            Assertions.assertEquals(1009, maxValue);
        }

    }

    protected TableName preparePageTable() throws IOException {
        final TableName pageTable = TableName.valueOf("test:page_image_thumbnail");
        try (
            Admin admin = this.connection.getAdmin()) {
            if (admin.tableExists(pageTable)) {
                admin.disableTable(pageTable);
                admin.deleteTable(pageTable);
            }
            this.createPageTableIfNeeded(admin, "test:page_image_thumbnail");
        }
        return pageTable;
    }

    @Test
    @Order(15)
    public void test025_shouldCheckImagesKey() throws Throwable {
        final TableName keyTable = TableName.valueOf("test:image_thumbnail_key");
        this.preparePageTable();
        this.hbaseStatsDAO.truncate();
        this.hbaseImageThumbnailDAO.truncate();
        for (int k = 1; k < 5; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, 1000 * k, 1);
            this.hbaseImageThumbnailDAO.put(hbaseData);
        }
        this.hbaseImageThumbnailDAO.flush();

        this.hbaseStatsDAO.getAll()
            .forEach((k, v) -> TestImageGenericDao.LOGGER.info(" --> key {}, value {} ", k, v));
    }

    @Test
    @Order(16)
    public void test026_shouldReturnOneAlbumPageOf1000ElemnsWhen1000DataRecorded() throws Throwable {

        this.hbaseImageThumbnailDAO.truncate();
        this.hbaseAlbumDAO.truncate();
        this.hbaseKeywordsDAO.truncate();
        List<HbaseImageThumbnail> data = new ArrayList<>(10000);
        for (int k = 0; k < 1000; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, 1, 1);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);
        long count = this.hbaseImageThumbnailDAO.count();
        Assertions.assertEquals(1000, count);

        Assertions.assertEquals(
            1000,
            this.hbaseAlbumDAO.getAllImagesOfMetadata("album1")
                .size());
        Assertions.assertEquals(
            1000,
            this.hbaseAlbumDAO.getAllImagesOfMetadata("album2")
                .size());

        Assertions.assertEquals(
            1000,
            this.hbaseKeywordsDAO.getAllImagesOfMetadata("keyword1")
                .size());
        Assertions.assertEquals(
            1000,
            this.hbaseKeywordsDAO.getAllImagesOfMetadata("keyword2")
                .size());

    }

    @Test
    @Order(17)
    public void test027_shouldReturnTwoAlbumPageElemnsWhen1005DataRecorded() throws Throwable {

        this.hbaseImageThumbnailDAO.truncate();
        this.hbaseAlbumDAO.truncate();
        this.hbaseKeywordsDAO.truncate();
        List<HbaseImageThumbnail> data = new ArrayList<>(10000);
        for (int k = 0; k < 1005; k++) {
            HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k, 1, 1);
            data.add(hbaseData);
        }
        this.hbaseImageThumbnailDAO.put(data);
        long count = this.hbaseImageThumbnailDAO.count();
        Assertions.assertEquals(1005, count);

        Assertions.assertEquals(
            1005,
            this.hbaseAlbumDAO.getAllImagesOfMetadata("album1")
                .size());
        Assertions.assertEquals(
            1005,
            this.hbaseAlbumDAO.getAllImagesOfMetadata("album2")
                .size());

        Assertions.assertEquals(4, this.hbaseAlbumDAO.countNbOfPages());
        Assertions.assertEquals(
            1005,
            this.hbaseKeywordsDAO.getAllImagesOfMetadata("keyword1")
                .size());
        Assertions.assertEquals(
            1005,
            this.hbaseKeywordsDAO.getAllImagesOfMetadata("keyword2")
                .size());
        Assertions.assertEquals(4, this.hbaseKeywordsDAO.countNbOfPages());

    }

    protected void incPageSize(Table table, long pageNumber) throws IOException {
        Increment inc = new Increment(TestImageGenericDao.convert(pageNumber)).addColumn(
            TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            TestImageGenericDao.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            1);
        table.increment(inc);
    }

    private static byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(long creationDate, short v) {
        HashSet<String> albums = new HashSet<>(Arrays.asList("album1", "album2"));
        HashSet<String> keywords = new HashSet<>(Arrays.asList("keyword1", "keyword2"));

        HashMap<Integer, SizeAndJpegContent> map = new HashMap<>();
        map.put(
            (int) v,
            SizeAndJpegContent.builder()
                .withJpegContent(new byte[] { 0, 1, 2, 3, 4 })
                .withHeight(1024)
                .withWidth(768)
                .build());

        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(creationDate)
            .withImageId("ABCDEF_" + v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(map)
            .withThumbName("Thumbnail_1.jpg")
            .withOriginalHeight(5000)
            .withOriginalHeight(7000)
            .withAlbums(albums)
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("Alpha9")
            .withCopyright("gs")
            .withFocalLens(new int[] { 2, 3 })
            .withImportName(new HashSet<>(Collections.singleton("Mon import")))
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

        HashMap<Integer, SizeAndJpegContent> map = new HashMap<>();
        map.put(
            (int) v,
            SizeAndJpegContent.builder()
                .withJpegContent(new byte[] { 0, 1, 2, 3, 4 })
                .withHeight(1024)
                .withWidth(768)
                .build());

        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withCreationDate(creationDate)
            .withImageId(id + '_' + v)
            .withImageName("Mon Image")
            .withPath("Mon path")
            .withThumbnail(map)
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
            .withImportName(new HashSet<>(Collections.singleton("Mon import")))
            .withIsoSpeed((short) 2)
            .withKeyWords(keywords)
            .withLens(new byte[] { 3, 4 })
            .withShiftExpo(new int[] { 4, 5 })
            .withSpeed(new int[] { 6, 7 })
            .build();
        return hbaseData;
    }

    protected TableName createPageTableIfNeeded(final Admin admin, String tableName) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (!admin.tableExists(hbaseTable)) {

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hbaseTable);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY));
            try {
                admin.createTable(builder.build());
                try (
                    Table table = this.connection.getTable(hbaseTable)) {

                    byte[] key = TestImageGenericDao.convert(0L);
                    Put put = new Put(key)
                        .addColumn(
                            AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            AbstractDAO.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                            TestImageGenericDao.convert(0l))
                        .addColumn(
                            AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractDAO.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                            TestImageGenericDao.convert(0L))
                        .addColumn(
                            AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractDAO.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                            TestImageGenericDao.convert(Long.MAX_VALUE));
                    table.put(put);
                } catch (Exception e) {
                    TestImageGenericDao.LOGGER.warn(
                        "Error when creating table {}, table already created {} ",
                        tableName,
                        ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            } catch (TableExistsException e) {
                TestImageGenericDao.LOGGER.warn(
                    "Error when creating table {}, table already created {} ",
                    tableName,
                    ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                TestImageGenericDao.LOGGER
                    .warn("Error when creating table {} : {} ", tableName, ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return hbaseTable;
    }

}
