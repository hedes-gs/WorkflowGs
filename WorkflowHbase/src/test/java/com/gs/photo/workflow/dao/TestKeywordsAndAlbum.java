package com.gs.photo.workflow.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Connection;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.gs.photo.workflow.recinhbase.ApplicationConfig;
import com.gs.photo.workflow.recinhbase.dao.HbaseAlbumDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImageThumbnailDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfAlbumDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfKeywordsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfPersonsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseImagesOfRatingsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseKeywordsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbasePersonsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseRatingsDAO;
import com.gs.photo.workflow.recinhbase.dao.HbaseStatsDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfAlbumDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfKeyWordsDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfPersonsDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfRatingsDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.HbaseKeywords;
import com.workflow.model.SizeAndJpegContent;

@SpringBootTest(classes = {
        HbaseAlbumDAO.class, HbaseKeywordsDAO.class, HbaseImagesOfAlbumDAO.class, HbaseImagesOfKeywordsDAO.class,
        ApplicationConfig.class, HbaseImagesOfRatingsDAO.class, HbaseRatingsDAO.class,
        HbaseImagesOfPersonsDAO.class, HbasePersonsDAO.class, HbaseImageThumbnailDAO.class, HbaseStatsDAO.class })
public class TestKeywordsAndAlbum {
    protected static Logger             LOGGER = LoggerFactory.getLogger(TestKeywordsAndAlbum.class);

    @Autowired
    protected IHbaseImagesOfAlbumDAO    hbaseImagesOfAlbumDAO;

    @Autowired
    protected IHbaseImagesOfKeyWordsDAO hbaseImagesOfKeyWordsDAO;

    @Autowired
    protected IHbaseImagesOfRatingsDAO  hbaseImagesOfRatingsDAO;

    @Autowired
    protected IHbaseImagesOfPersonsDAO  hbaseImagesOfPersonsDAO;

    @Autowired
    protected HbaseAlbumDAO             hbaseAlbumDAO;

    @Autowired
    protected HbaseKeywordsDAO          hbaseKeywordsDAO;

    @Autowired
    protected HbaseImageThumbnailDAO    hbaseImageThumbnailDAO;

    @Autowired
    protected HbaseStatsDAO             HbaseStatsDAO;

    @Autowired
    protected Connection                connection;

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {}

    private static boolean init = true;

    @BeforeEach
    public void setUp() throws Exception { this.hbaseImageThumbnailDAO.truncate(); }

    @Test
    public void test001_countAllShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test001");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2");
        Assertions.assertEquals(2, this.hbaseAlbumDAO.countAll());
        Assertions.assertEquals(1, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));
        Assertions.assertEquals(1, (int) this.hbaseImagesOfAlbumDAO.countAll("album2"));

    }

    @Test
    public void test002_countAllOfAlbumShouldReturn1WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test002");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2");
        Assertions.assertEquals(1, (int) this.hbaseAlbumDAO.countAll("album1"));
        Assertions.assertEquals(1, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));

    }

    @Test
    public void test003_countAllOfAlbumShouldReturn2When2HbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test003");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.truncate();
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        Assertions.assertEquals(2, (int) this.hbaseAlbumDAO.countAll("album1"));
    }

    @Test
    public void test004_countAllKeyWordsShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2Keywords()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test004");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        Assertions.assertEquals(2, this.hbaseKeywordsDAO.countAll());
        Assertions.assertEquals(1, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));
        Assertions.assertEquals(1, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword2"));

        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test005_countAllOfKeywordsShouldReturn1WhenOneHbaseImageThumbnailIsRecordedWith2Keyword()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test005");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        Assertions.assertEquals(1, (int) this.hbaseKeywordsDAO.countAll("keyword2"));
        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test006_countAllOfAlbumShouldReturn2When2HbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test006");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        Assertions.assertEquals(2, (int) this.hbaseKeywordsDAO.countAll("keyword2"));
        Assertions.assertEquals(2, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword2"));

        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test007_getAllOfAlbumShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2albums()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test007");
        this.hbaseImagesOfAlbumDAO.truncate();

        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1_1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2_1");
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi2, "album1_1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi2, "album2_1");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1_1");
        TestKeywordsAndAlbum.LOGGER.info("Found images {}", images);
        Assertions.assertEquals(2, images.size());
        MatcherAssert.assertThat(
            images,
            Matchers.containsInAnyOrder(
                this.toHbaseImageOfalbum(hbi2, "album1_1", "album2_1"),
                this.toHbaseImageOfalbum(hbi, "album1_1", "album2_1")));
    }

    @Test
    public void test008_getAllOfKeywordShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2albums()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test008");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi2, "keyword1");
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi2, "keyword2");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(2, images.size());
        MatcherAssert.assertThat(
            images,
            Matchers.containsInAnyOrder(
                this.toHbaseImageOfKeyword(hbi2, "keyword1", "keyword2"),
                this.toHbaseImageOfKeyword(hbi, "keyword1", "keyword2")));
    }

    @Test
    public void test009_getAllOfRatings5ShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2RatingsOfFive()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test009");

        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 1);
        this.hbaseImagesOfRatingsDAO.truncate();
        this.hbaseImagesOfRatingsDAO.addMetaData(hbi, 5L);
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 1);
        this.hbaseImagesOfRatingsDAO.addMetaData(hbi2, 5L);
        List<HbaseImageThumbnail> images = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5L);
        Assertions.assertEquals(2, images.size());
        MatcherAssert.assertThat(images, Matchers.containsInAnyOrder(hbi2, hbi));
        Assertions.assertEquals(2, this.hbaseImagesOfRatingsDAO.countAll(5l));
    }

    @Test
    public void test010_getAllOfKeywordsShouldReturnKw1AndKwd2When2HbaseImageThumbnailIsRecordedWith2Keywords()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test010");
        this.hbaseImagesOfKeyWordsDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        final List<HbaseKeywords> all = this.hbaseKeywordsDAO.getAll();
        Assertions.assertEquals(2, all.size());
        MatcherAssert.assertThat(
            all.stream()
                .map((t) -> t.getKeyword())
                .collect(Collectors.toList()),
            Matchers.containsInAnyOrder("keyword1", "keyword2"));
        Assertions.assertEquals(1, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));
        Assertions.assertEquals(1, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword2"));

    }

    @Test
    public void test011_getNoKeywordWhenAddingThenDeletingKeyWord() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test011");
        this.hbaseImagesOfKeyWordsDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234" + "", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(1, images.size());
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(0, images.size());
    }

    @Test
    public void test012_getNoKeywordWhenAddingThenDeletingKeyWordTwice() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test012");
        this.hbaseImagesOfKeyWordsDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(1, images.size());
        Assertions.assertEquals(1, this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(0, images.size());
        Assertions.assertEquals(0, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));

        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(1, images.size());
        Assertions.assertEquals(1, this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assertions.assertEquals(0, images.size());
        Assertions.assertEquals(0, (int) this.hbaseImagesOfKeyWordsDAO.countAll("keyword1"));

    }

    @Test
    public void test013_getNoAlbumWhenAddingThenDeletingAlbum() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test013");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(1, images.size());
        Assertions.assertEquals(1, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(0, images.size());
        Assertions.assertEquals(0, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));

    }

    @Test
    public void test014_getNoAlbumWhenAddingThenDeletingAlbumTwice() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test014");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(1, images.size());
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(0, images.size());

        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(1, images.size());
        Assertions.assertEquals(1, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assertions.assertEquals(0, images.size());
        Assertions.assertEquals(0, (int) this.hbaseImagesOfAlbumDAO.countAll("album1"));

    }

    @Test
    public void test015_getAllOfRatings5ShouldReturn0ElementsWhen2HbaseImageThumbnailIsRecordedWith2RatingsOfFiveThenDeleted()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test015");

        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 1);
        this.hbaseImagesOfRatingsDAO.truncate();
        this.hbaseImagesOfRatingsDAO.addMetaData(hbi, 5L);
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 1);
        this.hbaseImagesOfRatingsDAO.addMetaData(hbi2, 5L);
        List<HbaseImageThumbnail> images = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5L);
        Assertions.assertEquals(2, images.size());
        Assertions.assertEquals(2, this.hbaseImagesOfRatingsDAO.countAll(5l));

        this.hbaseImagesOfRatingsDAO.deleteMetaData(hbi, 5L);
        List<HbaseImageThumbnail> imagesAfterDelete = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5L);
        Assertions.assertEquals(1, imagesAfterDelete.size());
        Assertions.assertEquals(1, (int) this.hbaseImagesOfRatingsDAO.countAll(5l));
        MatcherAssert.assertThat(imagesAfterDelete, Matchers.containsInAnyOrder(hbi2));

        this.hbaseImagesOfRatingsDAO.deleteMetaData(hbi2, 5L);
        imagesAfterDelete = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5L);
        Assertions.assertEquals(0, imagesAfterDelete.size());
        Assertions.assertEquals(0, (int) this.hbaseImagesOfRatingsDAO.countAll(5l));

    }

    @Test
    public void test016_get1005AlbumWhenAddingBulkOf1005AlbumsWithAPageOf1000() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test016");
        this.hbaseImagesOfAlbumDAO.truncate();
        Collection<HbaseImageThumbnail> hbaseDatas = new ArrayList<>();
        for (int k = 0; k < 1005; k++) {
            HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234_" + k, 120L, (short) 1);
            hbi.getAlbums()
                .add("Album1");
            hbaseDatas.add(hbi);
        }
        this.hbaseImageThumbnailDAO.put(hbaseDatas);
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("Album1");
        Assertions.assertEquals(1005, images.size());
    }

    @Test
    public void test017_get999AlbumWhenAddingBulkOf1005AlbumsWithAPageOf1000AndRemove6Albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test017");
        this.hbaseImagesOfAlbumDAO.truncate();
        List<HbaseImageThumbnail> hbaseDatas = new ArrayList<>();
        for (int k = 0; k < 1005; k++) {
            HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234_" + k, 120L, (short) 1);
            hbi.getAlbums()
                .add("Album1");
            hbaseDatas.add(hbi);
        }
        this.hbaseImageThumbnailDAO.put(hbaseDatas);
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("Album1");
        Assertions.assertEquals(1005, images.size());
        for (int k = 0; k < 6; k++) {
            this.hbaseImagesOfAlbumDAO.deleteMetaData(hbaseDatas.get(k), "Album1");
        }
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("Album1");
        Assertions.assertEquals(999, images.size());
    }

    @Test
    public void test018_get1001AlbumWhenAddingBulkOf1000AlbumsWithAPageOf1000AndAddOneAlbumAtTheBeginning()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test018");
        this.hbaseImagesOfAlbumDAO.truncate();
        List<HbaseImageThumbnail> hbaseDatas = new ArrayList<>();
        for (int k = 1; k <= 1000; k++) {
            HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234_" + k, 120L, (short) 1);
            hbi.getAlbums()
                .add("Album1");
            hbaseDatas.add(hbi);
        }
        this.hbaseImageThumbnailDAO.put(hbaseDatas);
        List<HbaseImageThumbnail> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("Album1");
        Assertions.assertEquals(1000, images.size());
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234_0", 0L, (short) 1);
        hbi.getAlbums()
            .add("Album1");
        this.hbaseImageThumbnailDAO.put(hbi);
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("Album1");
        Assertions.assertEquals(1001, images.size());
    }

    private HbaseImageThumbnail toHbaseImageOfKeyword(HbaseImageThumbnail hbi, String... kw) {
        hbi.getKeyWords()
            .addAll(Arrays.asList(kw));
        return hbi;
    }

    private HbaseImageThumbnail toHbaseImageOfalbum(HbaseImageThumbnail hbi, String... album) {
        hbi.getAlbums()
            .addAll(Arrays.asList(album));
        return hbi;
    }

    private HbaseImagesOfRatings toHbaseImageOfRatings(HbaseImageThumbnail hbi) {
        return HbaseImagesOfRatings.builder()
            .withThumbNailImage(hbi)
            .withRatings(5L)
            .build();
    }

    protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(String id, long creationDate, short v) {
        HashSet<String> albums = new HashSet<>();
        HashSet<String> keywords = new HashSet<>();
        HashSet<Long> ratings = new HashSet<>();
        HashMap<Integer, SizeAndJpegContent> map = new HashMap<>();
        map.put(
            (int) v,
            SizeAndJpegContent.builder()
                .withJpegContent(new byte[] { 0, 1, 2, 3, 4 })
                .withHeight(1024)
                .withWidth(768)
                .build());

        HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
            .withPersons(new HashSet<>())
            .withRegionSalt((short) 0x10)
            .withCreationDate(creationDate)
            .withImageId(id)
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
            .withRatings(ratings)
            .build();
        return hbaseData;
    }
}
