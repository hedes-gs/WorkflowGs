package com.gs.photo.workflow.dao;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Connection;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.HbaseApplicationConfig;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfAlbum;
import com.workflow.model.HbaseImagesOfKeywords;
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.HbaseKeywords;
import com.workflow.model.SizeAndJpegContent;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        HbaseAlbumDAO.class, HbaseKeywordsDAO.class, HbaseImagesOfAlbumDAO.class, HbaseImagesOfKeywordsDAO.class,
        HbaseApplicationConfig.class, HbaseImagesOfRatingsDAO.class, HbaseRatingsDAO.class,
        HbaseImagesOfPersonsDAO.class, HbasePersonsDAO.class, HbaseImageThumbnailDAO.class, HbaseStatsDAO.class })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

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

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    private static boolean init = true;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void test001_countAllShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test001");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2");
        Assert.assertEquals(2, this.hbaseAlbumDAO.countAll());
    }

    @Test
    public void test002_countAllOfAlbumShouldReturn1WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test002");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2");
        Assert.assertEquals(1, this.hbaseAlbumDAO.countAll("album1"));
    }

    @Test
    public void test003_countAllOfAlbumShouldReturn2When2HbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test003");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.truncate();
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        Assert.assertEquals(2, this.hbaseAlbumDAO.countAll("album1"));
    }

    @Test
    public void test004_countAllKeyWordsShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2Keywords()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test004");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword2");
        Assert.assertEquals(2, this.hbaseKeywordsDAO.countAll());
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
        Assert.assertEquals(1, this.hbaseKeywordsDAO.countAll("keyword2"));
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
        Assert.assertEquals(2, this.hbaseKeywordsDAO.countAll("keyword2"));
        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test007_getAllOfAlbumShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2albums()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test007");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album2");
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi2, "album1");
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi2, "album2");
        List<HbaseImagesOfAlbum> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers
                .containsInAnyOrder(this.toHbaseImageOfalbum(hbi2, "album1"), this.toHbaseImageOfalbum(hbi, "album1")));
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
        List<HbaseImagesOfKeywords> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers.containsInAnyOrder(
                this.toHbaseImageOfKeyword(hbi2, "keyword1"),
                this.toHbaseImageOfKeyword(hbi, "keyword1")));
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
        List<HbaseImagesOfRatings> images = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5L);
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers.containsInAnyOrder(this.toHbaseImageOfRatings(hbi2), this.toHbaseImageOfRatings(hbi)));
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
        Assert.assertEquals(2, all.size());
        Assert.assertThat(
            all.stream()
                .map((t) -> t.getKeyword())
                .collect(Collectors.toList()),
            Matchers.containsInAnyOrder("keyword1", "keyword2"));
    }

    @Test
    public void test011_getNoKeywordWhenAddingThenDeletingKeyWord() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test011");
        this.hbaseImagesOfKeyWordsDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        List<HbaseImagesOfKeywords> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(0, images.size());
    }

    @Test
    public void test012_getNoKeywordWhenAddingThenDeletingKeyWordTwice() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test012");
        this.hbaseImagesOfKeyWordsDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        List<HbaseImagesOfKeywords> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(0, images.size());

        this.hbaseImagesOfKeyWordsDAO.addMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfKeyWordsDAO.deleteMetaData(hbi, "keyword1");
        images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(0, images.size());
    }

    @Test
    public void test013_getNoAlbumWhenAddingThenDeletingAlbum() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test013");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        List<HbaseImagesOfAlbum> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(0, images.size());
    }

    @Test
    public void test014_getNoAlbumWhenAddingThenDeletingAlbumTwice() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test014");
        this.hbaseImagesOfAlbumDAO.truncate();
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 1);
        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        List<HbaseImagesOfAlbum> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(0, images.size());

        this.hbaseImagesOfAlbumDAO.addMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(1, images.size());
        this.hbaseImagesOfAlbumDAO.deleteMetaData(hbi, "album1");
        images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(0, images.size());
    }

    private HbaseImagesOfKeywords toHbaseImageOfKeyword(HbaseImageThumbnail hbi, String kw) {
        return HbaseImagesOfKeywords.builder()
            .withThumbNailImage(hbi)
            .withKeyword(kw)
            .build();
    }

    private HbaseImagesOfAlbum toHbaseImageOfalbum(HbaseImageThumbnail hbi, String album) {
        return HbaseImagesOfAlbum.builder()
            .withThumbNailImage(hbi)
            .withAlbumName(album)
            .build();
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
            .withImportName("Mon import")
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
