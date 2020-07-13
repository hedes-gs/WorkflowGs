package com.gs.photo.workflow.dao;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

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

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        HbaseAlbumDAO.class, HbaseKeywordsDAO.class, HbaseImagesOfAlbumDAO.class, HbaseImagesOfKeywordsDAO.class,
        HbaseApplicationConfig.class, HbaseImagesOfRatingsDAO.class, HbaseRatingsDAO.class })
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
    protected HbaseAlbumDAO             hbaseAlbumDAO;

    @Autowired
    protected HbaseKeywordsDAO          hbaseKeywordsDAO;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    private static boolean init = true;

    @Before
    public void setUp() throws Exception {
        if (TestKeywordsAndAlbum.init) {
            TestKeywordsAndAlbum.LOGGER.info("... init");
            this.hbaseImagesOfAlbumDAO.truncate();
            this.hbaseImagesOfKeyWordsDAO.truncate();
            this.hbaseImagesOfRatingsDAO.truncate();
            TestKeywordsAndAlbum.init = false;
        }
    }

    @Test
    public void test001_countAllShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test001");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfAlbumDAO.flush();
        Assert.assertEquals(2, this.hbaseAlbumDAO.countAll());
        this.hbaseImagesOfAlbumDAO.truncate();
    }

    @Test
    public void test002_countAllOfAlbumShouldReturn1WhenOneHbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test002");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfAlbumDAO.flush();
        Assert.assertEquals(1, this.hbaseAlbumDAO.countAll("album1"));
        this.hbaseImagesOfAlbumDAO.truncate();
    }

    @Test
    public void test003_countAllOfAlbumShouldReturn2When2HbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test003");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfAlbumDAO.flush();
        Assert.assertEquals(2, this.hbaseAlbumDAO.countAll("album1"));
        this.hbaseImagesOfAlbumDAO.truncate();
    }

    @Test
    public void test004_countAllKeyWordsShouldReturn2WhenOneHbaseImageThumbnailIsRecordedWith2Keywords()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test004");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfKeyWordsDAO.flush();
        Assert.assertEquals(2, this.hbaseKeywordsDAO.countAll());
        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test005_countAllOfKeywordsShouldReturn1WhenOneHbaseImageThumbnailIsRecordedWith2Keyword()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test005");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.truncate();
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfKeyWordsDAO.flush();
        Assert.assertEquals(1, this.hbaseKeywordsDAO.countAll("keyword2"));
        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test006_countAllOfAlbumShouldReturn2When2HbaseImageThumbnailIsRecordedWith2albums() throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test006");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfKeyWordsDAO.flush();
        Assert.assertEquals(2, this.hbaseKeywordsDAO.countAll("keyword2"));
        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    @Test
    public void test007_getAllOfAlbumShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2albums()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test007");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 2);
        this.hbaseImagesOfAlbumDAO.updateMetadata(hbi2, (HbaseImageThumbnail) null);
        this.hbaseImagesOfAlbumDAO.flush();
        List<HbaseImagesOfAlbum> images = this.hbaseImagesOfAlbumDAO.getAllImagesOfMetadata("album1");
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers
                .containsInAnyOrder(this.toHbaseImageOfalbum(hbi2, "album1"), this.toHbaseImageOfalbum(hbi, "album1")));
        this.hbaseImagesOfAlbumDAO.truncate();
    }

    @Test
    public void test008_getAllOfKeywordShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2albums()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test008");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi2, (HbaseImageThumbnail) null);
        this.hbaseImagesOfKeyWordsDAO.flush();
        List<HbaseImagesOfKeywords> images = this.hbaseImagesOfKeyWordsDAO.getAllImagesOfMetadata("keyword1");
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers.containsInAnyOrder(
                this.toHbaseImageOfKeyword(hbi2, "keyword1"),
                this.toHbaseImageOfKeyword(hbi, "keyword1")));
        this.hbaseImagesOfAlbumDAO.truncate();
    }

    @Test
    public void test009_getAllOfRatings5ShouldReturn2ElementsWhen2HbaseImageThumbnailIsRecordedWith2RatingsOfFive()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test009");

        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("123456", 120L, (short) 2);
        this.hbaseImagesOfRatingsDAO.updateMetadata(hbi, hbi);
        HbaseImageThumbnail hbi2 = this.buildVersionHbaseImageThumbnail("123567", 120L, (short) 2);

        this.hbaseImagesOfRatingsDAO.updateMetadata(hbi2, hbi);
        this.hbaseImagesOfRatingsDAO.flush();
        List<HbaseImagesOfRatings> images = this.hbaseImagesOfRatingsDAO.getAllImagesOfMetadata(5);
        Assert.assertEquals(2, images.size());
        Assert.assertThat(
            images,
            Matchers.containsInAnyOrder(this.toHbaseImageOfRatings(hbi2), this.toHbaseImageOfRatings(hbi)));
        this.hbaseImagesOfRatingsDAO.truncate();
    }

    @Test
    public void test010_getAllOfKeywordsShouldReturnKw1AndKwd2When2HbaseImageThumbnailIsRecordedWith2Keywords()
        throws Throwable {
        TestKeywordsAndAlbum.LOGGER.info("... test010");
        HbaseImageThumbnail hbi = this.buildVersionHbaseImageThumbnail("1234", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        hbi = this.buildVersionHbaseImageThumbnail("1235", 120L, (short) 2);
        this.hbaseImagesOfKeyWordsDAO.updateMetadata(hbi, (HbaseImageThumbnail) null);
        this.hbaseImagesOfKeyWordsDAO.flush();
        final List<HbaseKeywords> all = this.hbaseKeywordsDAO.getAll();
        Assert.assertEquals(2, all.size());
        Assert.assertThat(
            all.stream()
                .map((t) -> t.getKeyword())
                .collect(Collectors.toList()),
            Matchers.containsInAnyOrder("keyword1", "keyword2"));
        this.hbaseImagesOfRatingsDAO.truncate();

        this.hbaseImagesOfKeyWordsDAO.truncate();
    }

    private HbaseImagesOfKeywords toHbaseImageOfKeyword(HbaseImageThumbnail hbi, String kw) {

        return HbaseImagesOfKeywords.builder()
            .withKeyword(kw)
            .withCreationDate(hbi.getCreationDate())
            .withHeight(hbi.getHeight())
            .withImageId(hbi.getImageId())
            .withImageName(hbi.getImageName())
            .withImportDate(hbi.getImportDate())
            .withOrientation(hbi.getOrientation())
            .withOriginalHeight(hbi.getOriginalHeight())
            .withOriginalWidth(hbi.getOriginalWidth())
            .withPath(hbi.getPath())
            .withThumbnail(hbi.getThumbnail())
            .withThumbName(hbi.getThumbName())
            .withVersion(hbi.getVersion())
            .withWidth(hbi.getWidth())
            .build();
    }

    private HbaseImagesOfAlbum toHbaseImageOfalbum(HbaseImageThumbnail hbi, String album) {
        return HbaseImagesOfAlbum.builder()
            .withAlbumName(album)
            .withCreationDate(hbi.getCreationDate())
            .withHeight(hbi.getHeight())
            .withImageId(hbi.getImageId())
            .withImageName(hbi.getImageName())
            .withImportDate(hbi.getImportDate())
            .withOrientation(hbi.getOrientation())
            .withOriginalHeight(hbi.getOriginalHeight())
            .withOriginalWidth(hbi.getOriginalWidth())
            .withPath(hbi.getPath())
            .withThumbnail(hbi.getThumbnail())
            .withThumbName(hbi.getThumbName())
            .withVersion(hbi.getVersion())
            .withWidth(hbi.getWidth())
            .build();
    }

    private HbaseImagesOfRatings toHbaseImageOfRatings(HbaseImageThumbnail hbi) {
        return HbaseImagesOfRatings.builder()
            .withRatings(5)
            .withCreationDate(hbi.getCreationDate())
            .withHeight(hbi.getHeight())
            .withImageId(hbi.getImageId())
            .withImageName(hbi.getImageName())
            .withImportDate(hbi.getImportDate())
            .withOrientation(hbi.getOrientation())
            .withOriginalHeight(hbi.getOriginalHeight())
            .withOriginalWidth(hbi.getOriginalWidth())
            .withPath(hbi.getPath())
            .withThumbnail(hbi.getThumbnail())
            .withThumbName(hbi.getThumbName())
            .withVersion(hbi.getVersion())
            .withWidth(hbi.getWidth())
            .build();
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
            .withRatings(5)
            .build();
        return hbaseData;
    }
}
