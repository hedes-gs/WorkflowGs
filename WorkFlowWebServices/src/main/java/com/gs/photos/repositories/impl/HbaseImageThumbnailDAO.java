package com.gs.photos.repositories.impl;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.cache2k.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.DateTimeHelper;
import com.gs.photo.workflow.exif.FieldType;
import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.gs.photos.repositories.IHbaseImageThumbnailDAO;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset.TypeValue;
import com.gsphotos.worflow.hbasefilters.FilterRowFindNextRowWithTwoFields;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.ModelConstants;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.ImageVersionDto;

@Component
public class HbaseImageThumbnailDAO extends GenericDAO<HbaseImageThumbnail> implements IHbaseImageThumbnailDAO {
    private static Logger                   LOGGER                   = LoggerFactory
        .getLogger(HbaseImageThumbnailDAO.class);
    private static final byte[]             FAMILY_THB_BYTES         = "thb".getBytes();
    private static final byte[]             FAMILY_SZ_BYTES          = "sz".getBytes();
    private static final byte[]             FAMILY_IMG_BYTES         = "img".getBytes();
    private static final byte[]             FAMILY_TECH_BYTES        = "tech".getBytes();
    private static final byte[]             FAMILY_META_BYTES        = "meta".getBytes();
    private static final byte[]             FAMILY_ALBUMS_BYTES      = "albums".getBytes();
    private static final byte[]             FAMILY_KEYWORDS_BYTES    = "keywords".getBytes();

    private static final byte[]             HEIGHT_BYTES             = "height".getBytes();
    private static final byte[]             WIDTH_BYTES              = "width".getBytes();
    private static final byte[]             ORIGINAL_HEIGHT_BYTES    = "originalHeight".getBytes();
    private static final byte[]             ORIGINAL_WIDTH_BYTES     = "originalWidth".getBytes();
    private static final byte[]             PATH_BYTES               = "path".getBytes();
    private static final byte[]             TUMB_NAME_BYTES          = "thumb_name".getBytes();
    private static final byte[]             IMAGE_NAME_BYTES         = "image_name".getBytes();
    private static final byte[]             TUMBNAIL_BYTES           = "thumbnail".getBytes();
    private static final byte[]             ORIENTATION_BYTES        = "orientation".getBytes();

    static protected ExecutorService        executorService          = Executors.newFixedThreadPool(64);

    @Value("${cache.jpegimages.name}")
    protected String                        cacheMainJpegImagesName;
    @Value("${cache.images.name}")
    protected String                        cacheImagesName;
    @Value("${cache.jpegimages.version.name}")
    protected String                        cacheJpegImagesVersionName;

    @Autowired
    protected CacheManager                  cacheManager;

    @Autowired
    protected IExifService                  exifService;

    @Autowired
    protected IHbaseImagesOfRatingsDAO      hbaseImagesOfRatingsDAO;

    @Autowired
    protected IHbaseImagesOfKeywordsDAO     hbaseImagesOfKeywordsDAO;

    protected Map<Integer, PageDescription> currentDefaultLoadedPage = new TreeMap<>();

    protected static class PageDescription {
        protected int                       hbasePageNumber;
        protected List<HbaseImageThumbnail> pageContent;

        public int getPageSize() { return this.pageContent.size(); }

        public PageDescription(int hbasePageNumber) {
            super();
            this.hbasePageNumber = hbasePageNumber;
            this.pageContent = new ArrayList<>();
        }

        public void add(HbaseImageThumbnail instance) { this.pageContent.add(instance); }

    }

    private final static int compareForSorting(HbaseImageThumbnail o1, HbaseImageThumbnail o2) {
        return Comparator.comparing(HbaseImageThumbnail::getCreationDate)
            .thenComparing(HbaseImageThumbnail::getImageId)
            .thenComparingInt(HbaseImageThumbnail::getVersion)
            .compare(o1, o2);
    }

    @Override
    public List<ImageDto> findLastImages(int pageSize, int pageNumber) {
        List<RegionInfo> regions = null;
        int countAll = 0;
        try {
            try (
                Admin admin = this.connection.getAdmin()) {
                regions = admin.getRegions(
                    this.getHbaseDataInformation()
                        .getTable());
            }
            countAll = super.countWithCoprocessorJob(this.getHbaseDataInformation());
            HbaseImageThumbnailDAO.LOGGER.info("Total nb of elements {} ", countAll);
        } catch (Throwable e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        final int pageSize2 = (pageSize % regions.size()) == 0 ? pageSize / regions.size()
            : (int) Math.round((pageSize / regions.size()) + 1);
        if (pageNumber > 0) {
            List<HbaseImageThumbnail> retValue = new ArrayList<>();
            try (
                Table table = this.connection.getTable(
                    this.getHbaseDataInformation()
                        .getTable())) {
                int totalLoadedElements = 0;
                int currentPageNumber = 0;
                int firstRow = pageSize * (pageNumber - 1);
                int currentIndex = 0;
                int initialIndex = 0;
                int startPage = -1;
                boolean startPageIsFound = false;
                while ((totalLoadedElements < Math.min(countAll, pageSize * pageNumber))) {
                    totalLoadedElements = totalLoadedElements
                        + this.loadPages(table, currentPageNumber, pageSize2, regions);
                    PageDescription pageDesc = this.currentDefaultLoadedPage.get(currentPageNumber);
                    if (((currentIndex + pageDesc.getPageSize()) >= firstRow) && !startPageIsFound) {
                        startPage = currentPageNumber;
                        initialIndex = firstRow - currentIndex;
                        startPageIsFound = true;
                        HbaseImageThumbnailDAO.LOGGER.info(
                            "Loaded {}, found startPage {}, firstRow is {}",
                            totalLoadedElements,
                            startPage,
                            firstRow);
                    }
                    currentIndex = currentIndex + pageDesc.getPageSize();
                    currentPageNumber++;
                    HbaseImageThumbnailDAO.LOGGER
                        .info("Loaded {} where countAll is {}  ", totalLoadedElements, countAll);
                }
                PageDescription pageDesc = this.currentDefaultLoadedPage.get(startPage);
                int lastIndex = Math.min(pageDesc.getPageSize(), initialIndex + (pageNumber * pageSize));
                for (int k = initialIndex; k < lastIndex; k++) {
                    retValue.add(pageDesc.pageContent.get(k));
                }
                do {
                    pageDesc = this.currentDefaultLoadedPage.get(startPage++);
                    if (pageDesc != null) {
                        for (int k = 0; (retValue.size() < pageSize) && (k < pageDesc.getPageSize()); k++) {
                            retValue.add(pageDesc.pageContent.get(k));
                        }
                    }
                } while ((retValue.size() < pageSize) && (pageDesc != null));

            } catch (IOException e) {
                HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
            final List<ImageDto> sortedRetValue = retValue.stream()
                .sorted((o1, o2) -> o2.compareTo(o1))
                .map((h) -> this.toImageDTO(h))
                .collect(Collectors.toList());
            sortedRetValue.forEach(
                (e) -> HbaseImageThumbnailDAO.LOGGER.info(
                    "Returning {} in page {}",
                    e.getData()
                        .getImageId(),
                    pageNumber));
            return sortedRetValue;
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    private int loadPages(Table table, int currentPageNumber, int pageSize, List<RegionInfo> regions) {
        final HbaseImageThumbnail lastOfPrevious = this.getLastRowOfPreviousPage(currentPageNumber, regions);
        PageDescription pageDescription = this.currentDefaultLoadedPage
            .computeIfAbsent(currentPageNumber, (c) -> this.createPageDescription(table, c, pageSize, lastOfPrevious));
        if (pageDescription.getPageSize() == 0) {
            lastOfPrevious.setVersion((short) (1000));
            pageDescription = this.createPageDescription(table, currentPageNumber, pageSize, lastOfPrevious);
            if (pageDescription.getPageSize() == 0) { throw new RuntimeException("Unable to cross a region..."); }
            this.currentDefaultLoadedPage.put(currentPageNumber, pageDescription);
        }
        return pageDescription.getPageSize();
    }

    private HbaseImageThumbnail getLastRowOfPreviousPage(int currentPageNumber, List<RegionInfo> regions) {
        HbaseImageThumbnail lastOfPrevious = null;
        if (this.currentDefaultLoadedPage.containsKey(currentPageNumber - 1)) {
            PageDescription pageDescription = this.currentDefaultLoadedPage.get(currentPageNumber - 1);
            if (pageDescription.getPageSize() > 0) {
                lastOfPrevious = pageDescription.pageContent.get(0);
            }
        }
        return lastOfPrevious;
    }

    private PageDescription createPageDescription(
        Table table,
        Integer currentPageNumber,
        int pageSize,
        HbaseImageThumbnail lastOfPrevious
    ) {
        try {
            PageDescription retValue = new PageDescription(currentPageNumber);
            Scan scan = this.createScanToGetAllColumnsWithoutImages();
            scan.setReversed(true);
            scan.setCaching(10000);

            if (lastOfPrevious != null) {
                HbaseImageThumbnailDAO.LOGGER
                    .info("Last presious to  [{}] - in page {} ", lastOfPrevious, currentPageNumber);
                scan.withStartRow(this.getKey(lastOfPrevious), false);
                HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                    .withCreationDate(0)
                    .withImageId(" ")
                    .withVersion((short) 0)
                    .build();
                scan.withStopRow(this.getKey(hbaseData));
            } else {
                HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
                    .withCreationDate(Long.MAX_VALUE)
                    .withImageId(" ")
                    .withVersion((short) 0)
                    .build();
                scan.withStartRow(this.getKey(hbaseData));
                hbaseData = HbaseImageThumbnail.builder()
                    .withCreationDate(0)
                    .withImageId(" ")
                    .withVersion((short) 0)
                    .build();
                scan.withStopRow(this.getKey(hbaseData));
            }
            scan.setLimit(pageSize);
            scan.setFilter(
                new FilterList(new FilterRowByLongAtAGivenOffset(ModelConstants.FIXED_WIDTH_CREATION_DATE
                    + ModelConstants.FIXED_WIDTH_IMAGE_ID, 1L, 1L, FilterRowByLongAtAGivenOffset.TypeValue.USHORT)));
            try (
                ResultScanner rs = table.getScanner(scan)) {
                rs.forEach((t) -> {
                    HbaseImageThumbnail instance = new HbaseImageThumbnail();
                    this.hbaseDataInformation.build(instance, t);
                    retValue.add(instance);
                });
                retValue.pageContent = retValue.pageContent.stream()
                    .sorted((o1, o2) -> HbaseImageThumbnailDAO.compareForSorting(o1, o2))
                    .collect(Collectors.toList());
                return retValue;
            } finally {
                HbaseImageThumbnailDAO.LOGGER.info(
                    "Current page {} with page size {}, found {} ",
                    currentPageNumber,
                    pageSize,
                    retValue.getPageSize());
                retValue.pageContent.forEach(
                    (e) -> HbaseImageThumbnailDAO.LOGGER
                        .info("Current page {} with page size {}, found {} ", currentPageNumber, pageSize, e));

            }
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    protected void saveInCacheMainJpegImagesName(HbaseImageThumbnail instance) {
        if (this.isMainVersion(instance)) {
            byte[] jpegImageToCache = instance.getThumbnail();
            if (instance.getOrientation() == 8) {
                try {
                    BufferedImage bi = ImageIO.read(new ByteArrayInputStream(instance.getThumbnail()));
                    bi = HbaseImageThumbnailDAO.rotateAntiCw(bi);
                    ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
                    ImageIO.write(bi, "jpg", os);
                    jpegImageToCache = os.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            this.cacheManager.getCache(this.cacheMainJpegImagesName)
                .putIfAbsent(instance.getImageId(), jpegImageToCache);
        }
    }

    @Override
    public ImageDto findById(OffsetDateTime creationDate, String id, int version) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);
        ImageKeyDto.Builder builder = ImageKeyDto.builder();
        ImageKeyDto key = builder.withCreationDate(creationDate)
            .withImageId(id)
            .withVersion(version)
            .build();
        ImageDto imgDto = cache.computeIfAbsent(key, () -> this.getImageDto(key, creationDate, id, version));
        return imgDto;
    }

    @Override
    public Optional<ImageDto> getNextImageById(OffsetDateTime creationDate, String id, int version) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withVersion((short) version)
            .build();

        List<HbaseImageThumbnail> listOfNext = this.getNextThumbNailsOf(hbaseData);
        listOfNext.sort((o1, o2) -> HbaseImageThumbnailDAO.compareForSorting(o2, o1));
        HbaseImageThumbnailDAO.LOGGER
            .info("getNextImageById of [{},{},{}] : {} ", creationDate, id, version, listOfNext);
        return listOfNext.stream()
            .map((h) -> this.toImageDTO(h))
            .findFirst();

    }

    @Override
    public Optional<ImageDto> getPreviousImageById(OffsetDateTime creationDate, String id, int version) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withVersion((short) version)
            .build();
        List<HbaseImageThumbnail> listOfPrevious = this.getPreviousThumbNailsOf(hbaseData);
        listOfPrevious.sort((o1, o2) -> HbaseImageThumbnailDAO.compareForSorting(o1, o2));
        HbaseImageThumbnailDAO.LOGGER
            .info("getPreviousImageById of [{},{},{}] : {} ", creationDate, id, version, listOfPrevious);
        return listOfPrevious.stream()
            .map((h) -> this.toImageDTO(h))
            .findFirst();
    }

    @Override
    public byte[] findImageRawById(OffsetDateTime creationDate, String id, int version) {
        Cache<String, Map<Integer, ImageVersionDto>> nativeCache = this
            .getNativeCacheForJpegImagesVersion(this.cacheManager);

        if (!nativeCache.containsKey(id)) {
            HbaseImageThumbnailDAO.LOGGER
                .info("-> findImageRawById cache missed for [{},{},{}] ", creationDate, id, version);
        }

        byte[] retValue = nativeCache.computeIfAbsent(id, () -> {
            Map<Integer, ImageVersionDto> map = new HashMap<>();
            map.put(
                version,
                this.getImageVersionDto(creationDate, id, version)
                    .orElseThrow(() -> new IllegalArgumentException()));
            return map;
        })
            .computeIfAbsent(
                version,
                (t) -> this.getImageVersionDto(creationDate, id, version)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to find version for")))
            .getJpegContent();
        return retValue;
    }

    @Override
    public int count(OffsetDateTime firstDate, OffsetDateTime lastDate) { return 500; }

    @Override
    public int count() throws Throwable { return super.countWithCoprocessorJob(this.getHbaseDataInformation()); }

    @Override
    public List<ImageDto> getThumbNailsByDate(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) {
        final Cache<String, Map<Integer, ImageVersionDto>> nativeCache = this
            .getNativeCacheForJpegImagesVersion(this.cacheManager);
        HbaseImageThumbnailDAO.LOGGER.info(
            "getThumbNailsByDate start date is {}, end date is {} and page size is {}",
            firstDate,
            lastDate,
            page.getPageSize());
        List<ImageDto> retValue = new ArrayList<>();
        final long firstDateEpochMillis = DateTimeHelper.toEpochMillis(firstDate);
        final long mastDateEpochMilli = DateTimeHelper.toEpochMillis(lastDate);
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = this.createScanToGetAllColumns();
            FilterList fl = new FilterList(
                new FilterRowByLongAtAGivenOffset(0, firstDateEpochMillis, mastDateEpochMilli));
            if (versions != null) {
                for (short v : versions) {
                    if (v != 0) {
                        fl.addFilter(
                            new FilterRowByLongAtAGivenOffset(ModelConstants.FIXED_WIDTH_CREATION_DATE
                                + ModelConstants.FIXED_WIDTH_IMAGE_ID, v, v, TypeValue.USHORT));
                    }
                }
            }

            fl.addFilter(new PageFilter(page.getPageSize()));

            scan.setFilter(fl);
            HbaseImageThumbnail hbt = HbaseImageThumbnail.builder()
                .withCreationDate(firstDateEpochMillis)
                .withVersion((short) 0)
                .withImageId(new String(""))
                .build();
            byte[] keyStartValue = new byte[this.hbaseDataInformation.getKeyLength()];
            byte[] keyStopValue = new byte[this.hbaseDataInformation.getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            scan.withStartRow(keyStartValue);
            hbt = HbaseImageThumbnail.builder()
                .withCreationDate(mastDateEpochMilli)
                .withVersion((short) 0)
                .withImageId(new String(""))
                .build();
            this.hbaseDataInformation.buildKey(hbt, keyStopValue);
            scan.withStopRow(keyStopValue, true);
            scan.setCaching(10000);
            try (
                ResultScanner rs = table.getScanner(scan)) {
                rs.forEach((t) -> this.processResultOfScanner(nativeCache, retValue, t));
            }
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        HbaseImageThumbnailDAO.LOGGER
            .info("-> end of getThumbNailsByDate start date is {}, end date is {}", firstDate, lastDate);

        return retValue;
    }

    protected void processResultOfScanner(
        final Cache<String, Map<Integer, ImageVersionDto>> nativeCache,
        List<ImageDto> retValue,
        Result t
    ) {
        long initTime = System.currentTimeMillis();
        HbaseImageThumbnailDAO.LOGGER.info("-> Start {} ", Instant.now());
        final HbaseImageThumbnail instance = new HbaseImageThumbnail();

        this.hbaseDataInformation.build(instance, t);
        final ImageDto imageDTO = this.toImageDTO(instance);
        HbaseImageThumbnailDAO.LOGGER.info(
            "-> Found {} at date {}, epoch {} [duration build : {}]",
            imageDTO.getData()
                .getImageId(),
            imageDTO.getCreationDateAsString(),
            imageDTO.getData()
                .getCreationDate(),
            (System.currentTimeMillis() - initTime) / 1000.0f);
        retValue.add(imageDTO);

        HbaseImageThumbnailDAO.executorService.submit(() -> {
            if (this.isMainVersion(instance)) {
                if (instance.getOrientation() == 8) {
                    try {
                        BufferedImage bi = ImageIO.read(new ByteArrayInputStream(instance.getThumbnail()));
                        bi = HbaseImageThumbnailDAO.rotateAntiCw(bi);
                        ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
                        ImageIO.write(bi, "jpg", os);
                        this.cacheManager.getCache(this.cacheMainJpegImagesName)
                            .putIfAbsent(instance.getImageId(), os.toByteArray());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    this.cacheManager.getCache(this.cacheMainJpegImagesName)
                        .putIfAbsent(instance.getImageId(), instance.getThumbnail());
                }
            }
            org.cache2k.processor.EntryProcessor<String, Map<Integer, ImageVersionDto>, Boolean> p = (entry) -> {
                if (!entry.exists()) {
                    entry.setValue(new HashMap<>());
                }
                entry.getValue()
                    .put((int) instance.getVersion(), this.toImageVersionDTO(instance));
                return true;
            };
            nativeCache.invoke(instance.getImageId(), p);
        });
        HbaseImageThumbnailDAO.LOGGER.info(
            "--> End of Found {} at date {}, epoch {}, {} [ {} ] ",
            imageDTO.getData()
                .getImageId(),
            imageDTO.getCreationDateAsString(),
            imageDTO.getData()
                .getCreationDate(),
            System.currentTimeMillis() / 1000.0f,
            (System.currentTimeMillis() - initTime) / 1000.0f);
    }

    protected Scan createScanToGetAllColumns() {
        Scan scan = new Scan();
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_SZ_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_THB_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_TECH_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_META_BYTES);
        return scan;
    }

    protected Scan createScanToGetAllColumnsWithoutImages() {
        Scan scan = new Scan();
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_SZ_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_THB_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_TECH_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(HbaseImageThumbnailDAO.FAMILY_META_BYTES);
        return scan;
    }

    private Cache<String, Map<Integer, ImageVersionDto>> getNativeCacheForJpegImagesVersion(
        CacheManager cacheManager2
    ) {
        return (Cache<String, Map<Integer, ImageVersionDto>>) cacheManager2.getCache(this.cacheJpegImagesVersionName)
            .getNativeCache();
    }

    private Cache<ImageKeyDto, ImageDto> getNativeCacheForImageDto(CacheManager cacheManager2) {
        return (Cache<ImageKeyDto, ImageDto>) cacheManager2.getCache(this.cacheImagesName)
            .getNativeCache();
    }

    protected static BufferedImage rotateCw(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        BufferedImage newImage = new BufferedImage(height, width, img.getType());

        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                newImage.setRGB(height - 1 - j, i, img.getRGB(i, j));
            }
        }

        return newImage;
    }

    protected static BufferedImage rotateAntiCw(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int newWidth = height;
        int newHeight = width;
        BufferedImage newImage = new BufferedImage(newWidth, newHeight, img.getType());
        Graphics2D g = newImage.createGraphics();
        g.translate((newWidth - width) / 2, (newHeight - height) / 2);
        g.rotate(-Math.PI / 2, width / 2, height / 2);
        g.drawRenderedImage(img, null);
        g.dispose();
        return newImage;
    }

    private ImageVersionDto toImageVersionDTO(HbaseImageThumbnail instance) {
        long initTime = System.currentTimeMillis();
        try {
            ImageVersionDto.Builder builderImageVersionDto = ImageVersionDto.builder();
            builderImageVersionDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
                .withImageId(instance.getImageId())
                .withVersion(instance.getVersion())
                .withJpegContent(instance.getThumbnail())
                .withOriginalHeight((int) instance.getOriginalHeight())
                .withOriginalWidth((int) instance.getOriginalWidth())
                .withImageName(instance.getImageName())
                .withOrientation((int) instance.getOrientation())
                .withImportDate(DateTimeHelper.toLocalDateTime(instance.getImportDate()));

            if (instance.getOrientation() == 8) {
                try {
                    BufferedImage bi = ImageIO.read(new ByteArrayInputStream(instance.getThumbnail()));
                    bi = HbaseImageThumbnailDAO.rotateAntiCw(bi);
                    ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
                    ImageIO.write(bi, "jpg", os);
                    builderImageVersionDto.withJpegContent(os.toByteArray());
                } catch (IOException e) {
                    HbaseImageThumbnailDAO.LOGGER.warn("Unexpected error", ExceptionUtils.getStackTrace(e));
                }
            }

            return builderImageVersionDto.build();
        } finally {
            HbaseImageThumbnailDAO.LOGGER
                .info("-> toImageVersionDTO [duration : {}]", (System.currentTimeMillis() - initTime) / 1000.0f);
        }
    }

    protected boolean isMainVersion(HbaseImageThumbnail instance) { return instance.getVersion() == 0; }

    private ImageDto toImageDTO(HbaseImageThumbnail instance) {
        ImageDto.Builder builderImageDto = ImageDto.builder();
        ImageKeyDto.Builder builderImageKeyDto = ImageKeyDto.builder();
        builderImageKeyDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
            .withImageId(instance.getImageId())
            .withVersion(instance.getVersion());

        builderImageDto.withData(builderImageKeyDto.build())
            .withOrientation((int) instance.getOrientation())
            .withCreationDateAsString(DateTimeHelper.toDateTimeAsString(instance.getCreationDate()))
            .withThumbnailHeight((int) instance.getHeight())
            .withThumbnailWidth((int) instance.getWidth())
            .withOriginalHeight((int) instance.getOriginalHeight())
            .withOriginalWidth((int) instance.getOriginalWidth())
            .withSpeed(this.exifService.toString(FieldType.RATIONAL, instance.getSpeed()))
            .withAperture(this.exifService.toString(FieldType.RATIONAL, instance.getAperture()))
            .withIso(Short.toString(instance.getIsoSpeed()))
            .withAlbum(
                instance.getAlbums()
                    .toString())
            .withKeywords(
                instance.getKeyWords()
                    .toArray(
                        new String[instance.getKeyWords()
                            .size()]))
            .withCamera(instance.getCamera())
            .withLens(new String(instance.getLens()))
            .withRatings(instance.getRatings())
            .withImageName(instance.getImageName());

        return builderImageDto.build();
    }

    private ImageDto getImageDto(ImageKeyDto imageKeyDto, OffsetDateTime creationDate, String id, int version) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withVersion((short) version)
            .build();

        Get get;
        byte[] key = this.getKey(hbaseData, this.hbaseDataInformation);
        get = new Get(key);
        try {
            try (
                Table table = this.connection.getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    HbaseImageThumbnail retValue = HbaseImageThumbnail.builder()
                        .build();
                    this.hbaseDataInformation.build(retValue, result);
                    return this.toImageDTO(retValue);
                }
            }
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        return null;

    }

    private Optional<ImageVersionDto> getImageVersionDto(OffsetDateTime creationDate, String id, int version) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withVersion((short) version)
            .build();
        try {
            Get get;
            byte[] key = this.getKey(hbaseData);
            get = new Get(key);
            try (
                Table table = this.connection.getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    HbaseImageThumbnail retValue = HbaseImageThumbnail.builder()
                        .build();
                    this.hbaseDataInformation.build(retValue, result);
                    return Optional.of(this.toImageVersionDTO(retValue));
                }
            }
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(null);

    }

    protected List<HbaseImageThumbnail> getNextThumbNailsOf(HbaseImageThumbnail initialKey) {
        HbaseImageThumbnailDAO.LOGGER.info("-> getNextThumbNailsOf of {}, end date is {}", initialKey);
        List<HbaseImageThumbnail> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            byte[] key = this.getKey(initialKey, this.getHbaseDataInformation());

            Scan scan = this.createScanToGetAllColumns();
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithTwoFields(ModelConstants.FIXED_WIDTH_CREATION_DATE,
                        initialKey.getImageId()
                            .getBytes("UTF-8"),
                        ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID,
                        (short) 1),
                    new PageFilter(1)));
            scan.withStartRow(key);
            scan.setReversed(true);
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                try {
                    HbaseImageThumbnail instance = new HbaseImageThumbnail();
                    this.getHbaseDataInformation()
                        .build(instance, t);
                    retValue.add(instance);
                    HbaseImageThumbnailDAO.LOGGER
                        .info("---> getNextThumbNailsOf of {}, found {}", initialKey, instance.getImageId());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HbaseImageThumbnailDAO.LOGGER
            .info("--> eND getNextThumbNailsOf of {}, elements found are {}", initialKey, retValue.size());
        return retValue;

    }

    protected List<HbaseImageThumbnail> getPreviousThumbNailsOf(HbaseImageThumbnail initialKey) {
        List<HbaseImageThumbnail> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {

            byte[] key = this.getKey(initialKey, this.getHbaseDataInformation());
            Scan scan = this.createScanToGetAllColumns();
            scan.withStartRow(key);
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithTwoFields(ModelConstants.FIXED_WIDTH_CREATION_DATE,
                        initialKey.getImageId()
                            .getBytes("UTF-8"),
                        ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID,
                        (short) 1),
                    new PageFilter(1)));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                try {
                    this.getHbaseDataInformation()
                        .build(instance, t);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    @Override
    public Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, int rating) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withVersion((short) version)
            .build();
        HbaseImageThumbnail retValue = null;
        Get get;
        byte[] key = this.getKey(hbaseData, this.hbaseDataInformation);
        get = new Get(key);
        try {
            try (
                Table table = this.connection.getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    retValue = HbaseImageThumbnail.builder()
                        .build();
                    this.hbaseDataInformation.build(retValue, result);
                    HbaseImageThumbnail previous = (HbaseImageThumbnail) retValue.clone();
                    retValue.setRatings(rating);
                    Put put = this.createHbasePut(
                        this.getKey(retValue, this.hbaseDataInformation),
                        this.getCfList(retValue, this.hbaseDataInformation));
                    table.put(put);
                    this.hbaseImagesOfRatingsDAO.updateMetadata(retValue, previous);
                    return Optional.of(this.toImageDTO(retValue));
                }
            } finally {
                try (
                    Table table = this.connection
                        .getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                    Result result = table.get(get);
                    retValue = HbaseImageThumbnail.builder()
                        .build();
                    this.hbaseDataInformation.build(retValue, result);
                    HbaseImageThumbnailDAO.LOGGER
                        .info("After updateRating keyword {}, hbase retrieved value {} ", rating, retValue);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        } catch (
            IOException |
            CloneNotSupportedException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        HbaseImageThumbnailDAO.LOGGER.info("Adding keyword {} to [{}]", keyword, id);
        keyword = keyword.trim();
        try {
            HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
            final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
            HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
                .withImageId(id)
                .withVersion((short) version)
                .build();
            HbaseImageThumbnail retValue = null;
            Get get;
            byte[] key = this.getKey(hbaseData, this.hbaseDataInformation);
            get = new Get(key);
            try {
                try (
                    Table table = this.connection
                        .getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                    Result result = table.get(get);
                    if ((result != null) && !result.isEmpty()) {
                        retValue = HbaseImageThumbnail.builder()
                            .build();
                        this.hbaseDataInformation.build(retValue, result);
                        retValue.getKeyWords()
                            .add(keyword);
                        Put put = this.createHbasePut(retValue);
                        put.addColumn(
                            HbaseImageThumbnailDAO.FAMILY_KEYWORDS_BYTES,
                            keyword.getBytes(Charset.forName("UTF-8")),
                            HbaseDataInformation.TRUE_VALUE);
                        this.bufferedMutator.mutate(put);
                        this.hbaseImagesOfKeywordsDAO.updateMetadata(retValue, keyword);
                        this.removeFromCache(id, creationDate, version);
                        this.bufferedMutator.flush();
                        return Optional.of(this.toImageDTO(retValue));
                    }
                }
            } catch (IOException e) {
                HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
            return Optional.empty();
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Adding keyword {} to [{}]", keyword, id);
        }
    }

    @Override
    public Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        keyword = keyword.trim();
        HbaseImageThumbnailDAO.LOGGER.info("Deleting keyword {} to [{}]", keyword, id);
        try {
            HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
            final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
            HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
                .withImageId(id)
                .withVersion((short) version)
                .build();
            HbaseImageThumbnail retValue = null;
            Get get;
            byte[] key = this.getKey(hbaseData, this.hbaseDataInformation);
            get = new Get(key);
            try {
                try (
                    Table table = this.connection
                        .getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                    Result result = table.get(get);
                    if ((result != null) && !result.isEmpty()) {
                        retValue = HbaseImageThumbnail.builder()
                            .build();
                        this.hbaseDataInformation.build(retValue, result);
                        retValue.getKeyWords()
                            .remove(keyword);
                        HbaseImageThumbnailDAO.LOGGER
                            .info("After keyword {} to [{}] : {}", keyword, id, retValue.getKeyWords());
                        Delete delete = this.createHbaseDelete(this.getKey(retValue, this.hbaseDataInformation));
                        delete.addColumns(
                            HbaseImageThumbnailDAO.FAMILY_KEYWORDS_BYTES,
                            keyword.getBytes(Charset.forName("UTF-8")));
                        this.bufferedMutator.mutate(delete);
                        this.hbaseImagesOfKeywordsDAO.removeFromMetadata(retValue, keyword);
                        this.removeFromCache(id, creationDate, version);
                        this.bufferedMutator.flush();
                        return Optional.of(this.toImageDTO(retValue));
                    }
                }
            } catch (IOException e) {
                HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            } finally {
                try (
                    Table table = this.connection
                        .getTable(TableName.valueOf(this.hbaseDataInformation.getTableName()))) {
                    Result result = table.get(get);
                    retValue = HbaseImageThumbnail.builder()
                        .build();
                    this.hbaseDataInformation.build(retValue, result);
                    HbaseImageThumbnailDAO.LOGGER
                        .info("After deleting keyword {}, hbase retrieved value {} ", keyword, retValue);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
            return Optional.empty();
        } finally {

            HbaseImageThumbnailDAO.LOGGER.info("End of deleting keyword {} to [{}]", keyword, id);
        }
    }

    protected void removeFromCache(String id, OffsetDateTime creationDate, int version) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);
        ImageKeyDto keyImageKeyDto = ImageKeyDto.builder()
            .withCreationDate(creationDate)
            .withImageId(id)
            .withVersion(version)
            .build();
        cache.remove(keyImageKeyDto);
    }

    @Override
    public void addAlbum(String id, OffsetDateTime creationDate, int version, String album) {}

    @Override
    public void delete(OffsetDateTime creationDate, String id, int version) { // TODO Auto-generated method stub
    }

}
