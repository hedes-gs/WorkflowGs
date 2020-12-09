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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;
import javax.imageio.ImageIO;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.cache2k.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.DateTimeHelper;
import com.gs.photo.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.workflow.exif.FieldType;
import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.gs.photos.repositories.IHbaseImageThumbnailDAO;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfMetadata;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.ImageVersionDto;

@Component
public class HbaseImageThumbnailDAO extends AbstractHbaseImageThumbnailDAO implements IHbaseImageThumbnailDAO {
    private static Logger                   LOGGER                   = LoggerFactory
        .getLogger(HbaseImageThumbnailDAO.class);

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

    @Autowired
    protected IHbaseImagesOfPersonsDAO      hbaseImagesOfPersonsDAO;

    protected Map<Integer, PageDescription> currentDefaultLoadedPage = new ConcurrentHashMap<>();

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
            .compare(o1, o2);
    }

    @Override
    public List<ImageDto> findLastImages(int pageSize, int pageNumber) {
        int countAll = 0;
        try {
            countAll = this.getNumberOfImages();
            HbaseImageThumbnailDAO.LOGGER.info("Total nb of elements {} ", countAll);
        } catch (Throwable e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        // final int pageSize2 = (pageSize % regions.size()) == 0 ? pageSize /
        // regions.size()
        // : (int) Math.round((pageSize / regions.size()) + 1);
        return this.buildPage(pageSize, pageNumber, countAll);
    }

    protected List<ImageDto> buildPage(int pageSize, int pageNumber, int countAll) {
        final int pageSize2 = pageSize;
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
                    totalLoadedElements = totalLoadedElements + this.loadPages(table, currentPageNumber, pageSize2);
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
                    pageDesc = this.currentDefaultLoadedPage.get(++startPage);
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

    @Override
    public List<ImageDto> findLastImagesByKeyword(int pageSize, int pageNumber, String keyword) {
        return this.hbaseImagesOfKeywordsDAO.getAllImagesOfMetadata(keyword, pageNumber, pageSize)
            .stream()
            .sorted((o1, o2) -> o2.compareTo(o1))
            .map((r) -> this.toImageDTO(r))
            .collect(Collectors.toList());
    }

    @Override
    public List<ImageDto> findLastImagesByPerson(int pageSize, int pageNumber, String person) {
        return this.hbaseImagesOfPersonsDAO.getAllImagesOfMetadata(person, pageNumber, pageSize)
            .stream()
            .sorted((o1, o2) -> o2.compareTo(o1))
            .map((r) -> this.toImageDTO(r))
            .collect(Collectors.toList());

    }

    private int loadPages(Table table, int currentPageNumber, int pageSize) {
        HbaseImageThumbnailDAO.LOGGER.info("Loading page {} with page size : {} ", currentPageNumber, pageSize);
        PageDescription pageDescription = this.currentDefaultLoadedPage
            .computeIfAbsent(currentPageNumber, (c) -> this.createPageDescription(table, c, pageSize));
        if (pageDescription.getPageSize() == 0) {
            pageDescription = this.createPageDescription(table, currentPageNumber, pageSize);
            this.currentDefaultLoadedPage.put(currentPageNumber, pageDescription);
        }
        return pageDescription.getPageSize();
    }

    private PageDescription createPageDescription(Table table, Integer currentPageNumber, int pageSize) {
        HbaseImageThumbnailDAO.LOGGER
            .info(" createPageDescription currentPageNumber = {},pageSize {} ", currentPageNumber, pageSize);

        try (
            Table pageTable = this.connection.getTable(TableName.valueOf("prod:" + IImageThumbnailDAO.TABLE_PAGE));) {
            PageDescription retValue = new PageDescription(currentPageNumber);
            long pageNumber = (currentPageNumber * pageSize) / IImageThumbnailDAO.PAGE_SIZE;

            Get get = new Get(AbstractDAO.convert(pageNumber));

            List<byte[]> getsList = new ArrayList(pageTable.get(get)
                .getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY)
                .keySet());

            long firstIndex = ((currentPageNumber * pageSize) % IImageThumbnailDAO.PAGE_SIZE);
            long lastIndex = (((currentPageNumber * pageSize) % IImageThumbnailDAO.PAGE_SIZE) + pageSize) - 1;
            HbaseImageThumbnailDAO.LOGGER.info(" Retrieving from {} to {} ", firstIndex, lastIndex);

            try (
                Table thumbTable = this.connection.getTable(
                    this.getHbaseDataInformation()
                        .getTable())) {
                Scan scan = this.createScanToGetAllColumnsWithoutImages()
                    .withStartRow(getsList.get((int) firstIndex))
                    .withStopRow(getsList.get((int) lastIndex), true);
                List<HbaseImageThumbnail> pageContent = StreamSupport.stream(
                    thumbTable.getScanner(scan)
                        .spliterator(),
                    false)
                    .map((r) -> {
                        HbaseImageThumbnail instance = new HbaseImageThumbnail();
                        this.hbaseDataInformation.build(instance, r);
                        return instance;
                    })
                    .collect(Collectors.toList());
                for (long k = 0; k < pageSize; k++) {
                    retValue.add(pageContent.get((int) k));
                }
            }
            return retValue;
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    protected void saveInCacheMainJpegImagesName(HbaseImageThumbnail instance) {
        byte[] jpegImageToCache = instance.getThumbnail()
            .get(1);
        if (instance.getOrientation() == 8) {
            try {
                BufferedImage bi = ImageIO.read(
                    new ByteArrayInputStream(instance.getThumbnail()
                        .get(1)));
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
    public long count(OffsetDateTime firstDate, OffsetDateTime lastDate) { return 500; }

    @Override
    public long count() throws Throwable { return super.countWithCoprocessorJob(this.getHbaseDataInformation()); }

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
            fl.addFilter(new PageFilter(page.getPageSize()));
            scan.setFilter(fl);
            HbaseImageThumbnail hbt = HbaseImageThumbnail.builder()
                .withCreationDate(firstDateEpochMillis)
                .withImageId(new String(""))
                .build();
            byte[] keyStartValue = new byte[this.hbaseDataInformation.getKeyLength()];
            byte[] keyStopValue = new byte[this.hbaseDataInformation.getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            scan.withStartRow(keyStartValue);
            hbt = HbaseImageThumbnail.builder()
                .withCreationDate(mastDateEpochMilli)
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
            if (instance.getOrientation() == 8) {
                try {
                    BufferedImage bi = ImageIO.read(
                        new ByteArrayInputStream(instance.getThumbnail()
                            .get(1)));
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
            org.cache2k.processor.EntryProcessor<String, Map<Integer, ImageVersionDto>, Boolean> p = (entry) -> {
                if (!entry.exists()) {
                    entry.setValue(new HashMap<>());
                }
                entry.getValue()
                    .put(1, this.toImageVersionDTO(instance));
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
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_SZ_BYTES);
        scan.addColumn(IImageThumbnailDAO.FAMILY_THB_BYTES, "1".getBytes(Charset.forName("UTF-8")));
        scan.addFamily(IImageThumbnailDAO.FAMILY_TECH_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_META_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_PERSONS_BYTES);
        return scan;
    }

    protected Scan createScanToGetAllColumnsWithoutImages() {
        Scan scan = new Scan();
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_SZ_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_TECH_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_META_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_PERSONS_BYTES);
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
                .withJpegContent(
                    instance.getThumbnail()
                        .get(1))
                .withOriginalHeight((int) instance.getOriginalHeight())
                .withOriginalWidth((int) instance.getOriginalWidth())
                .withImageName(instance.getImageName())
                .withOrientation((int) instance.getOrientation())
                .withImportDate(DateTimeHelper.toLocalDateTime(instance.getImportDate()));

            if (instance.getOrientation() == 8) {
                try {
                    BufferedImage bi = ImageIO.read(
                        new ByteArrayInputStream(instance.getThumbnail()
                            .get(1)));
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

    private ImageDto toImageDTO(HbaseImageThumbnail instance) {
        ImageDto.Builder builderImageDto = ImageDto.builder();
        ImageKeyDto.Builder builderImageKeyDto = ImageKeyDto.builder();
        builderImageKeyDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
            .withImageId(instance.getImageId())
            .withVersion(1);

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
            .withPersons(
                (instance.getPersons() != null) && (instance.getPersons()
                    .size() > 0) ? instance.getPersons()
                        .toArray(
                            new String[instance.getPersons()
                                .size()])
                        : null)
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
            // .withRatings(instance.getRatings())
            .withImageName(instance.getImageName());

        return builderImageDto.build();
    }

    private ImageDto toImageDTO(HbaseImagesOfMetadata instance) {
        ImageDto.Builder builderImageDto = ImageDto.builder();
        ImageKeyDto.Builder builderImageKeyDto = ImageKeyDto.builder();
        builderImageKeyDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
            .withImageId(instance.getImageId());

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
            .withCamera(instance.getCamera())
            .withLens(new String(instance.getLens()))
            .withImageName(instance.getImageName());

        return builderImageDto.build();
    }

    private ImageDto getImageDto(ImageKeyDto imageKeyDto, OffsetDateTime creationDate, String id, int version) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .build();

        Get get;
        byte[] key = GenericDAO.getKey(hbaseData, this.hbaseDataInformation);
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
                    if (!retValue.getThumbnail()
                        .containsKey(1)) {
                        HbaseImageThumbnailDAO.LOGGER
                            .error("Unable to find version 1 for {} - {} ", id, retValue.getPath());
                    }
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
            byte[] key = GenericDAO.getKey(initialKey, this.getHbaseDataInformation());

            Scan scan = this.createScanToGetAllColumns();
            scan.withStartRow(key)
                .setLimit(2)
                .setReversed(true);
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {
                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                this.getHbaseDataInformation()
                    .build(instance, t);
                retValue.add(instance);
                HbaseImageThumbnailDAO.LOGGER
                    .info("---> getNextThumbNailsOf of {}, found {}", initialKey, instance.getImageId());
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

            byte[] key = GenericDAO.getKey(initialKey, this.getHbaseDataInformation());
            Scan scan = this.createScanToGetAllColumns();
            scan.withStartRow(key);
            scan.setLimit(2);
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                this.getHbaseDataInformation()
                    .build(instance, t);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    @Override
    public Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .build();
        try {
            final HbaseImageThumbnail hbi = super.get(hbaseData);
            hbi.getRatings()
                .stream()
                .findFirst()
                .ifPresent((r) -> this.hbaseImagesOfRatingsDAO.deleteMetaData(hbi, r));
            this.hbaseImagesOfRatingsDAO.addMetaData(hbi, rating);
            HbaseImageThumbnail hbi2 = super.get(hbaseData);
            return Optional.of(this.toImageDTO(hbi2));
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Adding rating {} to [{}]", rating, id);
        }
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
                .build();

            HbaseImageThumbnail hbi = super.get(hbaseData);
            this.hbaseImagesOfKeywordsDAO.addMetaData(hbi, keyword);
            hbi = super.get(hbaseData);
            return Optional.of(this.toImageDTO(hbi));
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Adding keyword {} to [{}]", keyword, id);
        }
    }

    @Override
    public Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        HbaseImageThumbnailDAO.LOGGER.info("Deleting keyword {} to [{}]", keyword, id);
        keyword = keyword.trim();

        try {
            HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
            final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
            HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
                .withImageId(id)
                .build();
            HbaseImageThumbnail hbi = super.get(hbaseData);
            this.hbaseImagesOfKeywordsDAO.deleteMetaData(hbi, keyword);
            hbi = super.get(hbaseData);
            return Optional.of(this.toImageDTO(hbi));
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Deleting keyword {} to [{}]", keyword, id);
        }
    }

    protected void removeFromCache(String id, OffsetDateTime creationDate, int version) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);
        ImageKeyDto keyImageKeyDto = ImageKeyDto.builder()
            .withCreationDate(creationDate)
            .withImageId(id)
            .build();
        cache.remove(keyImageKeyDto);
    }

    @Override
    public void addAlbum(String id, OffsetDateTime creationDate, int version, String album) {}

    @Override
    public void delete(OffsetDateTime creationDate, String id, int version) { // TODO Auto-generated method stub
    }

    @Override
    public Optional<ImageDto> addPerson(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailDAO.LOGGER.info("Adding person {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
            final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
            HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
                .withImageId(id)
                .build();
            HbaseImageThumbnail hbi = super.get(hbaseData);
            this.hbaseImagesOfPersonsDAO.addMetaData(hbi, person);
            hbi = super.get(hbaseData);
            return Optional.of(this.toImageDTO(hbi));
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Deleting keyword {} to [{}]", person, id);
        }
    }

    @Override
    public Optional<ImageDto> deletePerson(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailDAO.LOGGER.info("Deleting person {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
            final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
            HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
                .withImageId(id)
                .build();
            HbaseImageThumbnail hbi = super.get(hbaseData);
            this.hbaseImagesOfPersonsDAO.deleteMetaData(hbi, person);
            hbi = super.get(hbaseData);
            return Optional.of(this.toImageDTO(hbi));
        } catch (IOException e) {
            HbaseImageThumbnailDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailDAO.LOGGER.info("End of Deleting keyword {} to [{}]", person, id);
        }
    }

    protected int getNumberOfImages() throws IOException {
        try (
            Table pageTable = this.connection.getTable(TableName.valueOf("prod:" + IImageThumbnailDAO.TABLE_PAGE));) {

            Scan scan = new Scan().addFamily(AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY);
            long retValue = StreamSupport.stream(
                pageTable.getScanner(scan)
                    .spliterator(),
                false)
                .mapToLong(
                    (c) -> Bytes.toLong(
                        CellUtil.cloneValue(
                            c.getColumnLatestCell(
                                AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                AbstractDAO.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS))))
                .sum();
            return (int) retValue;
        }
    }

    @PostConstruct
    protected void initLocal() throws IOException, InterruptedException {
        long countAll = 0;
        try {
            countAll = super.countWithCoprocessorJob(this.getHbaseDataInformation());
            HbaseImageThumbnailDAO.LOGGER.info("Total nb of elements {} ", countAll);
        } catch (Throwable e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(70);
        Collection<Callable<Integer>> recursiveTasks = new ArrayList<>();
        try (
            final Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {

            for (int k = 0; k < 70; k++) {
                final int page = k;
                recursiveTasks.add(() -> this.loadPages(table, page, 100));
            }
            forkJoinPool.invokeAll(recursiveTasks);
            forkJoinPool.shutdown();
            forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            HbaseImageThumbnailDAO.LOGGER.info("End of loading {} ", countAll);
        }
    }

}
