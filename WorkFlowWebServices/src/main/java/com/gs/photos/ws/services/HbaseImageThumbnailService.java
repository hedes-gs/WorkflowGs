package com.gs.photos.ws.services;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cache2k.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ServiceException;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photo.common.workflow.hbase.dao.PageDescription;
import com.gs.photos.ws.repositories.IHbaseImageThumbnailDAO;
import com.gs.photos.ws.repositories.IHbaseImageThumbnailService;
import com.gs.photos.ws.repositories.impl.HbaseExifDataDAO;
import com.gs.photos.ws.repositories.impl.IHbaseImagesOfAlbumsDAO;
import com.gs.photos.ws.repositories.impl.IHbaseImagesOfKeywordsDAO;
import com.gs.photos.ws.repositories.impl.IHbaseImagesOfPersonsDAO;
import com.gs.photos.ws.repositories.impl.IHbaseImagesOfRatingsDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.SizeAndJpegContent;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.ImageVersionDto;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Service
public class HbaseImageThumbnailService implements IHbaseImageThumbnailService {
    private static final Scheduler                    SCAN_RETRIEVE_THREAD_POOL_NEW_PARALLEL = Schedulers
        .newParallel("scan-retrieve", AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1);
    private static final Scheduler                    THREAD_POOL_FOR_SCAN_PREV_PARALLEL     = Schedulers
        .newParallel("scan-prev", AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1);
    private static final Scheduler                    SCAN_NEXT_THREAD_POOL                  = Schedulers
        .newParallel("scan-next", AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1);

    private static Logger                             LOGGER                                 = LoggerFactory
        .getLogger(HbaseImageThumbnailService.class);

    static protected ExecutorService                  executorService                        = Executors
        .newFixedThreadPool(64);

    @Value("${cache.jpegimages.name}")
    protected String                                  cacheMainJpegImagesName;
    @Value("${cache.images.name}")
    protected String                                  cacheImagesName;
    @Value("${cache.jpegimages.version.name}")
    protected String                                  cacheJpegImagesVersionName;

    @Autowired
    protected Connection                              connection;

    @Autowired
    protected CacheManager                            cacheManager;

    @Autowired
    protected IExifService                            exifService;

    @Autowired
    protected IHbaseImagesOfRatingsDAO                hbaseImagesOfRatingsDAO;

    @Autowired
    protected IHbaseImagesOfKeywordsDAO               hbaseImagesOfKeywordsDAO;

    @Autowired
    protected IHbaseImagesOfPersonsDAO                hbaseImagesOfPersonsDAO;

    @Autowired
    protected IHbaseImagesOfAlbumsDAO                 hbaseImagesOfAlbumsDAO;

    @Autowired
    protected IHbaseImageThumbnailDAO                 hbaseImageThumbnailDAO;

    @Autowired
    protected HbaseExifDataDAO                        hbaseExifDataDAO;

    protected ReentrantLock                           lockOnLocalCache                       = new ReentrantLock();

    protected Map<Long, PageDescription<ImageKeyDto>> currentDefaultLoadedPage               = new ConcurrentHashMap<>();

    protected ForkJoinPool                            forkJoinPool                           = new ForkJoinPool(120);

    private static int compare(ImageKeyDto a, ImageKeyDto b) {
        return Comparator.comparing(ImageKeyDto::getCreationDate)
            .thenComparing(ImageKeyDto::getImageId)
            .compare(a, b);
    }

    @Override
    public void invalidCache() {
        try {
            this.lockOnLocalCache.lock();
            this.currentDefaultLoadedPage.clear();
        } finally {
            this.lockOnLocalCache.unlock();
        }
    }

    @Override
    public Flux<ImageDto> findLastImages(int pageSize, int pageNumber) {
        try {
            this.lockOnLocalCache.lock();
            int countAll = 0;
            try {
                countAll = this.getNumberOfImages();
                HbaseImageThumbnailService.LOGGER.info("Total nb of elements {} ", countAll);
            } catch (Throwable e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            // final int pageSize2 = (pageSize % regions.size()) == 0 ? pageSize /
            // regions.size()
            // : (int) Math.round((pageSize / regions.size()) + 1);
            return Flux.fromStream(
                this.buildPage(pageSize, pageNumber, countAll)
                    .stream());
        } finally {
            this.lockOnLocalCache.unlock();
        }
    }

    protected List<ImageDto> buildPage(int pageSize, int pageNumber, int countAll) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);

        if (pageNumber > 0) {
            HbaseImageThumbnailService.LOGGER
                .info("[HBASE_IMG_THUMBNAIL_DAO]build page size is {}, number is {} ", pageSize, pageNumber);
            List<ImageKeyDto> retValue = new ArrayList<>();
            this.currentDefaultLoadedPage.clear();
            try {
                long pageNumberInTablePage = ((pageNumber - 1) * pageSize) / IImageThumbnailDAO.PAGE_SIZE;
                long initialIndex = ((pageNumber - 1) * pageSize)
                    - (pageNumberInTablePage * IImageThumbnailDAO.PAGE_SIZE);
                PageDescription<ImageKeyDto> pageDescription = this.currentDefaultLoadedPage.computeIfAbsent(
                    pageNumberInTablePage,
                    (c) -> this.hbaseImageThumbnailDAO.loadPageInTablePage(pageNumberInTablePage, pageSize));

                long lastIndex = Math.min(pageDescription.getPageSize(), initialIndex + pageSize);
                HbaseImageThumbnailService.LOGGER.info(
                    "[HBASE_IMG_THUMBNAIL_DAO]build page size is {}, number is {} - initial index {} - last index {}",
                    pageSize,
                    pageNumber,
                    initialIndex,
                    lastIndex);

                for (int k = (int) initialIndex; k < lastIndex; k++) {
                    retValue.add(
                        pageDescription.getPageContent()
                            .get(k));
                }
                if (retValue.size() < pageSize) {
                    pageDescription = this.currentDefaultLoadedPage.computeIfAbsent(
                        pageNumberInTablePage + 1,
                        (c) -> this.hbaseImageThumbnailDAO.loadPageInTablePage(pageNumberInTablePage, pageSize));
                    for (int k = 0; k < Math.min(pageDescription.getPageSize(), (pageSize - retValue.size())); k++) {
                        retValue.add(
                            pageDescription.getPageContent()
                                .get(k));
                    }

                }
            } catch (Exception e) {
                HbaseImageThumbnailService.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
            final List<ImageDto> sortedRetValue = retValue.stream()

                .map((x) -> cache.computeIfAbsent(x, () -> this.hbaseImageThumbnailDAO.getImageDto(x)))

                .collect(Collectors.toList());
            sortedRetValue.forEach(
                (e) -> HbaseImageThumbnailService.LOGGER.info(
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
    public Flux<ImageDto> findLastImagesByKeyword(int pageSize, int pageNumber, String keyword) {
        return this.hbaseImagesOfKeywordsDAO.getAllImagesOfMetadata(keyword, pageNumber, pageSize)
            .sort((o1, o2) -> o2.compareTo(o1))
            .map((r) -> this.toImageDTO(r));
    }

    @Override
    public Flux<ImageDto> findLastImagesByPerson(int pageSize, int pageNumber, String person) {
        return this.hbaseImagesOfPersonsDAO.getAllImagesOfMetadata(person, pageNumber, pageSize)
            .sort((o1, o2) -> o2.compareTo(o1))
            .map((r) -> this.toImageDTO(r));
    }

    @Override
    public Flux<ImageDto> findImagesByAlbum(int pageSize, int pageNumber, String album) {
        return this.hbaseImagesOfAlbumsDAO.getAllImagesOfMetadata(album, pageNumber, pageSize)
            .sort((o1, o2) -> o2.compareTo(o1))
            .map((r) -> this.toImageDTO(r));
    }

    static private class KeySet {
        byte[] key;
        Short  salt;

        public byte[] getKey() { return this.key; }

        public Short getSalt() { return this.salt; }

        KeySet(byte[] key) {
            this.key = key;
            this.salt = Bytes.toShort(key, 8);

        }
    }

    static private class KeySetOther {
        byte[] key;
        Short  salt;

        public byte[] getKey() { return this.key; }

        public Short getSalt() { return this.salt; }

        KeySetOther(byte[] key) {
            this.key = key;
            this.salt = Bytes.toShort(key, 0);

        }
    }

    private int compare(KeySet a, KeySet b) { return UnsignedBytes.lexicographicalComparator()
        .compare(a.key, b.key); }

    private int compare(KeySetOther a, KeySetOther b) {
        return UnsignedBytes.lexicographicalComparator()
            .compare(a.key, b.key);
    }

    protected void saveInCacheMainJpegImagesName(HbaseImageThumbnail instance) {
        byte[] jpegImageToCache = instance.getThumbnail()
            .get(1)
            .getJpegContent();
        if (instance.getOrientation() == 8) {
            try {
                BufferedImage bi = ImageIO.read(
                    new ByteArrayInputStream(instance.getThumbnail()
                        .get(1)
                        .getJpegContent()));
                bi = HbaseImageThumbnailService.rotateAntiCw(bi);
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
    public ImageDto findById(short salt, OffsetDateTime creationDate, String id, int version) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);
        ImageKeyDto.Builder builder = ImageKeyDto.builder();
        ImageKeyDto key = builder.withCreationDate(creationDate)
            .withSalt(salt)
            .withImageId(id)
            .withVersion(version)
            .build();
        ImageDto imgDto = cache.computeIfAbsent(key, () -> this.hbaseImageThumbnailDAO.getImageDto(key));
        return imgDto;
    }

    @Override
    public Optional<ImageDto> getNextImageById(short salt, OffsetDateTime creationDate, String id, int version) {
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        StopWatch watch = new StopWatch();
        watch.start();
        try {
            Flux<ImageDto> f = Flux.range(1, AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE)
                .parallel(AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1)
                .runOn(HbaseImageThumbnailService.SCAN_NEXT_THREAD_POOL)
                .map(
                    (k) -> HbaseImageThumbnail.builder()
                        .withCreationDate(creationDateAsLong)
                        .withRegionSalt(k.shortValue())
                        .withImageId(id)
                        .build())
                .flatMap((x) -> this.hbaseImageThumbnailDAO.getNextThumbNailsOf(x, x.getRegionSalt() != salt))
                .sorted((a, b) -> HbaseImageThumbnail.compareForSorting(a, b))
                .map((t) -> {
                    try {
                        return this.hbaseImageThumbnailDAO.get(t);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map((t) -> this.toImageDTO(t));
            return Optional.ofNullable(f.blockFirst());
        } finally {
            watch.stop();
            HbaseImageThumbnailService.LOGGER.info(
                "[HBASE_IMG_THUMBNAIL_DAO] duration getting next  for {}-{} is {} ",
                salt,
                id,
                watch.formatTime());
        }
    }

    @Override
    public Optional<ImageDto> getPreviousImageById(short salt, OffsetDateTime creationDate, String id, int version) {
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        StopWatch watch = new StopWatch();
        watch.start();
        try {
            Flux<ImageDto> f = Flux.range(1, AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE)
                .parallel(AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1)
                .runOn(HbaseImageThumbnailService.THREAD_POOL_FOR_SCAN_PREV_PARALLEL)
                .map(
                    (k) -> HbaseImageThumbnail.builder()
                        .withCreationDate(creationDateAsLong)
                        .withRegionSalt(k.shortValue())
                        .withImageId(id)
                        .build())
                .flatMap((x) -> this.hbaseImageThumbnailDAO.getPreviousThumbNailsOf(x, x.getRegionSalt() != salt))
                .sorted((a, b) -> HbaseImageThumbnail.compareForSorting(b, a))
                .map((t) -> {
                    try {
                        return this.hbaseImageThumbnailDAO.get(t);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map((t) -> this.toImageDTO(t));
            return Optional.ofNullable(f.blockFirst());
        } finally {
            watch.stop();
            HbaseImageThumbnailService.LOGGER
                .info("[HBASE_IMG_THUMBNAIL_DAO] duration getting previous for {} is {} ", salt, watch.formatTime());
        }
    }

    @Override
    public byte[] findImageRawById(short salt, OffsetDateTime creationDate, String id, int version) {
        Cache<String, Map<Integer, ImageVersionDto>> nativeCache = this
            .getNativeCacheForJpegImagesVersion(this.cacheManager);

        if (!nativeCache.containsKey(id)) {
            HbaseImageThumbnailService.LOGGER
                .info("-> findImageRawById cache missed for [{},{},{}] ", creationDate, id, version);
        }

        byte[] retValue = nativeCache.computeIfAbsent(id, () -> {
            Map<Integer, ImageVersionDto> map = new HashMap<>();
            map.put(
                version,
                this.hbaseImageThumbnailDAO.getImageVersionDto(creationDate, id, version)
                    .orElseThrow(() -> new IllegalArgumentException()));
            return map;
        })
            .computeIfAbsent(
                version,
                (t) -> this.hbaseImageThumbnailDAO.getImageVersionDto(creationDate, id, version)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to find version for")))
            .getJpegContent();
        return retValue;
    }

    @Override
    public long count(OffsetDateTime firstDate, OffsetDateTime lastDate) { return 500; }

    @Override
    public long count() throws Throwable { return this.hbaseImageThumbnailDAO.count(); }

    @Override
    public Flux<ImageDto> getThumbNailsByDate(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        KeyEnumType keyType,
        short... versions
    ) {
        HbaseImageThumbnailService.LOGGER.info(
            "[HBASE_IMG_THUMBNAIL_SERVICE] getThumbNailsByDate  {} - {} - page number {} - page size {}  ",
            firstDate,
            keyType,
            page.getPageNumber(),
            page.getPageSize());
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);

        Map<Short, List<KeySetOther>> keys = null;
        try (
            Table pageTable = this.connection.getTable(TableName.valueOf("prod:" + "image_thumbnail_key"))) {
            final String keyAsString = AbstractHbaseStatsDAO.toKey(firstDate, keyType)
                .get(keyType);
            byte[] key = keyAsString.getBytes(Charset.forName("UTF-8"));

            Get get = new Get(key);

            final NavigableMap<byte[], byte[]> currentPageContent = pageTable.get(get)
                .getFamilyMap("imgs".getBytes(Charset.forName("UTF-8")));

            if (currentPageContent != null) {
                keys = currentPageContent.keySet()
                    .stream()
                    .map(KeySetOther::new)
                    .collect(Collectors.groupingBy((c) -> c.getSalt()));

                List<Scan> scans = new ArrayList<>();
                for (short salt : keys.keySet()) {
                    final List<KeySetOther> list = keys.get(salt);
                    list.sort((a, b) -> this.compare(a, b));
                    byte[] firstKeyToRetrieve = list.get(0)
                        .getKey();
                    byte[] midOneKeyToRetrieve = list.get(list.size() / 3)
                        .getKey();
                    byte[] midTwoKeyToRetrieve = list.get((2 * list.size()) / 3)
                        .getKey();

                    byte[] lastKeyToRetrieve = list.get(list.size() - 1)
                        .getKey();

                    Scan scan = this.createScanToGetAllColumnsWithoutImages()
                        .withStartRow(firstKeyToRetrieve)
                        .withStopRow(midOneKeyToRetrieve, true);
                    scans.add(scan);
                    scan = this.createScanToGetAllColumnsWithoutImages()
                        .withStartRow(midOneKeyToRetrieve, false)
                        .withStopRow(midTwoKeyToRetrieve, true);
                    scans.add(scan);
                    scan = this.createScanToGetAllColumnsWithoutImages()
                        .withStartRow(midTwoKeyToRetrieve, false)
                        .withStopRow(lastKeyToRetrieve, true);

                    scans.add(scan);
                }

                Flux<ImageDto> f = Flux.fromStream(scans.stream())
                    .parallel(scans.size())
                    .runOn(HbaseImageThumbnailService.SCAN_RETRIEVE_THREAD_POOL_NEW_PARALLEL)
                    .flatMap((k) -> this.hbaseImageThumbnailDAO.getImageKeyDtoList(k))
                    .ordered((a, b) -> HbaseImageThumbnailService.compare(a, b))
                    .skip((page.getPageNumber() - 1) * page.getPageSize())
                    .take(page.getPageSize())
                    .map((x) -> cache.computeIfAbsent(x, () -> this.hbaseImageThumbnailDAO.getImageDto(x)));
                return f;
            } else {
                return Flux.empty();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Scan createScanToGetAllColumns() {
        Scan scan = new Scan();
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addColumn(IImageThumbnailDAO.FAMILY_THB_BYTES, "1".getBytes(Charset.forName("UTF-8")));
        scan.addFamily(IImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMPORT_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_PERSONS_BYTES);
        return scan;
    }

    protected Scan createScanToGetAllColumnsWithoutImages() {
        Scan scan = new Scan();
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMPORT_BYTES);
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

    private ImageVersionDto toImageVersionDTO(HbaseImageThumbnail instance, int version) {
        long initTime = System.currentTimeMillis();
        try {
            final SizeAndJpegContent sizeAndJpegContent = instance.getThumbnail()
                .get(version);
            HbaseImageThumbnailService.LOGGER.info("[{}]Image size is {}", instance.getImageId(), sizeAndJpegContent);
            ImageVersionDto.Builder builderImageVersionDto = ImageVersionDto.builder();
            final byte[] jpegContent = sizeAndJpegContent.getJpegContent();
            builderImageVersionDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
                .withImageId(instance.getImageId())
                .withThumbnailHeight(sizeAndJpegContent.getHeight())
                .withThumbnailWidth(sizeAndJpegContent.getWidth())
                .withJpegContent(jpegContent)
                .withOriginalHeight((int) instance.getOriginalHeight())
                .withOriginalWidth((int) instance.getOriginalWidth())
                .withImageName(instance.getImageName())
                .withOrientation((int) instance.getOrientation())
                .withImportDate(DateTimeHelper.toLocalDateTime(instance.getImportDate()));

            if (instance.getOrientation() == 8) {
                try {
                    BufferedImage bi = ImageIO.read(new ByteArrayInputStream(jpegContent));
                    bi = HbaseImageThumbnailService.rotateAntiCw(bi);
                    ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
                    ImageIO.write(bi, "jpg", os);
                    builderImageVersionDto.withJpegContent(os.toByteArray());
                } catch (IOException e) {
                    HbaseImageThumbnailService.LOGGER.warn("Unexpected error", ExceptionUtils.getStackTrace(e));
                }
            }

            return builderImageVersionDto.build();
        } finally {
            HbaseImageThumbnailService.LOGGER
                .info("-> toImageVersionDTO [duration : {}]", (System.currentTimeMillis() - initTime) / 1000.0f);
        }
    }

    @Override
    public ImageDto toImageDTO(HbaseImageThumbnail instance) {
        ImageDto.Builder builderImageDto = ImageDto.builder();
        ImageKeyDto.Builder builderImageKeyDto = ImageKeyDto.builder();
        builderImageKeyDto.withCreationDate(DateTimeHelper.toLocalDateTime(instance.getCreationDate()))
            .withSalt(instance.getRegionSalt())
            .withImageId(instance.getImageId())
            .withVersion(1);
        final SizeAndJpegContent sizeAndJpegContent = instance.getThumbnail()
            .get(1);

        builderImageDto.withData(builderImageKeyDto.build())
            .withOrientation((int) instance.getOrientation())
            .withCreationDateAsString(DateTimeHelper.toDateTimeAsString(instance.getCreationDate()))
            .withOriginalHeight((int) instance.getOriginalHeight())
            .withOriginalWidth((int) instance.getOriginalWidth())
            .withSpeed(this.exifService.toString(FieldType.RATIONAL, instance.getSpeed()))
            .withAperture(this.exifService.toString(FieldType.RATIONAL, instance.getAperture()))
            .withThumbnailHeight(sizeAndJpegContent != null ? sizeAndJpegContent.getHeight() : 0)
            .withThumbnailWidth(sizeAndJpegContent != null ? sizeAndJpegContent.getWidth() : 0)
            .withIso(Short.toString(instance.getIsoSpeed()))
            .withPersons(
                (instance.getPersons() != null) && (instance.getPersons()
                    .size() > 0) ? instance.getPersons()
                        .toArray(
                            new String[instance.getPersons()
                                .size()])
                        : null)
            .withAlbums(
                instance.getAlbums()
                    .toArray(
                        new String[instance.getPersons()
                            .size()]))
            .withKeywords(
                instance.getKeyWords()
                    .toArray(
                        new String[instance.getKeyWords()
                            .size()]))
            .withCamera(instance.getCamera())
            .withLens(instance.getLens() != null ? new String(instance.getLens()) : "Lens is unknown")
            // .withRatings(instance.getRatings())
            .withImageName(instance.getImageName());

        return builderImageDto.build();
    }

    protected HbaseImageThumbnail toHbaseImageThumbnailKey(OffsetDateTime creationDate, String id) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        final long creationDateAsLong = DateTimeHelper.toEpochMillis(creationDate);
        HbaseImageThumbnail hbaseData = builder.withCreationDate(creationDateAsLong)
            .withImageId(id)
            .withRegionSalt((short) creationDate.getDayOfMonth())
            .withKeyWords(new HashSet<>())
            .withAlbums(new HashSet<>())
            .withPersons(new HashSet<>())
            .withRatings(new HashSet<>())
            .build();
        return hbaseData;
    }

    @Override
    public Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating) {

        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            // HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(hbaseData);
            this.hbaseImagesOfRatingsDAO.addMetaData(hbi, rating);
            HbaseImageThumbnail hbi2 = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi2);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of Adding rating {} to [{}]", rating, id);
        }
    }

    @Override
    public Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        HbaseImageThumbnailService.LOGGER.info("Adding keyword {} to [{}]", keyword, id);
        keyword = keyword.trim();

        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            // HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(hbaseData);
            this.hbaseImagesOfKeywordsDAO.addMetaData(hbi, keyword);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of Adding keyword {} to [{}]", keyword, id);
        }
    }

    @Override
    public Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        HbaseImageThumbnailService.LOGGER.info("Deleting keyword {} to [{}]", keyword, id);
        keyword = keyword.trim();

        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            // HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(hbaseData);
            this.hbaseImagesOfKeywordsDAO.deleteMetaData(hbi, keyword);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of Deleting keyword {} to [{}]", keyword, id);
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
    public Optional<ImageDto> addAlbum(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailService.LOGGER.info("Adding album {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            // HbaseImageThumbnail hbi = this.hbaseImageThumbnailDAO.get(hbaseData);
            this.hbaseImagesOfAlbumsDAO.addMetaData(hbi, person);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of adding album  {} to [{}]", person, id);
        }
    }

    @Override
    public Optional<ImageDto> delete(OffsetDateTime creationDate, String id) throws IOException {
        HbaseImageThumbnail hbaseData = this.toHbaseImageThumbnailKey(creationDate, id);
        Optional<ImageDto> retValue = this
            .getNextImageById(hbaseData.getRegionSalt(), creationDate, hbaseData.getImageId(), 1);
        HbaseImageThumbnailService.LOGGER
            .info("Deleting image id {} - salt {}", hbaseData.getImageId(), hbaseData.getRegionSalt());
        this.hbaseImageThumbnailDAO.delete(hbaseData);
        try {
            HbaseImageThumbnailService.LOGGER
                .info("Deleting EXIFs of image id {} - salt {}", hbaseData.getImageId(), hbaseData.getRegionSalt());
            this.hbaseExifDataDAO.delete(hbaseData.getRegionSalt(), id);
        } catch (ServiceException e) {
            throw new RuntimeException(e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    @Override
    public Optional<ImageDto> addPerson(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailService.LOGGER.info("Adding person {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            this.hbaseImagesOfPersonsDAO.addMetaData(hbi, person);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of adding person  {} to [{}]", person, id);
        }
    }

    protected ImageDto updateLocalCache(String id, OffsetDateTime creationDate, int version, HbaseImageThumbnail hbi) {
        Cache<ImageKeyDto, ImageDto> cache = this.getNativeCacheForImageDto(this.cacheManager);
        ImageKeyDto.Builder builder = ImageKeyDto.builder();
        ImageKeyDto key = builder.withCreationDate(creationDate)
            .withSalt(hbi.getRegionSalt())
            .withImageId(id)
            .withVersion(version)
            .build();
        final ImageDto imageDTO = this.toImageDTO(hbi);
        cache.put(key, imageDTO);
        return imageDTO;
    }

    @Override
    public Optional<ImageDto> deletePerson(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailService.LOGGER.info("Deleting person {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            this.hbaseImagesOfPersonsDAO.deleteMetaData(hbi, person);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of Deleting person {} to [{}]", person, id);
        }
    }

    @Override
    public Optional<ImageDto> deleteAlbum(String id, OffsetDateTime creationDate, int version, String person) {
        HbaseImageThumbnailService.LOGGER.info("Deleting album {} to [{}]", person, id);
        person = person.trim();
        try {
            HbaseImageThumbnail hbi = this.toHbaseImageThumbnailKey(creationDate, id);
            this.hbaseImagesOfAlbumsDAO.deleteMetaData(hbi, person);
            hbi = this.hbaseImageThumbnailDAO.get(hbi);
            ImageDto imgDto = this.updateLocalCache(id, creationDate, version, hbi);
            return Optional.of(imgDto);
        } catch (IOException e) {
            HbaseImageThumbnailService.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        } finally {
            HbaseImageThumbnailService.LOGGER.info("End of Deleting person {} to [{}]", person, id);
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

}
