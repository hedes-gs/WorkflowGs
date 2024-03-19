package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfMetadata;
import com.workflow.model.ModelConstants;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class AbstractHbaseImagesOfMetadataDAO<T extends HbaseImagesOfMetadata, T2> extends GenericDAO<T>
    implements IHbaseImagesOfMetadataDAO<HbaseImageThumbnail, T2> {

    private static Logger                        LOGGER                             = LoggerFactory
        .getLogger(AbstractHbaseImagesOfMetadataDAO.class);
    private static final Scheduler               THREAD_POOL_FOR_SCAN_PREV_PARALLEL = Schedulers
        .newParallel("scan-prev", AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1);
    private static final Scheduler               THREAD_POOL_FOR_SCAN_NEXT_PARALLEL = Schedulers
        .newParallel("scan-next", AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1);

    protected static final byte[]                FAMILY_SZ_AS_BYTES                 = "sz"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_THB_AS_BYTES                = "thb"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_IMG_AS_BYTES                = "img"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_TECH_AS_BYTES               = "tech"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_ALBUMS_AS_BYTES             = "albums"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_KEYWORDS_AS_BYTES           = "keywords"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_PERSONS_AS_BYTES            = "persons"
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]                FAMILY_META_AS_BYTES               = "meta"
        .getBytes(Charset.forName("UTF-8"));

    protected Map<Long, PageDescription<byte[]>> currentDefaultLoadedPage           = new ConcurrentHashMap<>();

    static private class KeySet {
        byte[] key;
        Short  salt;

        public byte[] getKey() { return this.key; }

        public Short getSalt() { return this.salt; }

        KeySet(byte[] key) {
            this.key = key;
            this.salt = Bytes.toShort(key, 0);
        }
    }

    private int compare(KeySet a, KeySet b) { return UnsignedBytes.lexicographicalComparator()
        .compare(a.key, b.key); }

    public static byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    protected ReentrantLock lockOnLocalCache = new ReentrantLock();

    protected Optional<Long> findPageOf(Table pageTable, T2 t, HbaseImageThumbnail hbi) {
        // filter to get the first page which current date value is between
        // min and max
        final byte[] rowKeyToIndex = this.getRowKey(t, hbi);
        final long date = this.extractDateOfRowKeyOfTableSourceToIndex(hbi);
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractDAO.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractDAO.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterValue = new SingleColumnValueFilter(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY,
            rowKeyToIndex,
            CompareOperator.EQUAL,
            new BinaryComparator(AbstractDAO.TRUE_VALUE));
        Scan scan = new Scan().setFilter(new FilterList(filterMin2, filterMax, filterValue))
            .setLimit(1);
        try (
            ResultScanner rs = pageTable.getScanner(scan)) {
            return Streams.stream(rs)
                .map((f) -> this.extractPageNumber(f.getRow()))
                .findFirst();
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract byte[] getRowKey(T2 t, HbaseImageThumbnail hbi);

    protected long extractDateOfRowKeyOfTableSourceToIndex(HbaseImageThumbnail t) { return t.getCreationDate(); }

    private long extractPageNumber(byte[] row) { return Bytes.toLong(row); }

    @Override
    public Flux<HbaseImageThumbnail> getPrevious(T2 meta, HbaseImageThumbnail t) throws IOException {
        Flux<HbaseImageThumbnail> f = Flux.range(1, AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE)
            .parallel(AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1)
            .runOn(AbstractHbaseImagesOfMetadataDAO.THREAD_POOL_FOR_SCAN_PREV_PARALLEL)
            .map((k) -> this.build(k, meta, t))
            .flatMap((x) -> this.getPreviousThumbNailsOf(x, x.getRegionSalt() != t.getRegionSalt()))
            .sorted((a, b) -> HbaseImageThumbnail.compareForSorting(b, a))
            .take(1);
        return f;
    }

    @Override
    public Flux<HbaseImageThumbnail> getNext(T2 meta, HbaseImageThumbnail t) throws IOException {
        Flux<HbaseImageThumbnail> f = Flux.range(1, AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE)
            .parallel(AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE - 1)
            .runOn(AbstractHbaseImagesOfMetadataDAO.THREAD_POOL_FOR_SCAN_NEXT_PARALLEL)
            .map((k) -> this.build(k, meta, t))
            .flatMap((x) -> this.getNextThumbNailsOf(x, x.getRegionSalt() != t.getRegionSalt()))
            .sorted((a, b) -> HbaseImageThumbnail.compareForSorting(b, a))
            .take(1);
        return f;
    }

    protected Flux<byte[]> getFluxForPage(Table thumbTable, long pageNumberInTablePage, int pageSize) {
        try (
            Table pageTable = this.connection.getTable(
                TableName.valueOf(
                    this.getHbaseDataInformation()
                        .getNameSpace() + ":"
                        + this.getHbaseDataInformation()
                            .getPageTableName()))) {

            AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                "[HbaseImagesOfMetadataDAO]Create requested page nb {} with page size : {} ",
                pageNumberInTablePage,
                pageSize);
            Get get = new Get(AbstractDAO.convert(pageNumberInTablePage));

            final NavigableMap<byte[], byte[]> currentPageContent = pageTable.get(get)
                .getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY);
            return Flux.fromStream(
                currentPageContent.keySet()
                    .stream())
                .map(KeySet::new)
                .groupBy((c) -> c.getSalt())
                .flatMap(Flux::collectList)
                .flatMap((c) -> this.scan(thumbTable, c))
                .take(pageSize);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Flux<byte[]> scan(Table thumbTable, Collection<KeySet> keys) {
        try {
            List<KeySet> list = new ArrayList<>(keys);
            list.sort((a, b) -> this.compare(a, b));
            byte[] firstKeyToRetrieve = Arrays.copyOfRange(
                list.get(0)
                    .getKey(),
                8,
                list.get(0)
                    .getKey().length);
            byte[] lastKeyToRetrieve = Arrays.copyOfRange(
                list.get(list.size() - 1)
                    .getKey(),
                8,
                list.get(list.size() - 1)
                    .getKey().length);
            AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                "[HbaseImagesOfMetadataDAO]Scan first row is {}, last row is {}",
                Arrays.toString(firstKeyToRetrieve),
                Arrays.toString(lastKeyToRetrieve));
            Scan scan = this.createScanToGetAllColumnsWithoutImages()
                .withStartRow(firstKeyToRetrieve)
                .withStopRow(lastKeyToRetrieve, true);
            return Flux.fromIterable(thumbTable.getScanner(scan))
                .map((r) -> r.getRow());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected Scan createScanToGetAllColumnsWithoutImages() {
        Scan scan = new Scan();
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMG_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_SZ_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_TECH_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_ALBUMS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_KEYWORDS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_PERSONS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_RATINGS_BYTES);
        scan.addFamily(IImageThumbnailDAO.FAMILY_IMPORT_BYTES);
        return scan;
    }

    protected Flux<HbaseImageThumbnail> getNextThumbNailsOf(T key, boolean includeRow) {
        byte[] saltAsByte = new byte[2];
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Bytes.putAsShort(saltAsByte, 0, key.getRegionSalt());
            Scan scan = this.createScanToGetOnlyRowKey(new PrefixFilter(saltAsByte));
            scan.withStartRow(this.toHbaseKey(key), includeRow)
                .setLimit(1);
            ResultScanner rs = table.getScanner(scan);
            return Flux.fromIterable(rs)
                .map((t) -> this.toHbaseImageThumbnail(t.getRow()))
                .sort((a, b) -> a.compareTo(b));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected Flux<HbaseImageThumbnail> getPreviousThumbNailsOf(T key, boolean incluseRow) {
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            byte[] saltAsByte = new byte[2];
            Bytes.putAsShort(saltAsByte, 0, key.getRegionSalt());
            Scan scan = this.createScanToGetOnlyRowKey(new PrefixFilter(saltAsByte));
            scan.withStartRow(this.toHbaseKey(key), incluseRow)
                .setLimit(1)
                .setReversed(true);
            ResultScanner rs = table.getScanner(scan);
            return Flux.fromIterable(rs)
                .map((t) -> this.toHbaseImageThumbnail(t.getRow()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Scan createScanToGetOnlyRowKey(Filter prefixFilter) {
        Scan scan = new Scan();
        scan.setFilter(new FilterList(prefixFilter, new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
        return scan;
    }

    public long countNbOfPages() throws Throwable {
        try (
            AggregationClient ac = new AggregationClient(HBaseConfiguration.create())) {
            long retValue = ac
                .rowCount(this.hbaseDataInformation.getPageTable(), new LongColumnInterpreter(), new Scan());
            return retValue;
        }
    }

    protected static Scan createScanToGetAllColumns() {
        Scan scan = new Scan().addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_IMG_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_THB_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_SZ_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_TECH_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_ALBUMS_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_KEYWORDS_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_PERSONS_AS_BYTES)
            .addFamily(AbstractHbaseImagesOfMetadataDAO.FAMILY_META_AS_BYTES);
        return scan;
    }

    @Override
    public long countAll(T2 key) {
        byte[] rowKey = this.getRowKeyForMetaDataTable(key);
        Get get = new Get(rowKey);
        try (
            Table table = AbstractDAO.getTable(this.connection, this.getMetaDataTable())) {
            Result res = table.get(get);
            byte[] nbOfelements = res.getFamilyMap(AbstractHbaseImagesOfAlbumsDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY)
                .get(AbstractHbaseImagesOfAlbumsDAO.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS);
            return Bytes.toLong(nbOfelements);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    protected Flux<byte[]> getPageOfImagesAsFlux(
        String metaDataFamily,
        T2 metaData,
        Table pageTable,
        Table thumbTable,
        long pageNumberInTablePage
    ) {

        return Flux.range((int) pageNumberInTablePage, 1)
            .map((x) -> this.createGetQueryForTablePage(metaData, x))
            .flatMap((get) -> this.getRows(get, pageTable))
            .map(KeySet::new)
            .groupBy((key) -> key.getSalt())
            .flatMap(Flux::collectList)
            .map((x) -> this.toScan(x, metaData, metaDataFamily))
            .flatMap((x) -> this.toIterable(x, thumbTable))
            .map((x) -> x.getRow());

    }

    protected abstract Get createGetQueryForTablePage(T2 metaData, Integer x);

    private Flux<byte[]> getRows(Get get, Table pageTable) {
        try {
            final Result result = pageTable.get(get);
            if ((result != null) && (result.getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY) != null)) {
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                    "Found elements in get {} : nb of elements {}",
                    get,
                    result.getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY)
                        .size());
                return Flux.fromIterable(
                    result.getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY)
                        .keySet());
            } else {
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info("Found No elements in get {}", get);
                return Flux.empty();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Flux<Result> toIterable(Scan scan, Table thumbTable) {
        try {
            return Flux.fromIterable(thumbTable.getScanner(scan));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Scan toScan(List<KeySet> list, T2 metaData, String metaDataFamily) {
        list.sort((a, b) -> this.compare(a, b));
        byte[] firstKeyToRetrieve = Arrays.copyOfRange(
            list.get(0)
                .getKey(),
            this.getIndexOfImageRowKeyInTablePage(),
            list.get(0)
                .getKey().length);
        byte[] lastKeyToRetrieve = Arrays.copyOfRange(
            list.get(list.size() - 1)
                .getKey(),
            this.getIndexOfImageRowKeyInTablePage(),
            list.get(list.size() - 1)
                .getKey().length);
        final SingleColumnValueFilter filter = new SingleColumnValueFilter(
            metaDataFamily.getBytes(Charset.forName("UTF-8")),
            this.metadataColumnValuetoByte(metaData),
            CompareOperator.EQUAL,
            new BinaryComparator(new byte[] { 1 }));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
            "[HBASE_IMG_THUMBNAIL_DAO]Scan first row is {}/{}, last row is {}/{} - filter is {}",
            Arrays.toString(firstKeyToRetrieve),
            new String(firstKeyToRetrieve),
            Arrays.toString(lastKeyToRetrieve),
            new String(lastKeyToRetrieve),
            filter);
        Scan scan = this.createScanToGetAllColumnsWithoutImages()
            .setFilter(filter)
            .withStartRow(firstKeyToRetrieve)
            .withStopRow(lastKeyToRetrieve, true);
        return scan;
    }

    protected byte[] metadataColumnValuetoByte(T2 metaData) {
        return metaData.toString()
            .getBytes(Charset.forName("UTF-8"));
    }

    protected PageDescription<byte[]> loadPageInTablePage(
        Table pageTable,
        Table thumbTable,
        long pageNumberInTablePage
    ) {
        AbstractHbaseImagesOfMetadataDAO.LOGGER
            .info(" createPageDescription currentPageNumber = {},pageSize {} ", pageNumberInTablePage);

        try {

            AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                "[HBASE_IMG_THUMBNAIL_DAO]Create requested page nb {} with page size : {} ",
                pageNumberInTablePage);
            Get get = new Get(AbstractDAO.convert(pageNumberInTablePage));

            final NavigableMap<byte[], byte[]> currentPageContent = pageTable.get(get)
                .getFamilyMap(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY);

            Map<Short, List<KeySet>> keys = currentPageContent.keySet()
                .stream()
                .map(KeySet::new)
                .collect(Collectors.groupingBy((c) -> c.getSalt()));

            currentPageContent.keySet()
                .stream()
                .map(
                    (c) -> new String(c,
                        (2 * ModelConstants.FIXED_WIDTH_CREATION_DATE) + ModelConstants.FIXED_WIDTH_SHORT,
                        ModelConstants.FIXED_WIDTH_IMAGE_ID))
                .forEach((c) -> AbstractHbaseImagesOfMetadataDAO.LOGGER.info(".. Found image id {} ", c));
            PageDescription<byte[]> retValue = new PageDescription<>(pageNumberInTablePage, new ArrayList<>());
            for (short salt : keys.keySet()) {
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info("[HBASE_IMG_THUMBNAIL_DAO] Building scan {} ", salt);
                final List<KeySet> list = keys.get(salt);
                list.sort((a, b) -> this.compare(a, b));
                byte[] firstKeyToRetrieve = Arrays.copyOfRange(
                    list.get(0)
                        .getKey(),
                    this.getIndexOfImageRowKeyInTablePage(),
                    list.get(0)
                        .getKey().length);
                byte[] lastKeyToRetrieve = Arrays.copyOfRange(
                    list.get(list.size() - 1)
                        .getKey(),
                    this.getIndexOfImageRowKeyInTablePage(),
                    list.get(list.size() - 1)
                        .getKey().length);
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                    "[HBASE_IMG_THUMBNAIL_DAO]Scan first row is {}, last row is {}",
                    Arrays.toString(firstKeyToRetrieve),
                    Arrays.toString(lastKeyToRetrieve));
                Scan scan = this.createScanToGetAllColumnsWithoutImages()
                    .withStartRow(firstKeyToRetrieve)
                    .withStopRow(lastKeyToRetrieve, true);
                List<byte[]> pageContent = StreamSupport.stream(
                    thumbTable.getScanner(scan)
                        .spliterator(),
                    false)
                    .map((r) -> r.getRow())
                    .collect(Collectors.toList());
                retValue.getPageContent()
                    .addAll(pageContent);
                AbstractHbaseImagesOfMetadataDAO.LOGGER
                    .info("[HBASE_IMG_THUMBNAIL_DAO] end of Building scan {} ", salt);
            }
            // retValue.sort((o1, o2) -> o1.compareTo(o2));
            AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                "[HBASE_IMG_THUMBNAIL_DAO] end of load page {} - nb of found elements {}",
                pageNumberInTablePage,
                retValue.getPageSize());
            return retValue;
        } catch (IOException e) {
            AbstractHbaseImagesOfMetadataDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    protected Flux<byte[]> buildPage(int pageSize, int pageNumber, int countAll, Table pageTable, Table thumbTable) {
        if (pageNumber > 0) {
            AbstractHbaseImagesOfMetadataDAO.LOGGER
                .info("[HBASE_IMG_THUMBNAIL_DAO]build page size is {}, number is {} ", pageSize, pageNumber);
            try {
                long pageNumberInTablePage = ((pageNumber - 1) * pageSize) / IImageThumbnailDAO.PAGE_SIZE;
                long initialIndex = ((pageNumber - 1) * pageSize)
                    - (pageNumberInTablePage * IImageThumbnailDAO.PAGE_SIZE);
                PageDescription<byte[]> pageDescription = this.currentDefaultLoadedPage.computeIfAbsent(
                    pageNumberInTablePage,
                    (c) -> this.loadPageInTablePage(pageTable, thumbTable, pageNumberInTablePage));

                long lastIndex = Math.min(pageDescription.getPageSize(), initialIndex + pageSize);
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                    "[HBASE_IMG_THUMBNAIL_DAO]build page size is {}, number is {} - initial index {} - last index {}",
                    pageSize,
                    pageNumber,
                    initialIndex,
                    lastIndex);
                if (((lastIndex - initialIndex) + 1) < pageSize) {
                    this.currentDefaultLoadedPage.computeIfAbsent(
                        pageNumberInTablePage + 1,
                        (c) -> this.loadPageInTablePage(pageTable, thumbTable, pageNumberInTablePage));
                }

                return Flux.range((int) pageNumberInTablePage, 2)
                    .flatMap(
                        (x) -> Flux.fromIterable(
                            this.currentDefaultLoadedPage.get(x)
                                .getPageContent()))
                    .take(pageSize);

            } catch (Exception e) {
                AbstractHbaseImagesOfMetadataDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
        } else {
            return Flux.empty();
        }
    }

    protected Flux<byte[]> buildPageAsflux(
        String metaDataFamily,
        T2 metaData,
        int pageSize,
        int pageNumber,
        Table pageTable,
        Table thumbTable
    ) {
        if (pageNumber > 0) {

            try {
                long pageNumberInTablePage = ((pageNumber - 1) * pageSize) / IImageThumbnailDAO.PAGE_SIZE;
                long initialIndex = ((pageNumber - 1) * pageSize)
                    - (pageNumberInTablePage * IImageThumbnailDAO.PAGE_SIZE);
                AbstractHbaseImagesOfMetadataDAO.LOGGER.info(
                    "[HBASE_IMG_THUMBNAIL_DAO]build page size is {}, number is {} - initialIndex {}",
                    pageSize,
                    pageNumber,
                    initialIndex);
                return Flux.concat(
                    this.getPageOfImagesAsFlux(metaDataFamily, metaData, pageTable, thumbTable, pageNumberInTablePage),
                    this.getPageOfImagesAsFlux(
                        metaDataFamily,
                        metaData,
                        pageTable,
                        thumbTable,
                        pageNumberInTablePage + 1))
                    .skip(initialIndex)
                    .take(pageSize);
            } catch (Exception e) {
                AbstractHbaseImagesOfMetadataDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
        } else {
            return Flux.empty();
        }
    }

    /*
     * Abstract methods
     *
     */
    protected abstract int getIndexOfImageRowKeyInTablePage();

    protected abstract TableName getMetaDataTable();

    protected abstract byte[] getRowKeyForMetaDataTable(T2 key);

    protected abstract HbaseImageThumbnail toHbaseImageThumbnail(byte[] rowKey);

    protected abstract T build(Integer salt, T2 meta, HbaseImageThumbnail t);

    protected abstract byte[] toHbaseKey(T him);

    protected AbstractHbaseImagesOfMetadataDAO(
        Connection connection,
        String nameSpace
    ) {
        super(connection,
            nameSpace);
    }

}
