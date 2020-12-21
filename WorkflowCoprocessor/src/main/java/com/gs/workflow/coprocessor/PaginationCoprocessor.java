package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.workflow.coprocessor.DateTimeHelper.KeyEnumType;

public class PaginationCoprocessor implements RegionCoprocessor, RegionObserver {

    protected static Logger       LOGGER                              = LoggerFactory
        .getLogger(PaginationCoprocessor.class);
    protected static final long   PAGE_SIZE                           = 1000L;
    protected static final String TABLE_SOURCE                        = "image_thumbnail";
    protected static final String TABLE_IMAGE_THUMBNAIL_KEY           = "image_thumbnail_key";
    protected static final String TABLE_PAGE                          = "page_image_thumbnail";
    protected static final byte[] TABLE_PAGE_DESC_COLUMN_FAMILY       = "max_min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER = "max".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER = "min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_LIST_COLUMN_FAMILY       = "list".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements"
        .getBytes(Charset.forName("UTF-8"));
    protected static final String COLUMN_STAT_NAME                    = "stats";
    protected static final String FAMILY_IMGS_NAME                    = "imgs";
    protected static final String FAMILY_STATS_NAME                   = "fstats";
    protected static final byte[] COLUMN_STAT_AS_BYTES                = PaginationCoprocessor.COLUMN_STAT_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_IMGS_NAME_AS_BYTES           = PaginationCoprocessor.FAMILY_IMGS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_STATS_NAME_AS_BYTES          = PaginationCoprocessor.FAMILY_STATS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TRUE_VALUE                          = new byte[] { 1 };
    protected static final byte[] TABLE_SOURCE_THUMBNAIL              = "thb".getBytes(Charset.forName("UTF-8"));

    protected Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }

    protected Connection                   hbaseConnection;
    protected Map<String, BufferedMutator> bufferedMutatorOfEnvs = new ConcurrentHashMap<>();

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        RegionCoprocessor.super.start(env);
        this.hbaseConnection = this.hbaseConnection();
        PaginationCoprocessor.LOGGER.info("STARTING Coprocessor..." + this);
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() { return Optional.of(this); }

    protected BufferedMutator getBufferedMutator(String nameSpace) {
        return this.bufferedMutatorOfEnvs.computeIfAbsent(nameSpace, (key) -> {
            try {
                return this.hbaseConnection.getBufferedMutator(
                    new BufferedMutatorParams(
                        TableName.valueOf(key + ":" + PaginationCoprocessor.TABLE_IMAGE_THUMBNAIL_KEY))
                            .writeBufferSize(100 * 1024));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void incrementDateInterval(String namespace, byte[] imageRow) throws IOException {
        BufferedMutator bufferedMutator = this.getBufferedMutator(namespace);
        long date = Bytes.toLong(imageRow);
        List<Mutation> mutationsList = this.splitCreationDateToYearMonthDayAndHour(date)
            .stream()
            .flatMap((d) -> {
                Put imageInfo = new Put(DateTimeHelper.toBytes(d)).addColumn(
                    PaginationCoprocessor.FAMILY_IMGS_NAME_AS_BYTES,
                    imageRow,
                    PaginationCoprocessor.TRUE_VALUE);
                Increment inc = new Increment(DateTimeHelper.toBytes(d)).addColumn(
                    PaginationCoprocessor.FAMILY_STATS_NAME_AS_BYTES,
                    PaginationCoprocessor.COLUMN_STAT_AS_BYTES,
                    1);
                return Stream.of(imageInfo, inc);
            })
            .collect(Collectors.toList());
        bufferedMutator.mutate(mutationsList);
        bufferedMutator.flush();
    }

    protected Collection<String> splitCreationDateToYearMonthDayAndHour(long date) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(date);
        Map<KeyEnumType, String> keys = DateTimeHelper.toKey(ldt, KeyEnumType.ALL);
        keys.remove(KeyEnumType.ALL);
        return keys.values();
    }

    protected long createSecundaryIndex(Region region, String namespace, byte[] row) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + PaginationCoprocessor.TABLE_PAGE))) {
            long date = this.getDateOfRow(row);

            long pageNumber = this.findPage(tablePage, date);
            Put put = new Put(PaginationCoprocessor.convert(pageNumber));
            put.addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, row, new byte[] { 1 });
            tablePage.put(put);
            return pageNumber;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void updateTablePage(Region region, String namespace, byte[] currentPageRow) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + PaginationCoprocessor.TABLE_PAGE))) {

            RowLock rowLock = null;
            try {
                rowLock = region != null ? region.getRowLock(currentPageRow, false) : null;

                Get get = new Get(currentPageRow).addFamily(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY)
                    .addFamily(PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY)
                    .addFamily(PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY);

                Result result = region.get(get);
                final NavigableMap<byte[], byte[]> familyMap = result
                    .getFamilyMap(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                long recordedSize = Bytes.toLong(
                    CellUtil.cloneValue(
                        result.getColumnLatestCell(
                            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS)));
                if (familyMap.size() == 0) {
                    PaginationCoprocessor.LOGGER.warn(
                        "updateTablePage, for row {} no elements in list family  ",
                        this.toHexString(currentPageRow));
                }
                byte[] firstRow = familyMap.firstKey();
                byte[] lastRow = familyMap.lastKey();
                long min = Bytes.toLong(firstRow);
                long max = Bytes.toLong(lastRow);
                PaginationCoprocessor.LOGGER.info(
                    " Min {} and max {} found in current page : {} - nb Of elements in page {}, recorded size {}",
                    min,
                    max,
                    Bytes.toLong(currentPageRow),
                    familyMap.size(),
                    recordedSize);

                if (PaginationCoprocessor.LOGGER.isDebugEnabled()) {
                    PaginationCoprocessor.LOGGER.debug(
                        "updateTablePage, check current page row {} - {} - nb of elements : {}  ",
                        this.toHexString(currentPageRow),
                        Bytes.toLong(currentPageRow),
                        familyMap.size());
                }
                while (familyMap.size() > PaginationCoprocessor.PAGE_SIZE) {
                    lastRow = familyMap.lastKey();
                    PaginationCoprocessor.LOGGER.info(
                        "Page is full, currentPageRow is {},  found last row {} - {}",
                        this.toHexString(currentPageRow),
                        Bytes.toLong(lastRow),
                        this.toHexString(lastRow));
                    Delete del = new Delete(currentPageRow)
                        .addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, lastRow);
                    familyMap.remove(lastRow);
                    Put put1 = new Put(currentPageRow)
                        .addColumn(
                            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                            PaginationCoprocessor.convert(max))
                        .addColumn(
                            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                            PaginationCoprocessor.convert(min))
                        .addColumn(
                            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                            PaginationCoprocessor.convert((long) familyMap.size()));
                    region.delete(del);
                    region.put(put1);
                    PaginationCoprocessor.LOGGER.info(
                        "Region table has been updated, now reinsert found last row {} - {}",
                        Bytes.toLong(lastRow),
                        this.toHexString(lastRow));
                    Long pageNumberToReinsert = this.findPage(tablePage, Bytes.toLong(lastRow));
                    if (pageNumberToReinsert == Bytes.toLong(currentPageRow)) {
                        PaginationCoprocessor.LOGGER.error(
                            "Page number is the current page row, where reinsert should be done {} - {}, ignoring",
                            this.toHexString(lastRow),
                            pageNumberToReinsert);

                    } else {
                        PaginationCoprocessor.LOGGER.info(
                            "Page number where reinsert should be done {} - {}",
                            this.toHexString(lastRow),
                            pageNumberToReinsert);
                        this.recordInPageTable(tablePage, lastRow, pageNumberToReinsert);
                    }
                    max = Bytes.toLong(familyMap.lastKey());

                }
                if (rowLock != null) {
                    rowLock.release();
                    rowLock = null;
                }
                this.updateMinAndMaxInPageTable(currentPageRow, max, min, familyMap.size(), region);

            } finally {
                if (rowLock != null) {
                    rowLock.release();
                }
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            PaginationCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    private void recordInPageTable(Table tablePage, byte[] row, Long pageNumber) throws IOException {
        Put put = new Put(PaginationCoprocessor.convert(pageNumber));
        put.addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, row, new byte[] { 1 });
        tablePage.put(put);
        PaginationCoprocessor.LOGGER.info("End of recordInPageTable {} in page {}", this.toHexString(row), pageNumber);
    }

    protected void updateMinAndMaxInPageTable(byte[] row, long currentMax, long currentMin, long size, Region region)
        throws IOException {
        Put put1 = new Put(row).addColumn(
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            PaginationCoprocessor.convert(size));
        region.checkAndRowMutate(
            row,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.GREATER,
            new LongComparator(size),
            RowMutations.of(Arrays.asList(put1)));

        put1 = new Put(row).addColumn(
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            PaginationCoprocessor.convert(currentMax));
        region.checkAndRowMutate(
            row,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(currentMax),
            RowMutations.of(Arrays.asList(put1)));

        put1 = new Put(row).addColumn(
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            PaginationCoprocessor.convert(currentMin));

        region.checkAndRowMutate(
            row,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS,
            new LongComparator(currentMin),
            RowMutations.of(Arrays.asList(put1)));
        PaginationCoprocessor.LOGGER.info(
            "End of updateMinAndMaxInPageTable current min is {} current max is {}, find size is {}, row is {}",
            currentMin,
            currentMax,
            size,
            this.toHexString(row));

    }

    protected void incPageSize(Table table, long pageNumber) throws IOException {
        Increment inc = new Increment(PaginationCoprocessor.convert(pageNumber)).addColumn(
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            1);
        table.increment(inc);
    }

    protected String toHexString(byte[] row) {
        StringBuilder strBuilder = new StringBuilder();
        for (byte b : row) {
            strBuilder.append(Integer.toHexString((b & 0x000000ff)));
        }
        return strBuilder.toString();
    }

    protected byte[] getLastRow(Table table, long pageNumber) throws IOException {
        Get get = new Get(PaginationCoprocessor.convert(pageNumber));
        get.addFamily(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
        Result res = table.get(get);
        NavigableMap<byte[], byte[]> elements = res.getFamilyMap(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
        if (elements != null) { return elements.lastEntry()
            .getKey(); }

        return null;
    }

    protected long findPage(Table table, long date) {
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;

        // filter to get the first page which is not full
        SingleColumnValueFilter filterMaxNbOfElements = new SingleColumnValueFilter(
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.LESS,
            new LongComparator(PaginationCoprocessor.PAGE_SIZE));

        // filter to get the first page which current min value is greater than the
        // fiven date
        SingleColumnValueFilter filterMin = new SingleColumnValueFilter(
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(date));

        // filter to get the first page which current date value is between
        // min and max
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(date));

        Scan scan = new Scan()
            .setFilter(
                new FilterList(FilterList.Operator.MUST_PASS_ONE,
                    Arrays.asList(filterMaxNbOfElements, filterMin, new FilterList(filterMin2, filterMax))))
            .setLimit(1);
        try (

            ResultScanner rs = table.getScanner(scan)) {
            Result res = rs.next();
            if (res != null) {
                pageNumber = Bytes.toLong(res.getRow());
                pageIsFound = true;
                PaginationCoprocessor.LOGGER.info("found page {}, for date {}", pageNumber, date);
            } else {
                try {
                    Get get = new Get(PaginationCoprocessor.convert(0L));
                    Result res1 = table.get(get);
                    byte[] minValue = res1.getValue(
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER);
                    byte[] maxValue = res1.getValue(
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER);
                    PaginationCoprocessor.LOGGER.warn(
                        "Min/Max Values of page 0 is {},{}, Nothnig found for date {}, should create a new one",
                        Bytes.toLong(minValue),
                        Bytes.toLong(maxValue),
                        date);
                } catch (Exception e) {
                    PaginationCoprocessor.LOGGER.warn("Error ", e);
                }
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }

        if (!pageIsFound) {
            PaginationCoprocessor.LOGGER.info("Nothnig found for date {}, creating another page", date);
            scan = new Scan().setLimit(1)
                .setReversed(true);
            try (
                ResultScanner rs = table.getScanner(scan)) {
                Result res = rs.next();
                if (res != null) {
                    pageNumber = Bytes.toLong(res.getRow()) + 1;
                } else {
                    pageNumber = 0;
                }
                Put put = new Put(PaginationCoprocessor.convert(pageNumber))
                    .addColumn(
                        PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                        PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                        PaginationCoprocessor.convert(0l))
                    .addColumn(
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                        PaginationCoprocessor.convert(0L))
                    .addColumn(
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                        PaginationCoprocessor.convert(Long.MAX_VALUE));
                table.put(put);
                PaginationCoprocessor.LOGGER.info("Creating page {} pageNumber for date {}", pageNumber, date);
            } catch (IOException e) {
                PaginationCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }

        }
        return pageNumber;
    }

    private long getDateOfRow(byte[] row) { return Bytes.toLong(row, 0, 8); }

    private static byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    @Override
    public void prePut(
        ObserverContext<RegionCoprocessorEnvironment> observerContext,
        Put put,
        WALEdit edit,
        Durability durability
    ) throws IOException {
        this.processPreMutation(observerContext, put);
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        PaginationCoprocessor.LOGGER.info("[PAGINATION]Pre append detected");
        this.processPreMutation(c, append);
        return null;
    }

    protected void processPreMutation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation put) {
        final TableName table2 = observerContext.getEnvironment()
            .getRegion()
            .getRegionInfo()
            .getTable();
        String table = table2.getNameAsString();

        if (table.endsWith(":" + PaginationCoprocessor.TABLE_SOURCE)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(PaginationCoprocessor.TABLE_SOURCE_THUMBNAIL);
            if ((cells != null) && (cells.size() > 0)) {
                cells.forEach((c) -> {
                    Integer key = Integer.parseInt(new String(CellUtil.cloneQualifier(c)));
                    if (key == 1) {
                        PaginationCoprocessor.LOGGER.info(
                            "[PAGINATION]Creating secundary index for table {} - namespace is {} - row is {} ",
                            table2,
                            table2.getNamespaceAsString(),
                            new String(put.getRow(), 8, 64, Charset.forName("UTF-8")));
                        this.createSecundaryIndex(
                            observerContext.getEnvironment()
                                .getRegion(),
                            table2.getNamespaceAsString(),
                            put.getRow());
                    }
                });
            } else {
                PaginationCoprocessor.LOGGER
                    .warn("Unable to find some thumbnail for {} ", this.toHexString(put.getRow()));
            }
        }
    }

    @Override
    public void postPut(
        ObserverContext<RegionCoprocessorEnvironment> observerContext,
        Put put,
        WALEdit edit,
        Durability durability
    ) throws IOException {
        this.processPostMutation(observerContext, put);
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result)
        throws IOException {
        PaginationCoprocessor.LOGGER.info("[PAGINATION]Post append detected");
        this.processPostMutation(c, append);
        return null;
    }

    protected void processPostMutation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation put) {
        final TableName table2 = observerContext.getEnvironment()
            .getRegion()
            .getRegionInfo()
            .getTable();
        String table = table2.getNameAsString();
        if (table.endsWith(":" + PaginationCoprocessor.TABLE_PAGE)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
            if (cells != null) {
                PaginationCoprocessor.LOGGER
                    .info("PostPut, cells are {}, {}, row is {} ", table, cells, this.toHexString(put.getRow()));

                this.updateTablePage(
                    observerContext.getEnvironment()
                        .getRegion(),
                    table2.getNamespaceAsString(),
                    put.getRow());
            }
        } else if (table.endsWith(":" + PaginationCoprocessor.TABLE_SOURCE)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(PaginationCoprocessor.TABLE_SOURCE_THUMBNAIL);
            if ((cells != null) && (cells.size() > 0)) {
                cells.forEach((c) -> {
                    Integer key = Integer.parseInt(new String(CellUtil.cloneQualifier(c)));
                    if (key == 1) {
                        PaginationCoprocessor.LOGGER.info(
                            "Incrementing nb of elements for table {} - namespace is {} - row is {} ",
                            table2,
                            table2.getNamespaceAsString(),
                            new String(put.getRow(), 8, 64, Charset.forName("UTF-8")));
                        try {
                            this.incrementDateInterval(table2.getNamespaceAsString(), put.getRow());
                        } catch (IOException e) {
                            PaginationCoprocessor.LOGGER.warn("Error ", e);
                        }
                    }
                });

            }
        }
    }

}