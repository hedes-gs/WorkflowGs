package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
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
    public static final int       FIXED_WIDTH_IMAGE_ID                = 64;
    public static final int       FIXED_WIDTH_REGION_SALT             = 2;
    public static final int       FIXED_WIDTH_CREATION_DATE           = 8;

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
        long date = this.getDateOfRowOfTableSource(imageRow);
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

    protected long createSecundaryIndexInPagedTable(Region region, String namespace, byte[] row) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + PaginationCoprocessor.TABLE_PAGE))) {
            long date = this.getDateOfRowOfTableSource(row);
            long pageNumber = this.findPage(tablePage, date);
            PaginationCoprocessor.LOGGER
                .info("[PAGINATION] createSecundaryIndexInPagedTable page is {}, date is {} ", pageNumber, date);
            this.recordOfRowOfSourceInPageTable(tablePage, row, pageNumber);
            PaginationCoprocessor.LOGGER
                .info("[PAGINATION] end of createSecundaryIndex page is {}, date is {} ", pageNumber, date);
            return pageNumber;
        } catch (IOException e) {
            PaginationCoprocessor.LOGGER.error(
                "[PAGINATION]Unable to create secundary index for row {}, exception is {} ",
                this.toHexString(row),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    protected void updateTablePage(Region region, String namespace, byte[] currentPageRow) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + PaginationCoprocessor.TABLE_PAGE))) {

            RowLock rowLock = null;
            try {
                PaginationCoprocessor.LOGGER.info(
                    "[PAGINATION]Trying to get lock for {}:{}",
                    PaginationCoprocessor.TABLE_PAGE,
                    this.toHexString(currentPageRow));
                rowLock = region != null ? region.getRowLock(currentPageRow, false) : null;
                PaginationCoprocessor.LOGGER.info(
                    "[PAGINATION]Got lock for {}:{}",
                    PaginationCoprocessor.TABLE_PAGE,
                    this.toHexString(currentPageRow));
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
                        "[PAGINATION]updateTablePage, for row {} no elements in list family  ",
                        this.toHexString(currentPageRow));
                }
                byte[] firstRow = familyMap.firstKey();
                byte[] lastRow = familyMap.lastKey();
                long min = this.getDateOfRowInsertedOfTablePage(firstRow);
                long max = this.getDateOfRowInsertedOfTablePage(lastRow);
                PaginationCoprocessor.LOGGER.info(
                    "[PAGINATION]Min {} and max {} found in current page : {} - nb Of elements in page {}, recorded size {}",
                    min,
                    max,
                    Bytes.toLong(currentPageRow),
                    familyMap.size(),
                    recordedSize);
                long expextedPageNumberToReinsert = Bytes.toLong(currentPageRow) + 1;
                List<Put> putListOf = new ArrayList<>();
                while (familyMap.size() > PaginationCoprocessor.PAGE_SIZE) {
                    lastRow = familyMap.lastKey();
                    PaginationCoprocessor.LOGGER.info(
                        "[PAGINATION]Page is full, currentPageRow is {},  found last row {} - {}",
                        this.toHexString(currentPageRow),
                        this.getDateOfRowInsertedOfTablePage(lastRow),
                        this.toHexString(lastRow));
                    Delete del = new Delete(currentPageRow)
                        .addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, lastRow);
                    familyMap.remove(lastRow);
                    max = this.getDateOfRowInsertedOfTablePage(familyMap.lastKey());
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
                        "[PAGINATION]Region table has been updated, now reinsert in page {}, found last row {} - {}",
                        expextedPageNumberToReinsert,
                        this.getDateOfRowInsertedOfTablePage(lastRow),
                        this.toHexString(lastRow));

                    Long pageNumberToReinsert = expextedPageNumberToReinsert; // this.findPage(tablePage,
                                                                              // this.getDateOfRowInsertedOfTablePage(lastRow));
                    /*
                     * if (pageNumberToReinsert <= Bytes.toLong(currentPageRow)) {
                     * PaginationCoprocessor.LOGGER.error(
                     * "[PAGINATION]Page number is the current page row, where reinsert should be done, expected page is {} , found page is {}, row is  {}, ignoring"
                     * , expextedPageNumberToReinsert, pageNumberToReinsert,
                     * this.toHexString(lastRow)); } else if (pageNumberToReinsert >=
                     * expextedPageNumberToReinsert) { if (pageNumberToReinsert >
                     * expextedPageNumberToReinsert) { PaginationCoprocessor.LOGGER.warn(
                     * "[PAGINATION]Expected page where reinsert should be done is {} - found page is {}"
                     * , expextedPageNumberToReinsert, pageNumberToReinsert);
                     *
                     * } this.recordOfPreviouslyInsertedRowInPageTable(tablePage, lastRow,
                     * pageNumberToReinsert); }
                     */
                    putListOf.add(this.buildPutToRecordInNextPage(tablePage, lastRow, pageNumberToReinsert));
                    max = this.getDateOfRowInsertedOfTablePage(familyMap.lastKey());
                }

                this.updateMinAndMaxInPageTable(currentPageRow, max, min, familyMap.size(), region);
                if (rowLock != null) {
                    PaginationCoprocessor.LOGGER.info(
                        "[PAGINATION]Releasing  lock for {}:{}",
                        PaginationCoprocessor.TABLE_PAGE,
                        this.toHexString(currentPageRow));
                    rowLock.release();
                    rowLock = null;
                }
                // -> We break the transaction here :(
                tablePage.put(putListOf);
            } finally {
                if (rowLock != null) {
                    PaginationCoprocessor.LOGGER.info(
                        "[PAGINATION]Releasing lock for {}:{}",
                        PaginationCoprocessor.TABLE_PAGE,
                        this.toHexString(currentPageRow));
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

    private void recordOfRowOfSourceInPageTable(Table tablePage, byte[] row, Long pageNumber) throws IOException {
        PaginationCoprocessor.LOGGER
            .info("[PAGINATION]Start of initial record in page table {} in page {}", this.toHexString(row), pageNumber);
        Put put = new Put(PaginationCoprocessor.convert(pageNumber));
        long date = this.getDateOfRowOfTableSource(row);
        byte[] newRowOfPageTable = new byte[row.length + 8];
        System.arraycopy(row, 0, newRowOfPageTable, 8, row.length);
        final byte[] converedDate = PaginationCoprocessor.convert(date);
        System.arraycopy(converedDate, 0, newRowOfPageTable, 0, converedDate.length);
        put.addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, newRowOfPageTable, new byte[] { 1 });
        tablePage.put(put);
        PaginationCoprocessor.LOGGER
            .info("[PAGINATION]End of recordInPageTable {} in page {}", this.toHexString(row), pageNumber);
    }

    private Put buildPutToRecordInNextPage(Table tablePage, byte[] row, Long pageNumber) throws IOException {
        PaginationCoprocessor.LOGGER
            .info("[PAGINATION]Start of update of page {} -  Row is {}", pageNumber, this.toHexString(row));
        Put put = new Put(PaginationCoprocessor.convert(pageNumber));
        put.addColumn(PaginationCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, row, new byte[] { 1 });
        PaginationCoprocessor.LOGGER
            .info("[PAGINATION]End of update of page {} -  Row is {}", pageNumber, this.toHexString(row));
        return put;
    }

    protected void updateMinAndMaxInPageTable(byte[] row, long currentMax, long currentMin, long size, Region region)
        throws IOException {
        Put put1 = new Put(row).addColumn(
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            PaginationCoprocessor.convert(size));
        boolean done = region.checkAndRowMutate(
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
        done = done && region.checkAndRowMutate(
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

        done = done && region.checkAndRowMutate(
            row,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            PaginationCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS,
            new LongComparator(currentMin),
            RowMutations.of(Arrays.asList(put1)));
        PaginationCoprocessor.LOGGER.info(
            "[PAGINATION]End of updateMinAndMaxInPageTable current min is {} current max is {}, find size is {}, row is {} - updated is {}",
            currentMin,
            currentMax,
            size,
            this.toHexString(row),
            done);

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
                PaginationCoprocessor.LOGGER.info("[PAGINATION]found page {}, for date {}", pageNumber, date);
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }

        if (!pageIsFound) {
            PaginationCoprocessor.LOGGER.info("[PAGINATION]Nothnig found for date {}, creating another page", date);
            scan = new Scan().setLimit(1)
                .setReversed(true);
            try (
                ResultScanner rs = table.getScanner(scan)) {
                Result res = rs.next();
                if (res != null) {
                    pageNumber = Bytes.toLong(res.getRow()) + 1;
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
                } else {
                    throw new RuntimeException("Scan returned nothnig... Stopping !!!");
                }
            } catch (IOException e) {
                PaginationCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }

        }
        return pageNumber;
    }

    private long getDateOfRowOfTableSource(byte[] row) {
        return Bytes.toLong(row, PaginationCoprocessor.FIXED_WIDTH_REGION_SALT, 8);
    }

    private long getDateOfRowInsertedOfTablePage(byte[] row) { return Bytes.toLong(row, 0, 8); }

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
                            new String(put.getRow(),
                                PaginationCoprocessor.FIXED_WIDTH_CREATION_DATE
                                    + PaginationCoprocessor.FIXED_WIDTH_REGION_SALT,
                                64,
                                Charset.forName("UTF-8")));
                        this.createSecundaryIndexInPagedTable(
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
                PaginationCoprocessor.LOGGER.info(
                    "[PAGINATION]PostPut, cells are {}, {}, row is {} ",
                    table,
                    cells,
                    this.toHexString(put.getRow()));

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