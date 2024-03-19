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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.gs.workflow.coprocessor.DateTimeHelper.KeyEnumType;
import com.gs.workflow.coprocessor.LockTableHelper.LockRequest;
import com.gs.workflow.coprocessor.LockTableHelper.LockResponse;

public class ImagesPageCoprocessor extends AbstractProcessor implements RegionCoprocessor, RegionObserver {

    // Page table is organized as following :
    // | 0 - 7 | 8 -> 8 + row length |
    // | Creation date | <Row of hbase image>

    protected static Logger         LOGGER                              = LoggerFactory
        .getLogger(ImagesPageCoprocessor.class);
    protected static final long     PAGE_SIZE                           = 1000L;
    protected static final String   TABLE_SOURCE                        = "image_thumbnail";
    protected static final String   TABLE_IMAGE_THUMBNAIL_KEY           = "image_thumbnail_key";
    protected static final String   TABLE_PAGE                          = "page_image_thumbnail";
    protected static final byte[]   TABLE_PAGE_DESC_COLUMN_FAMILY       = "max_min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER = "max".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER = "min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TABLE_PAGE_LIST_COLUMN_FAMILY       = "list".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements"
        .getBytes(Charset.forName("UTF-8"));
    protected static final String   COLUMN_STAT_NAME                    = "stats";
    protected static final String   FAMILY_IMGS_NAME                    = "imgs";
    protected static final String   FAMILY_STATS_NAME                   = "fstats";
    protected static final byte[]   COLUMN_STAT_AS_BYTES                = ImagesPageCoprocessor.COLUMN_STAT_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   FAMILY_IMGS_NAME_AS_BYTES           = ImagesPageCoprocessor.FAMILY_IMGS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   FAMILY_STATS_NAME_AS_BYTES          = ImagesPageCoprocessor.FAMILY_STATS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]   TRUE_VALUE                          = new byte[] { 1 };
    protected static final byte[]   TABLE_SOURCE_THUMBNAIL              = "thb".getBytes(Charset.forName("UTF-8"));
    public static final int         FIXED_WIDTH_IMAGE_ID                = 64;
    public static final int         FIXED_WIDTH_REGION_SALT             = 2;
    public static final int         FIXED_WIDTH_CREATION_DATE           = 8;
    public static final Set<String> SET_OF_METADATA_FAMILY              = Set
        .of("albums", "keyWords", "persons", "ratings");

    protected Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }

    protected Connection                   hbaseConnection;
    protected Map<String, BufferedMutator> bufferedMutatorOfEnvs = new ConcurrentHashMap<>();

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        RegionCoprocessor.super.start(env);
        this.hbaseConnection = this.hbaseConnection();
        ImagesPageCoprocessor.LOGGER.info("STARTING Coprocessor..." + this);
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() { return Optional.of(this); }

    protected BufferedMutator getBufferedMutator(String nameSpace) {
        return this.bufferedMutatorOfEnvs.computeIfAbsent(nameSpace, (key) -> {
            try {
                return this.hbaseConnection.getBufferedMutator(
                    new BufferedMutatorParams(
                        TableName.valueOf(key + ":" + ImagesPageCoprocessor.TABLE_IMAGE_THUMBNAIL_KEY))
                            .writeBufferSize(100 * 1024));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected Collection<String> splitCreationDateToYearMonthDayAndHour(long date) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(date);
        Map<KeyEnumType, String> keys = DateTimeHelper.toKey(ldt, KeyEnumType.ALL);
        keys.remove(KeyEnumType.ALL);
        return keys.values();
    }

    protected void incrementDateInterval(String namespace, byte[] imageRow) throws IOException {
        BufferedMutator bufferedMutator = this.getBufferedMutator(namespace);
        long date = this.getDateOfRowOfTableSource(imageRow);
        List<Mutation> mutationsList = this.splitCreationDateToYearMonthDayAndHour(date)
            .stream()
            .flatMap((d) -> {
                Put imageInfo = new Put(DateTimeHelper.toBytes(d)).addColumn(
                    ImagesPageCoprocessor.FAMILY_IMGS_NAME_AS_BYTES,
                    imageRow,
                    ImagesPageCoprocessor.TRUE_VALUE);
                Increment inc = new Increment(DateTimeHelper.toBytes(d)).addColumn(
                    ImagesPageCoprocessor.FAMILY_STATS_NAME_AS_BYTES,
                    ImagesPageCoprocessor.COLUMN_STAT_AS_BYTES,
                    1);
                return Stream.of(imageInfo, inc);
            })
            .collect(Collectors.toList());
        bufferedMutator.mutate(mutationsList);
        bufferedMutator.flush();
    }

    protected void decrementDateInterval(String namespace, byte[] imageRow) throws IOException {
        BufferedMutator bufferedMutator = this.getBufferedMutator(namespace);
        long date = this.getDateOfRowOfTableSource(imageRow);
        List<Mutation> mutationsList = this.splitCreationDateToYearMonthDayAndHour(date)
            .stream()
            .flatMap((d) -> {
                Delete imageInfo = new Delete(DateTimeHelper.toBytes(d))
                    .addColumn(ImagesPageCoprocessor.FAMILY_IMGS_NAME_AS_BYTES, imageRow);
                Increment inc = new Increment(DateTimeHelper.toBytes(d)).addColumn(
                    ImagesPageCoprocessor.FAMILY_STATS_NAME_AS_BYTES,
                    ImagesPageCoprocessor.COLUMN_STAT_AS_BYTES,
                    -1);
                return Stream.of(imageInfo, inc);
            })
            .collect(Collectors.toList());
        bufferedMutator.mutate(mutationsList);
        bufferedMutator.flush();
    }

    protected long createSecundaryIndexInPagedTable(String namespace, byte[] row) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + this.getTablePageForMetadata()))) {
            long date = this.getDateOfRowOfTableSource(row);
            long pageNumber = this.findPageAndCreateIfNeeded(tablePage, date);
            ImagesPageCoprocessor.LOGGER.info(
                "[PAGINATION][PAGE {} ] createSecundaryIndexInPagedTable date is {} ",
                Long.toHexString(pageNumber),
                date);
            this.recordOfRowOfSourceInPageTable(tablePage, row, pageNumber);
            return pageNumber;
        } catch (IOException e) {
            ImagesPageCoprocessor.LOGGER.error(
                "[PAGINATION]Unable to create secundary index for row {}, exception is {} ",
                this.toHexString(row),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    protected void updateTablePage(Region region, String namespace, byte[] currentPageRow) {
        try {

            RowLock rowLock = null;
            try {
                ImagesPageCoprocessor.LOGGER.info(
                    "[PAGINATION][PAGE {} ] Trying to get lock for {} in region {} ",
                    this.toHexString(currentPageRow),
                    this.getTablePageForMetadata(),
                    region);
                rowLock = region != null ? region.getRowLock(currentPageRow, false) : null;
                Get get = new Get(currentPageRow).addFamily(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY)
                    .addFamily(ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY)
                    .addFamily(ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY);
                Result result = region.get(get);
                final NavigableMap<byte[], byte[]> familyMap = result
                    .getFamilyMap(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                long recordedSize = Bytes.toLong(
                    CellUtil.cloneValue(
                        result.getColumnLatestCell(
                            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS)));
                if (familyMap.size() == 0) {
                    ImagesPageCoprocessor.LOGGER.warn(
                        "[PAGINATION][PAGE {} ] updateTablePage, no elements in list family  ",
                        this.toHexString(currentPageRow));
                }
                byte[] firstRow = familyMap.firstKey();
                byte[] lastRow = familyMap.lastKey();
                long min = this.getDateOfRowInsertedOfTablePage(firstRow);
                long max = this.getDateOfRowInsertedOfTablePage(lastRow);
                if ((recordedSize % 100) == 0) {
                    ImagesPageCoprocessor.LOGGER.info(
                        "[PAGINATION][PAGE {} ] Min {}  max {} - family size {} - recorded size {}",
                        this.toHexString(currentPageRow),
                        min,
                        max,
                        familyMap.size(),
                        recordedSize);
                }
                long expextedPageNumberToReinsert = Bytes.toLong(currentPageRow) + 1;
                List<Put> putListOf = new ArrayList<>();
                while (familyMap.size() > ImagesPageCoprocessor.PAGE_SIZE) {
                    lastRow = familyMap.lastKey();
                    ImagesPageCoprocessor.LOGGER.info(
                        "[PAGINATION][PAGE {} ]Page is full, date of last row {} - last row {}",
                        this.toHexString(currentPageRow),
                        this.getDateOfRowInsertedOfTablePage(lastRow),
                        this.toHexString(lastRow));
                    Delete del = new Delete(currentPageRow)
                        .addColumn(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, lastRow);
                    familyMap.remove(lastRow);
                    max = this.getDateOfRowInsertedOfTablePage(familyMap.lastKey());
                    Put put1 = new Put(currentPageRow)
                        .addColumn(
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                            ImagesPageCoprocessor.convert(max))
                        .addColumn(
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                            ImagesPageCoprocessor.convert(min))
                        .addColumn(
                            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                            ImagesPageCoprocessor.convert((long) familyMap.size()));
                    region.delete(del);
                    region.put(put1);
                    ImagesPageCoprocessor.LOGGER.info(
                        "[PAGINATION]Region table has been updated, now reinsert in page {}, found last row {} - {}",
                        expextedPageNumberToReinsert,
                        this.getDateOfRowInsertedOfTablePage(lastRow),
                        this.toHexString(lastRow));

                    Long pageNumberToReinsert = this
                        .findPageAndCreateIfNeeded(region, this.getDateOfRowInsertedOfTablePage(lastRow));
                    putListOf.add(this.buildPutToRecordInNextPage(lastRow, pageNumberToReinsert));
                    max = this.getDateOfRowInsertedOfTablePage(familyMap.lastKey());
                }

                this.updateMinAndMaxInPageTable(currentPageRow, max, min, familyMap.size(), region);
                if (rowLock != null) {
                    rowLock.release();
                    rowLock = null;
                }
                // -> We break the transaction here :(
                putListOf.forEach((p) -> {
                    try {
                        region.put(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                // tablePage.put(putListOf);
            } finally {
                if (rowLock != null) {
                    rowLock.release();
                }
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            ImagesPageCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    private void recordOfRowOfSourceInPageTable(Table tablePage, byte[] row, Long pageNumber) throws IOException {
        Put put = new Put(ImagesPageCoprocessor.convert(pageNumber));
        long date = this.getDateOfRowOfTableSource(row);
        byte[] newRowOfPageTable = new byte[row.length + 8];
        System.arraycopy(row, 0, newRowOfPageTable, 8, row.length);
        final byte[] converedDate = ImagesPageCoprocessor.convert(date);
        System.arraycopy(converedDate, 0, newRowOfPageTable, 0, converedDate.length);
        put.addColumn(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, newRowOfPageTable, new byte[] { 1 });
        tablePage.put(put);
        ImagesPageCoprocessor.LOGGER
            .info("[PAGINATION]End of recordInPageTable {} in page {}", this.toHexString(row), pageNumber);
    }

    private Put buildPutToRecordInNextPage(byte[] row, Long pageNumber) throws IOException {
        ImagesPageCoprocessor.LOGGER
            .info("[PAGINATION]Start of update of page {} -  Row is {}", pageNumber, this.toHexString(row));
        Put put = new Put(ImagesPageCoprocessor.convert(pageNumber));
        put.addColumn(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, row, new byte[] { 1 });
        return put;
    }

    protected void updateMinAndMaxInPageTable(byte[] row, long currentMax, long currentMin, long size, Region region)
        throws IOException {
        Put put1 = new Put(row).addColumn(
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            ImagesPageCoprocessor.convert(size));
        boolean done = region.checkAndMutate(
            row,
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.GREATER,
            new LongComparator(size),
            put1);

        put1 = new Put(row).addColumn(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            ImagesPageCoprocessor.convert(currentMax));
        boolean intermDone1 = region.checkAndMutate(
            row,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(currentMax),
            put1);

        put1 = new Put(row).addColumn(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            ImagesPageCoprocessor.convert(currentMin));

        boolean intermDone2 = region.checkAndMutate(
            row,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS,
            new LongComparator(currentMin),
            put1);
        ImagesPageCoprocessor.LOGGER.info(
            "[PAGINATION][PAGE {} ]updateMinAndMaxInPageTable,  region is {}, step 3 current min is {} current max is {}, size to record is {} -  steps are {},{},{}",
            this.toHexString(row),
            region.toString(),
            currentMin,
            currentMax,
            size,
            done,
            intermDone1,
            intermDone2);
    }

    protected String toHexString(byte[] row) {
        StringBuilder strBuilder = new StringBuilder();
        for (byte b : row) {
            strBuilder.append(Integer.toHexString((b & 0x000000ff)));
        }
        return strBuilder.toString();
    }

    protected String toHexString(byte[] row, int index, int length) {
        StringBuilder strBuilder = new StringBuilder();
        for (int k = index; k < (index + length); k++) {
            byte b = row[k];
            strBuilder.append(Integer.toHexString((b & 0x000000ff)));
        }
        return strBuilder.toString();
    }

    protected long findPageAndCreateIfNeeded(Region region, long date) throws IOException {
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;

        // filter to get the first page which is not full
        SingleColumnValueFilter filterMaxNbOfElements = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.LESS,
            new LongComparator(ImagesPageCoprocessor.PAGE_SIZE));

        // filter to get the first page which current min value is greater than the
        // fiven date
        SingleColumnValueFilter filterMin = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(date));

        // filter to get the first page which current date value is between
        // min and max
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(date));

        Scan scan = new Scan()
            .setFilter(
                new FilterList(FilterList.Operator.MUST_PASS_ONE,
                    Arrays.asList(filterMaxNbOfElements, filterMin, new FilterList(filterMin2, filterMax))))
            .setLimit(1);
        try (

            RegionScanner rs = region.getScanner(scan)) {
            List<Cell> result = new ArrayList<>();
            boolean res = rs.next(result);
            if (result.size() >= 1) {
                final byte[] rowArray = result.get(0)
                    .getRowArray();
                final int length = result.get(0)
                    .getRowLength();
                final int offset = result.get(0)
                    .getRowOffset();
                ImagesPageCoprocessor.LOGGER.warn(
                    "[PAGINATION] Find {} cells in region scan - row is {} - length is {}, offset is {} ",
                    result.size(),
                    this.toHexString(rowArray, offset, length),
                    length,
                    offset);
                pageNumber = Bytes.toLong(rowArray, offset);
                pageIsFound = true;
                ImagesPageCoprocessor.LOGGER
                    .info("[PAGINATION]found page {}, for date {}, region is ", pageNumber, date, region.toString());
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }

        if (!pageIsFound) {
            pageNumber = this.createPage(region, date, pageNumber);

        }
        return pageNumber;
    }

    protected long findPageAndCreateIfNeeded(Table table, long date) throws IOException {
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;

        // filter to get the first page which is not full
        SingleColumnValueFilter filterMaxNbOfElements = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.LESS,
            new LongComparator(ImagesPageCoprocessor.PAGE_SIZE));

        // filter to get the first page which current min value is greater than the
        // fiven date
        SingleColumnValueFilter filterMin = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(date));

        // filter to get the first page which current date value is between
        // min and max
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
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
                ImagesPageCoprocessor.LOGGER.info("[PAGINATION]found page {}, for date {}", pageNumber, date);
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }

        if (!pageIsFound) {
            pageNumber = this.createPage(table, date, pageNumber);

        }
        return pageNumber;
    }

    protected long createPage(Region region, long date, long pageNumber) throws IOException {
        ImagesPageCoprocessor.LOGGER
            .info("[PAGINATION]Nothnig found for date {}, creating another page in region {}", date, region.toString());
        Scan scan = new Scan().setLimit(1)
            .setReversed(true);
        try (
            RegionScanner rs = region.getScanner(scan)) {
            List<Cell> result = new ArrayList<>();
            rs.next(result);
            if (result.size() >= 1) {
                final byte[] rowArray = result.get(0)
                    .getRowArray();
                final int length = result.get(0)
                    .getRowLength();
                final int offset = result.get(0)
                    .getRowOffset();
                ImagesPageCoprocessor.LOGGER.warn(
                    "[PAGINATION] Find {} cells in region scan - row is {} - length is {}, offset is {} ",
                    result.size(),
                    this.toHexString(rowArray, offset, length),
                    length,
                    offset);
                pageNumber = Bytes.toLong(rowArray, offset) + 1;
                Put put = new Put(ImagesPageCoprocessor.convert(pageNumber))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                        ImagesPageCoprocessor.convert(0l))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                        ImagesPageCoprocessor.convert(0L))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                        ImagesPageCoprocessor.convert(Long.MAX_VALUE));
                region.put(put);
            } else {
                throw new RuntimeException("Region Scan returned nothnig... Stopping !!!");
            }
        } catch (IOException e) {
            ImagesPageCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        ImagesPageCoprocessor.LOGGER.info("[PAGINATION]Creating page {} pageNumber for date {}", pageNumber, date);
        return pageNumber;
    }

    protected long createPage(Table table, long date, long pageNumber) throws IOException {
        ImagesPageCoprocessor.LOGGER.info("[PAGINATION]Nothnig found for date {}, creating another page", date);
        Scan scan = new Scan().setLimit(1)
            .setReversed(true);
        try (
            ResultScanner rs = table.getScanner(scan)) {
            Result res = rs.next();
            if (res != null) {
                pageNumber = Bytes.toLong(res.getRow()) + 1;
                Put put = new Put(ImagesPageCoprocessor.convert(pageNumber))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                        ImagesPageCoprocessor.convert(0l))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                        ImagesPageCoprocessor.convert(0L))
                    .addColumn(
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                        ImagesPageCoprocessor.convert(Long.MAX_VALUE));
                table.put(put);
                ImagesPageCoprocessor.LOGGER
                    .info("[PAGINATION][PAGE {}] Create new page for date {}", Long.toHexString(pageNumber), date);
            } else {
                throw new RuntimeException("Scan returned nothnig... Stopping !!!");
            }
        } catch (IOException e) {
            ImagesPageCoprocessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        return pageNumber;
    }

    private long getDateOfRowOfTableSource(byte[] row) {
        return Bytes.toLong(row, ImagesPageCoprocessor.FIXED_WIDTH_REGION_SALT, 8);
    }

    private long getDateOfRowInsertedOfTablePage(byte[] row) { return Bytes.toLong(row, 0, 8); }

    private long extractDateOfRowKeyOfTabkeSourceToIndex(byte[] row) { return this.getDateOfRowOfTableSource(row); }

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

        if (table.endsWith(":" + this.getTableSource())) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(ImagesPageCoprocessor.TABLE_SOURCE_THUMBNAIL);

            Optional.ofNullable(cells)
                .stream()
                .flatMap((x) -> x.stream())
                .filter((x) -> Integer.parseInt(new String(CellUtil.cloneQualifier(x))) == 1)
                .map((x) -> {
                    this.createSecundaryIndexInPagedTable(table2.getNamespaceAsString(), put.getRow());
                    return x;
                })
                .findAny()
                .ifPresent((x) -> {

                    if (ImagesPageCoprocessor.LOGGER.isDebugEnabled()) {
                        ImagesPageCoprocessor.LOGGER.debug(
                            "[PAGINATION]Creating secundary index for table {} - namespace is {} - row is {} ",
                            table2,
                            table2.getNamespaceAsString(),
                            new String(put.getRow(),
                                ImagesPageCoprocessor.FIXED_WIDTH_CREATION_DATE
                                    + ImagesPageCoprocessor.FIXED_WIDTH_REGION_SALT,
                                64,
                                Charset.forName("UTF-8")));
                    }
                });
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

    protected void deleteSecundaryIndex(Region region, String namespaceAsString, byte[] rowToIndex) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespaceAsString + ':' + this.getTablePageForMetadata()))) {
            ImagesPageCoprocessor.LOGGER.info(
                "[COPROC][{}] deleteSecundaryIndex, region is {},row put is {}",
                this.getCoprocName(),
                region,
                this.toHexString(rowToIndex));
            final Optional<Long> findPageOf = this.findPageOf(tablePage, rowToIndex);
            if (!findPageOf.isPresent()) {
                AbstractMetadataLongCoprocessor.LOGGER.info(
                    "[COPROC][{}] deleteSecundaryIndex page not found for  {}",
                    this.getCoprocName(),
                    this.toHexString(rowToIndex));
            }
            findPageOf.ifPresent((pageNumber) -> {
                try {
                    Delete delete = new Delete(ImagesPageCoprocessor.convert(pageNumber));
                    delete.addColumn(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY, rowToIndex);
                    tablePage.delete(delete);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Optional<Long> findPageOf(Table pageTable, byte[] rowKeyToIndex) {
        ImagesPageCoprocessor.LOGGER.info(
            "[COPROC][{}] findPageOf in table {}, search page for {} ",
            this.getCoprocName(),
            pageTable.getName(),
            this.toHexString(rowKeyToIndex));
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;
        // filter to get the first page which current date value is between
        // min and max
        final long date = this.extractDateOfRowKeyOfTabkeSourceToIndex(rowKeyToIndex);
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterValue = new SingleColumnValueFilter(
            ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY,
            rowKeyToIndex,
            CompareOperator.EQUAL,
            new BinaryComparator(ImagesPageCoprocessor.TRUE_VALUE));

        Scan scan = new Scan().setFilter(new FilterList(filterMin2, filterMax, filterValue))
            .setLimit(1);
        try (

            ResultScanner rs = pageTable.getScanner(scan)) {
            Result res = rs.next();
            if (res != null) {
                pageNumber = this.extractPageNumber(res.getRow());
                pageIsFound = true;
                ImagesPageCoprocessor.LOGGER.info("[{}]found page {}", this.getCoprocName(), pageNumber);
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }
        if (pageIsFound) { return Optional.ofNullable(pageNumber); }
        ImagesPageCoprocessor.LOGGER.info(
            "[COPROC][{}] findPageOf, no page is found for {} - date is {} - scanning...",
            this.getCoprocName(),
            this.toHexString(rowKeyToIndex),
            date);
        scan = new Scan();
        try (

            ResultScanner rs = pageTable.getScanner(scan)) {
            Result res = rs.next();
            while (res != null) {
                final Result finalResult = res;
                NavigableMap<byte[], byte[]> map = res
                    .getFamilyMap(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                if (map != null) {
                    map.forEach(
                        (k, v) -> ImagesPageCoprocessor.LOGGER.info(
                            "[COPROC][{}] for page {}, found element {}",
                            this.getCoprocName(),
                            this.toHexString(finalResult.getRow()),
                            this.toHexString(k)));
                }
                long maxValue = Bytes.toLong(
                    CellUtil.cloneValue(
                        res.getColumnLatestCell(
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER)));
                long minValue = Bytes.toLong(
                    CellUtil.cloneValue(
                        res.getColumnLatestCell(
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER)));
                ImagesPageCoprocessor.LOGGER.info(
                    "[COPROC][{}] for page {}, found min {}, max {}",
                    this.getCoprocName(),
                    this.toHexString(res.getRow()),
                    minValue,
                    maxValue);
                res = rs.next();
            }

            ImagesPageCoprocessor.LOGGER.info(
                "[COPROC][{}] findPageOf, no page is found for {} - date is {} - End of scanning...",
                this.getCoprocName(),
                this.toHexString(rowKeyToIndex),
                date);

        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    private long extractPageNumber(byte[] row) { return Bytes.toLong(row); }

    @Override
    public void postDelete(
        ObserverContext<RegionCoprocessorEnvironment> observerContext,
        Delete delete,
        WALEdit edit,
        Durability durability
    ) throws IOException {
        final TableName table2 = observerContext.getEnvironment()
            .getRegion()
            .getRegionInfo()
            .getTable();
        String table = table2.getNameAsString();
        if (table.endsWith(":" + this.getTableSource())) {
            if (delete.getFamilyCellMap()
                .size() == 0) {
                ImagesPageCoprocessor.LOGGER.info(
                    "[COPROC][{}] postDelete, table is {}, row put is {}",
                    this.getCoprocName(),
                    this.getTableSource(),
                    this.toHexString(delete.getRow()));
                try (
                    Table metaDataPageTable = this.hbaseConnection.getTable(
                        TableName.valueOf(table2.getNamespaceAsString() + ":" + this.getTablePageForMetadata()))) {
                    long lockNumber = Long.MIN_VALUE;
                    try {
                        lockNumber = this.getLock(metaDataPageTable);
                        this.deleteSecundaryIndex(
                            observerContext.getEnvironment()
                                .getRegion(),
                            table2.getNamespaceAsString(),
                            delete.getRow());
                        this.decrementDateInterval(table2.getNamespaceAsString(), delete.getRow());
                    } catch (ServiceException e) {
                        throw new RuntimeException(e);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            this.releaseLock(metaDataPageTable, lockNumber);
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        } else if (table.endsWith(":" + this.getTablePageForMetadata())) {
            List<Cell> cells = delete.getFamilyCellMap()
                .get(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
            ImagesPageCoprocessor.LOGGER.info(
                "[COPROC][{}] postDelete, table is {}, row put is {}",
                this.getCoprocName(),
                this.getTablePageForMetadata(),
                this.toHexString(delete.getRow()));
            if (cells != null) {
                cells.stream()
                    .map((c) -> CellUtil.cloneQualifier(c))
                    .forEach(
                        (r) -> this.reorganizePagesOfImagesIfNeeded(
                            observerContext.getEnvironment()
                                .getRegion(),
                            delete.getRow(),
                            r,
                            table2.getNamespaceAsString()));
            }
        }
    }

    protected void reorganizePagesOfImagesIfNeeded(
        Region region,
        byte[] rowPage,
        byte[] rowKeyToDelete,
        String namespaceAsString
    ) {
        if (this.writerSequenceNumber.get() != 0) {
            long pageNumber = this.extractPageNumber(rowPage);
            long nextPageNumber = pageNumber + 1;
            ImagesPageCoprocessor.LOGGER.info(
                "[COPROC][{}] reorganizePagesOfImagesIfNeeded : Lock was taken,  delete - page num : {} on table page is allowed.",
                this.getCoprocName(),
                pageNumber);
            try {
                byte[] nextPageNumberRow = ImagesPageCoprocessor.convert(nextPageNumber);
                Get get = new Get(nextPageNumberRow).addFamily(ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY)
                    .addFamily(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                Result result = region.get(get);
                if (result != null) {
                    final NavigableMap<byte[], byte[]> listOfRowsOfNextPage = result
                        .getFamilyMap(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                    if ((listOfRowsOfNextPage != null) && !listOfRowsOfNextPage.isEmpty()) {
                        Get getElementsOfCurrentPage = new Get(rowPage)
                            .addFamily(ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY);
                        long nbOfElementsInCurrentPage = Bytes.toLong(
                            CellUtil.cloneValue(
                                region.get(getElementsOfCurrentPage)
                                    .getColumnLatestCell(
                                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                        ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS)));
                        byte[] firstRow = listOfRowsOfNextPage.firstKey();
                        Delete del = new Delete(nextPageNumberRow)
                            .addColumn(ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY, firstRow);
                        region.delete(del);
                        Put put = new Put(rowPage)
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY,
                                firstRow,
                                ImagesPageCoprocessor.TRUE_VALUE)
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                ImagesPageCoprocessor.convert(nbOfElementsInCurrentPage++));
                        region.put(put);
                        pageNumber++;
                        nextPageNumber++;
                    } else {
                        ImagesPageCoprocessor.LOGGER.warn(
                            "[COPROC][{}] for page [{}] nothing in the family {}",
                            this.getCoprocName(),
                            nextPageNumber,
                            new String(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY));
                    }
                } else {
                    ImagesPageCoprocessor.LOGGER
                        .info("[COPROC][{}] nothing found for page {}", this.getCoprocName(), nextPageNumberRow);
                }

                Get getInfoOfCurrentRowPage = new Get(rowPage);
                final Result result2 = region.get(getInfoOfCurrentRowPage);
                if (result2 != null) {
                    NavigableMap<byte[], byte[]> map = result2
                        .getFamilyMap(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                    if ((map == null) || (map.size() == 0)) {
                        ImagesPageCoprocessor.LOGGER.info(
                            "[COPROC][{}] No more elements in row page {}, deleting page {}",
                            this.getCoprocName(),
                            this.toHexString(rowPage),
                            pageNumber);
                        // Delete del = new Delete(rowPage);
                        // tablePage.delete(del);
                        Put put = new Put(rowPage)
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                ImagesPageCoprocessor.convert((long) 0))
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                                ImagesPageCoprocessor.convert(0L))
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                                ImagesPageCoprocessor.convert(Long.MAX_VALUE));
                        region.put(put);
                    } else if (map.size() > 0) {

                        long minDate = this.extractDateOfRowKeyOfTabkeSourceToIndex(map.firstKey());
                        long maxDate = this.extractDateOfRowKeyOfTabkeSourceToIndex(map.lastKey());
                        ImagesPageCoprocessor.LOGGER.info(
                            "[COPROC][{}] decremeting elements in row page {}, page is {}, new min {}, new max  {}, nb of elements {}",
                            this.getCoprocName(),
                            this.toHexString(rowPage),
                            pageNumber,
                            minDate,
                            maxDate,
                            map.size());
                        Put put = new Put(rowPage)
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                ImagesPageCoprocessor.convert((long) map.size()))
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                                ImagesPageCoprocessor.convert(minDate))
                            .addColumn(
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                ImagesPageCoprocessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                                ImagesPageCoprocessor.convert(maxDate));
                        region.put(put);
                    }
                } else {
                    ImagesPageCoprocessor.LOGGER.info(
                        "[COPROC][{}] row page {} does not exist",
                        this.getCoprocName(),
                        this.toHexString(rowPage));

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            ImagesPageCoprocessor.LOGGER.warn(
                "[COPROC][{}] Lock was not taken on page  {}  !! Delete on table page is disallowed - row page {} \n row key to delete {}",
                this.getCoprocName(),
                region,
                this.toHexString(rowPage),
                this.toHexString(rowKeyToDelete));
        }
    }

    protected void processPostMutation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation put) {
        final TableName table2 = observerContext.getEnvironment()
            .getRegion()
            .getRegionInfo()
            .getTable();
        String table = table2.getNameAsString();
        if (table.endsWith(":" + this.getTablePageForMetadata())) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(ImagesPageCoprocessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
            if (cells != null) {
                ImagesPageCoprocessor.LOGGER.info(
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
        } else if (table.endsWith(":" + this.getTableSource())) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(ImagesPageCoprocessor.TABLE_SOURCE_THUMBNAIL);
            Optional.ofNullable(cells)
                .stream()
                .flatMap((x) -> x.stream())
                .filter((x) -> Integer.parseInt(new String(CellUtil.cloneQualifier(x))) == 1)
                .findFirst()
                .ifPresent((x) -> {
                    try {
                        // if (this.rowDataIsPresent(table2.getNamespaceAsString(), put.getRow()))
                        {
                            this.incrementDateInterval(table2.getNamespaceAsString(), put.getRow());
                        }
                    } catch (IOException e) {
                        ImagesPageCoprocessor.LOGGER.warn("Error ", e);
                    }
                });
        }
    }

    private boolean rowDataIsPresent(String namespace, byte[] row) {
        try {
            if (!Objects.equals(namespace, "prod")) {
                ImagesPageCoprocessor.LOGGER.info("PRocessing an unexpected namespace : {} ", namespace);
            }
            final TableName tableName = TableName.valueOf(namespace + ':' + this.getTableSource());
            try (
                Table table = this.hbaseConnection.getTable(tableName)) {
                Get get = new Get(row);
                return table.get(get)
                    .getColumnCells(row, row)
                    .size() == 1;
            } catch (IOException e) {
                ImagesPageCoprocessor.LOGGER.error(" Unexpected error in hbase ", e);
            }
            return true;
        } catch (Exception e) {
            ImagesPageCoprocessor.LOGGER.error(" Unexpected error in hbase ", e);
            throw new RuntimeException(e);
        }
    }

    protected String getTablePageForMetadata() { return ImagesPageCoprocessor.TABLE_PAGE; }

    protected String getTableSource() { return ImagesPageCoprocessor.TABLE_SOURCE; }

    @Override
    protected String getCoprocName() { return "COPROC-IMAGES"; }

    @Override
    public void releaseLock(RpcController controller, LockRequest request, RpcCallback<LockResponse> done) { // TODO
                                                                                                             // Auto-generated
                                                                                                             // method
                                                                                                             // stub
    }

}