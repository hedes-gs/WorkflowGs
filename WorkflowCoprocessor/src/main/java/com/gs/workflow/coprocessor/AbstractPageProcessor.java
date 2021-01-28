package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.gs.workflow.coprocessor.LockTableHelper.LockRequest;
import com.gs.workflow.coprocessor.LockTableHelper.LockResponse;

public abstract class AbstractPageProcessor<T> extends LockTableHelper._LockService
    implements RegionCoprocessor, RegionObserver {
    protected static final long   PAGE_SIZE                           = 1000;
    protected static final String COLUMN_STAT_NAME                    = "stats";
    protected static final String FAMILY_IMGS_NAME                    = "imgs";
    protected static final String FAMILY_STATS_NAME                   = "fstats";
    protected static final byte[] COLUMN_STAT_AS_BYTES                = AbstractMetadataLongCoprocessor.COLUMN_STAT_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_IMGS_NAME_AS_BYTES           = AbstractMetadataLongCoprocessor.FAMILY_IMGS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_STATS_NAME_AS_BYTES          = AbstractMetadataLongCoprocessor.FAMILY_STATS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TRUE_VALUE                          = new byte[] { 1 };

    protected static final byte[] TABLE_PAGE_DESC_COLUMN_FAMILY       = "max_min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER = "max".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER = "min".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_LIST_COLUMN_FAMILY       = "list".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements"
        .getBytes(Charset.forName("UTF-8"));
    public static final byte[][]  SOURCE_FAMILIES_TO_EXCLUDE          = {
            "thb".getBytes(Charset.forName("UTF-8")), "albums".getBytes(Charset.forName("UTF-8")),
            "keywords".getBytes(Charset.forName("UTF-8")), "ratings".getBytes(Charset.forName("UTF-8")),
            "persons".getBytes(Charset.forName("UTF-8")), };
    private static final long     INVALID_SEQ                         = 0;
    private final AtomicLong      writerSequenceNumber                = new AtomicLong(
        AbstractPageProcessor.INVALID_SEQ);
    protected static final byte[] TABLE_SOURCE_THUMBNAIL              = "thb".getBytes(Charset.forName("UTF-8"));
    public static final int       FIXED_WIDTH_IMAGE_ID                = 64;
    public static final int       FIXED_WIDTH_REGION_SALT             = 2;

    protected Long getLock(Table metaDataPageTable) throws ServiceException, Throwable {
        Long lockNumber = new Random().nextLong();
        Map<byte[], Long> res = metaDataPageTable.coprocessorService(
            LockTableHelper._LockService.class,
            null,
            null,
            aggregate -> this.doLock(lockNumber, aggregate));
        AbstractPageProcessor.LOGGER
            .info("[COPROC][{}] getLock, lockNumber is {} - res is {}", this.getCoprocName(), lockNumber, res);
        return lockNumber;
    }

    protected Long doLock(Long lockNumber, LockTableHelper._LockService aggregate) throws IOException {
        AbstractPageProcessor.LOGGER
            .info("[COPROC][{}]doing lock {} for aggregate {}", this.getCoprocName(), lockNumber, aggregate);
        CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse>();
        LockTableHelper.LockRequest request = LockTableHelper.LockRequest.newBuilder()
            .setColumn("test")
            .setFamily("familytest")
            .setValue(lockNumber)
            .build();
        aggregate.lock(null, request, rpcCallback);
        LockTableHelper.LockResponse response = rpcCallback.get();
        return response.hasSum() ? response.getSum() : 0L;
    }

    protected Long releaseLock(Table metaDataPageTable, long lockNumber) throws ServiceException, Throwable {
        Map<byte[], Long> res = metaDataPageTable.coprocessorService(
            LockTableHelper._LockService.class,
            null,
            null,
            aggregate -> this.doReleaseLock(lockNumber, aggregate));
        return lockNumber;
    }

    private Long doReleaseLock(long lockNumber, LockTableHelper._LockService aggregate) throws IOException {
        CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse>();
        LockTableHelper.LockRequest request = LockTableHelper.LockRequest.newBuilder()
            .setColumn("test")
            .setFamily("familytest")
            .setValue(lockNumber)
            .build();
        aggregate.releaseLock(null, request, rpcCallback);
        LockTableHelper.LockResponse response = rpcCallback.get();
        return response.hasSum() ? response.getSum() : 0L;
    }

    protected Connection                   hbaseConnection;
    protected RegionCoprocessorEnvironment env;

    protected static Logger                LOGGER = LoggerFactory.getLogger(AbstractPageProcessor.class);

    protected Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }

    @Override
    public Iterable<Service> getServices() { return Collections.singleton(this); }

    @Override
    public Optional<RegionObserver> getRegionObserver() { return Optional.of(this); }

    @Override
    public void releaseLock(RpcController controller, LockRequest request, RpcCallback<LockResponse> done) {
        long v = this.writerSequenceNumber.getAndSet(0);
        AbstractPageProcessor.LOGGER.info("[COPROC][{}]Lock is released... !", this.getCoprocName());

        LockResponse response = LockResponse.newBuilder()
            .setSum(v)
            .build();
        done.run(response);

    }

    @Override
    public void lock(RpcController controller, LockRequest request, RpcCallback<LockResponse> done) {
        AbstractPageProcessor.LOGGER.info("[COPROC][{}]Trying to get lock...", this.getCoprocName());
        boolean isInterrupted = false;
        while (!this.writerSequenceNumber.compareAndSet(0, request.getValue())) {
            try {
                java.util.concurrent.TimeUnit.MILLISECONDS.sleep(100);

            } catch (InterruptedException e) {
                isInterrupted = true;
                break;
            }
        }
        if (!isInterrupted) {
            AbstractPageProcessor.LOGGER.info("[COPROC][{}]Region is locked...", this.getCoprocName());
            LockResponse response = LockResponse.newBuilder()
                .setSum(this.writerSequenceNumber.get())
                .build();
            done.run(response);
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        RegionCoprocessor.super.start(env);
        this.hbaseConnection = this.hbaseConnection();
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        AbstractPageProcessor.LOGGER.info("STARTING Coprocessor..." + this.getCoprocName());
    }

    protected Optional<Long> findPageOf(Table pageTable, byte[] rowKeyToIndex) {
        AbstractPageProcessor.LOGGER.info(
            "[COPROC][{}] findPageOf in table {}, search page for {} ",
            this.getCoprocName(),
            pageTable.getName(),
            AbstractPageProcessor.toHexString(rowKeyToIndex));
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;
        // filter to get the first page which current date value is between
        // min and max
        final long date = this.extractDateOfRowKeyToIndex(rowKeyToIndex);
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER_OR_EQUAL,
            new LongComparator(date));
        SingleColumnValueFilter filterValue = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY,
            rowKeyToIndex,
            CompareOperator.EQUAL,
            new BinaryComparator(AbstractPageProcessor.TRUE_VALUE));

        Scan scan = new Scan().setFilter(new FilterList(filterMin2, filterMax, filterValue))
            .setLimit(1);
        try (

            ResultScanner rs = pageTable.getScanner(scan)) {
            Result res = rs.next();
            if (res != null) {
                pageNumber = this.extractPageNumber(res.getRow());
                pageIsFound = true;
                AbstractPageProcessor.LOGGER
                    .info("found page {}, for metadata {}", pageNumber, this.extractMetadataValue(res.getRow()));
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }
        if (pageIsFound) { return Optional.ofNullable(pageNumber); }
        AbstractPageProcessor.LOGGER.info(
            "[COPROC][{}] findPageOf, no page is found for {} - date is {} - scanning...",
            this.getCoprocName(),
            AbstractPageProcessor.toHexString(rowKeyToIndex),
            date);
        scan = new Scan();
        try (

            ResultScanner rs = pageTable.getScanner(scan)) {
            Result res = rs.next();
            while (res != null) {
                final Result finalResult = res;
                NavigableMap<byte[], byte[]> map = res
                    .getFamilyMap(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                if (map != null) {
                    map.forEach(
                        (k, v) -> AbstractPageProcessor.LOGGER.info(
                            "[COPROC][{}] for page {}, found element {}",
                            this.getCoprocName(),
                            AbstractPageProcessor.toHexString(finalResult.getRow()),
                            AbstractPageProcessor.toHexString(k)));
                }
                long maxValue = Bytes.toLong(
                    CellUtil.cloneValue(
                        res.getColumnLatestCell(
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER)));
                long minValue = Bytes.toLong(
                    CellUtil.cloneValue(
                        res.getColumnLatestCell(
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER)));
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] for page {}, found min {}, max {}",
                    this.getCoprocName(),
                    AbstractPageProcessor.toHexString(res.getRow()),
                    minValue,
                    maxValue);
                res = rs.next();
            }

            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] findPageOf, no page is found for {} - date is {} - End of scanning...",
                this.getCoprocName(),
                AbstractPageProcessor.toHexString(rowKeyToIndex),
                date);

        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    protected void createEntryInTableMetaData(Table metaDataTable, T metadata) {
        try {
            Increment inc = new Increment(this.getRowKeyForMetaDataTable(metadata)).addColumn(
                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                1);
            metaDataTable.increment(inc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void deleteEntryInTableMetaData(Table metaDataTable, T metadata) {
        try {
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] deleteEntryInTableMetaData, decrementing table is {}, metadata to delete is {}",
                this.getCoprocName(),
                this.getTableSource(),
                metadata);

            Increment dec = new Increment(this.getRowKeyForMetaDataTable(metadata)).addColumn(
                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                -1);
            metaDataTable.increment(dec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract byte[] getRowKeyForMetaDataTable(T metadata);

    protected void reorganizeIfNeeded(
        Table tablePage,
        Region region,
        byte[] rowPage,
        byte[] rowKeyToDelete,
        String namespaceAsString
    ) {
        if (this.writerSequenceNumber.get() != 0) {
            long pageNumber = this.extractPageNumber(rowPage);
            long nextPageNumber = pageNumber + 1;
            T metaData = this.extractMetadataValue(rowPage);
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] Lock was taken,  delete - page num : {} - mataData {} -  on table page is allowed.",
                this.getCoprocName(),
                pageNumber,
                metaData);
            try {
                byte[] nextPageNumberRow = this.toRowKey(metaData, nextPageNumber);
                Get get = new Get(nextPageNumberRow).addFamily(AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY)
                    .addFamily(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                Result result = tablePage.get(get);
                if (result != null) {
                    final NavigableMap<byte[], byte[]> listOfRowsOfNextPage = result
                        .getFamilyMap(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                    if ((listOfRowsOfNextPage != null) && !listOfRowsOfNextPage.isEmpty()) {
                        Get getElementsOfCurrentPage = new Get(rowPage)
                            .addFamily(AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY);
                        long nbOfElementsInCurrentPage = Bytes.toLong(
                            CellUtil.cloneValue(
                                region.get(getElementsOfCurrentPage)
                                    .getColumnLatestCell(
                                        AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                        AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS)));
                        byte[] firstRow = listOfRowsOfNextPage.firstKey();
                        Delete del = new Delete(nextPageNumberRow)
                            .addColumn(AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY, firstRow);
                        tablePage.delete(del);
                        Put put = new Put(rowPage)
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY,
                                firstRow,
                                AbstractPageProcessor.TRUE_VALUE)
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                AbstractPageProcessor.convert(nbOfElementsInCurrentPage++));
                        region.put(put);
                        pageNumber++;
                        nextPageNumber++;
                    } else {
                        AbstractPageProcessor.LOGGER.warn(
                            "[COPROC][{}] for page [{},{}] nothing in the family {}",
                            this.getCoprocName(),
                            metaData,
                            nextPageNumber,
                            new String(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY));
                    }
                } else {
                    AbstractPageProcessor.LOGGER
                        .info("[COPROC][{}] nothing found for page {}", this.getCoprocName(), nextPageNumberRow);
                }

                Get getInfoOfCurrentRowPage = new Get(rowPage);
                final Result result2 = region.get(getInfoOfCurrentRowPage);
                if (result2 != null) {
                    NavigableMap<byte[], byte[]> map = result2
                        .getFamilyMap(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                    if ((map == null) || (map.size() == 0)) {
                        AbstractPageProcessor.LOGGER.info(
                            "[COPROC][{}] No more elements in row page {}, deleting page {}",
                            this.getCoprocName(),
                            AbstractPageProcessor.toHexString(rowPage),
                            pageNumber);
                        // Delete del = new Delete(rowPage);
                        // tablePage.delete(del);
                        Put put = new Put(rowPage)
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                AbstractPageProcessor.convert((long) 0))
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                                AbstractPageProcessor.convert(0L))
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                                AbstractPageProcessor.convert(Long.MAX_VALUE));
                        region.put(put);
                    } else if (map.size() > 0) {

                        long minDate = this.extractDateOfRowKeyToIndex(map.firstKey());
                        long maxDate = this.extractDateOfRowKeyToIndex(map.lastKey());
                        AbstractPageProcessor.LOGGER.info(
                            "[COPROC][{}] decremeting elements in row page {}, page is {}, new min {}, new max  {}, nb of elements {}",
                            this.getCoprocName(),
                            AbstractPageProcessor.toHexString(rowPage),
                            pageNumber,
                            minDate,
                            maxDate,
                            map.size());
                        Put put = new Put(rowPage)
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                                AbstractPageProcessor.convert((long) map.size()))
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                                AbstractPageProcessor.convert(minDate))
                            .addColumn(
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                                AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                                AbstractPageProcessor.convert(maxDate));
                        region.put(put);
                    }
                } else {
                    AbstractPageProcessor.LOGGER.info(
                        "[COPROC][{}] row page {} does not exist",
                        this.getCoprocName(),
                        AbstractPageProcessor.toHexString(rowPage));

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            AbstractPageProcessor.LOGGER.warn(
                "[COPROC][{}] Lock was not taken on page  {}  !! Delete on table page is disallowed - row page {} \n row key to delete {}",
                this.getCoprocName(),
                tablePage,
                AbstractPageProcessor.toHexString(rowPage),
                AbstractPageProcessor.toHexString(rowKeyToDelete));
        }
    }

    protected long findFirstPageWithAvailablePlace(Table pageTable, byte[] rowKeyToIndex, T metaData) {
        boolean pageIsFound = false;
        long pageNumber = Long.MIN_VALUE;
        final long dateOfRowIndex = this.extractDateOfRowKeyToIndex(rowKeyToIndex);

        // Filter to get the metadata
        RowFilter filterOnMetadata = this.getRowFilter(metaData);

        // filter to get the first page which is not full
        SingleColumnValueFilter filterMaxNbOfElements = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.LESS,
            new LongComparator(AbstractPageProcessor.PAGE_SIZE));

        // filter to get the first page which current min value is greater than the
        // fiven date
        SingleColumnValueFilter filterMin = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(dateOfRowIndex));

        // filter to get the first page which current date value is between
        // min and max
        SingleColumnValueFilter filterMin2 = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS_OR_EQUAL,
            new LongComparator(dateOfRowIndex));
        SingleColumnValueFilter filterMax = new SingleColumnValueFilter(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(dateOfRowIndex));

        Scan scan = new Scan()
            .setFilter(
                new FilterList(filterOnMetadata,
                    new FilterList(FilterList.Operator.MUST_PASS_ONE,
                        Arrays.asList(filterMaxNbOfElements, filterMin, new FilterList(filterMin2, filterMax)))))
            .setLimit(1);
        try (

            ResultScanner rs = pageTable.getScanner(scan)) {
            Result res = rs.next();
            if (res != null) {
                pageNumber = this.extractPageNumber(res.getRow());
                pageIsFound = true;
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] found page {}, for metadata {}",
                    this.getCoprocName(),
                    pageNumber,
                    this.extractMetadataValue(res.getRow()));
            }
        } catch (
            IllegalArgumentException |
            IOException e) {
            throw new RuntimeException(e);
        }

        if (!pageIsFound) {
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] No page  found for metadata {} with date {}, creating another page",
                this.getCoprocName(),
                metaData,
                dateOfRowIndex);
            scan = new Scan().setFilter(filterOnMetadata)
                .setLimit(1)
                .setReversed(true);
            try (
                ResultScanner rs = pageTable.getScanner(scan)) {
                Result res = rs.next();
                if (res != null) {
                    pageNumber = this.extractPageNumber(res.getRow()) + 1;
                } else {
                    pageNumber = 0;
                }
                final byte[] rowKey = this.toRowKey(metaData, pageNumber);
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] Creating page {}, for metadata {} - rowkey is ",
                    this.getCoprocName(),
                    pageNumber,
                    metaData,
                    AbstractPageProcessor.toHexString(rowKey));
                Put put = new Put(rowKey)
                    .addColumn(
                        AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                        AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                        AbstractPageProcessor.convert(0l))
                    .addColumn(
                        AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                        AbstractPageProcessor.convert(0L))
                    .addColumn(
                        AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                        AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                        AbstractPageProcessor.convert(Long.MAX_VALUE));
                pageTable.put(put);
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] End of creating page {} pageNumber for date {}",
                    this.getCoprocName(),
                    pageNumber,
                    metaData);
            } catch (IOException e) {
                AbstractPageProcessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }

        }
        return pageNumber;
    }

    protected abstract RowFilter getRowFilter(T metaData);

    protected void updateTablePage(
        Table tablePage,
        Region region,
        String namespace,
        byte[] currentPageRow,
        T metaData
    ) {
        try {

            RowLock rowLock = null;
            try {
                rowLock = region != null ? region.getRowLock(currentPageRow, false) : null;
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] Getting information for row {} ",
                    this.getCoprocName(),
                    AbstractPageProcessor.toHexString(currentPageRow));
                Get get = new Get(currentPageRow).addFamily(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY)
                    .addFamily(AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY)
                    .addFamily(AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY);

                Result result = region.get(get);
                final NavigableMap<byte[], byte[]> familyMap = result
                    .getFamilyMap(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                long recordedSize = Bytes.toLong(
                    CellUtil.cloneValue(
                        result.getColumnLatestCell(
                            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS)));
                if (familyMap.size() == 0) {
                    AbstractPageProcessor.LOGGER.warn(
                        "updateTablePage, for row {} no elements in list family  ",
                        AbstractPageProcessor.toHexString(currentPageRow));
                }
                byte[] firstRow = familyMap.firstKey();
                byte[] lastRow = familyMap.lastKey();
                long min = Bytes.toLong(firstRow);
                long max = Bytes.toLong(lastRow);
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] Min {} and max {} found in current page : {} - nb Of elements in page {}, recorded size {}",
                    this.getCoprocName(),
                    min,
                    max,
                    this.extractPageNumber(currentPageRow),
                    familyMap.size(),
                    recordedSize);

                if (AbstractPageProcessor.LOGGER.isDebugEnabled()) {
                    AbstractPageProcessor.LOGGER.debug(
                        "[COPROC][{}] updateTablePage, check current page row {} - {} - nb of elements : {}  ",
                        this.getCoprocName(),
                        AbstractPageProcessor.toHexString(currentPageRow),
                        Bytes.toLong(currentPageRow),
                        familyMap.size());
                }
                while (familyMap.size() > AbstractPageProcessor.PAGE_SIZE) {
                    lastRow = familyMap.lastKey();
                    AbstractPageProcessor.LOGGER.info(
                        "[COPROC][{}] Page is full, currentPageRow is {},  found last row {} - {}",
                        this.getCoprocName(),
                        AbstractPageProcessor.toHexString(currentPageRow),
                        Bytes.toLong(lastRow),
                        AbstractPageProcessor.toHexString(lastRow));
                    Delete del = new Delete(currentPageRow)
                        .addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, lastRow);
                    familyMap.remove(lastRow);
                    Put put1 = new Put(currentPageRow)
                        .addColumn(
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                            AbstractPageProcessor.convert(max))
                        .addColumn(
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                            AbstractPageProcessor.convert(min))
                        .addColumn(
                            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                            AbstractPageProcessor.convert((long) familyMap.size()));
                    region.delete(del);
                    region.put(put1);
                    AbstractPageProcessor.LOGGER.info(
                        "[COPROC][{}] Region table has been updated, now reinsert found last row {}",
                        this.getCoprocName(),
                        AbstractPageProcessor.toHexString(lastRow));
                    Long pageNumberToReinsert = this.findFirstPageWithAvailablePlace(tablePage, lastRow, metaData);
                    if (pageNumberToReinsert == this.extractPageNumber(currentPageRow)) {
                        AbstractPageProcessor.LOGGER.error(
                            "[COPROC][{}] Page number is the current page row, where reinsert should be done {} - {}, ignoring",
                            this.getCoprocName(),
                            AbstractPageProcessor.toHexString(lastRow),
                            pageNumberToReinsert);

                    } else {
                        AbstractPageProcessor.LOGGER.info(
                            "[COPROC][{}] Page number where reinsert should be done {} - {}",
                            this.getCoprocName(),
                            AbstractPageProcessor.toHexString(lastRow),
                            pageNumberToReinsert);
                        this.recordInPageTable(tablePage, lastRow, pageNumberToReinsert, metaData);
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
            AbstractPageProcessor.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    private void recordInPageTable(Table tablePage, byte[] row, Long pageNumber, T metaData) throws IOException {
        Put put = new Put(this.toRowKey(metaData, pageNumber));
        put.addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, row, new byte[] { 1 });
        tablePage.put(put);
        AbstractPageProcessor.LOGGER.info(
            "[COPROC][{}] End of recordInPageTable {} in page {}",
            this.getCoprocName(),
            AbstractPageProcessor.toHexString(row),
            pageNumber);
    }

    protected void updateMinAndMaxInPageTable(byte[] row, long currentMax, long currentMin, long size, Region region)
        throws IOException {
        Put put1 = new Put(row).addColumn(
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            AbstractPageProcessor.convert(size));
        region.checkAndRowMutate(
            row,
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
            CompareOperator.GREATER,
            new LongComparator(size),
            RowMutations.of(Arrays.asList(put1)));

        put1 = new Put(row).addColumn(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            AbstractPageProcessor.convert(currentMax));
        region.checkAndRowMutate(
            row,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
            CompareOperator.GREATER,
            new LongComparator(currentMax),
            RowMutations.of(Arrays.asList(put1)));

        put1 = new Put(row).addColumn(
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            AbstractPageProcessor.convert(currentMin));

        region.checkAndRowMutate(
            row,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_FAMILY,
            AbstractPageProcessor.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
            CompareOperator.LESS,
            new LongComparator(currentMin),
            RowMutations.of(Arrays.asList(put1)));
        AbstractPageProcessor.LOGGER.info(
            "[COPROC][{}] End of updateMinAndMaxInPageTable current min is {} current max is {}, find size is {}, row is {}",
            this.getCoprocName(),
            currentMin,
            currentMax,
            size,
            AbstractPageProcessor.toHexString(row));

    }

    protected void updateTableOfImagesOfMetaDataOnInsert(Put put, String nameSpace, String tableMetaData, T metaData) {
        Put putInTableMetaData = new Put(this.buildTableMetaDataRowKey(metaData, put.getRow()));
        try (
            Table table = this.hbaseConnection.getTable(TableName.valueOf(nameSpace + ":" + tableMetaData))) {
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] updateTableOfImagesOfMetaDataOnInsert, table is {}, metadata is {}, row put is {}",
                this.getCoprocName(),
                tableMetaData,
                metaData,
                AbstractPageProcessor.toHexString(put.getRow()));
            put.getFamilyCellMap()
                .entrySet()
                .stream()
                .filter((e) -> this.shouldFamilyInsourceBeRecordedInMetadata(e.getKey()))
                .forEach(
                    (k) -> k.getValue()
                        .forEach(
                            (c) -> putInTableMetaData
                                .addColumn(k.getKey(), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c))));
            table.put(putInTableMetaData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected void updateTableOfImagesOfMetaDataOnDelete(
        Delete delete,
        String nameSpace,
        String tableMetaData,
        T metaData
    ) {
        Delete deleteInTableMetaData = new Delete(this.buildTableMetaDataRowKey(metaData, delete.getRow()));
        try (
            Table table = this.hbaseConnection.getTable(TableName.valueOf(nameSpace + ":" + tableMetaData))) {
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] updateTableOfImagesOfMetaDataOnDelete, table is {}, metadata is {}, row put is {}",
                this.getCoprocName(),
                tableMetaData,
                metaData,
                AbstractPageProcessor.toHexString(delete.getRow()));
            table.delete(deleteInTableMetaData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

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
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] postDelete, table is {}, row put is {}",
                this.getCoprocName(),
                this.getTableSource(),
                AbstractPageProcessor.toHexString(delete.getRow()));
            List<Cell> cells = delete.getFamilyCellMap()
                .get(this.getTableSourceFamily());
            if (cells != null) {
                try (
                    Table metaDataPageTable = this.hbaseConnection.getTable(
                        TableName.valueOf(table2.getNamespaceAsString() + ":" + this.getTablePageForMetadata()));
                    Table metaDataTable = this.hbaseConnection
                        .getTable(TableName.valueOf(table2.getNamespaceAsString() + ":" + this.getTableMetaData()))) {
                    long lockNumber = Long.MIN_VALUE;
                    try {
                        lockNumber = this.getLock(metaDataPageTable);
                        cells.stream()
                            .map((c) -> CellUtil.cloneQualifier(c))
                            .peek(
                                (c) -> AbstractPageProcessor.LOGGER.info(
                                    "[COPROC][{}] postDelete, hex metadata to delete is {}",
                                    this.getCoprocName(),
                                    AbstractPageProcessor.toHexString(c)))
                            .filter((c) -> (c != null) && (c.length > 0))
                            .map((c) -> this.getMetaData(c))
                            .forEach((metaData) -> {
                                AbstractPageProcessor.LOGGER.info(
                                    "[COPROC][{}] postDelete, table is {}, metadata to delete is {}",
                                    this.getCoprocName(),
                                    this.getTableSource(),
                                    metaData);
                                this.deleteEntryInTableMetaData(metaDataTable, metaData);
                                this.updateTableOfImagesOfMetaDataOnDelete(
                                    delete,
                                    table2.getNamespaceAsString(),
                                    this.getTableImagesOfMetaData(),
                                    metaData);
                                this.deleteSecundaryIndex(
                                    observerContext.getEnvironment()
                                        .getRegion(),
                                    table2.getNamespaceAsString(),
                                    metaData,
                                    delete.getRow());
                            });
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
            try (
                Table metaDataPageTable = this.hbaseConnection.getTable(
                    TableName.valueOf(table2.getNamespaceAsString() + ":" + this.getTablePageForMetadata()))) {
                List<Cell> cells = delete.getFamilyCellMap()
                    .get(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] postDelete, table is {}, row put is {}",
                    this.getCoprocName(),
                    this.getTablePageForMetadata(),
                    AbstractPageProcessor.toHexString(delete.getRow()));
                if (cells != null) {
                    cells.stream()
                        .map((c) -> CellUtil.cloneQualifier(c))
                        .forEach(
                            (r) -> this.reorganizeIfNeeded(
                                metaDataPageTable,
                                observerContext.getEnvironment()
                                    .getRegion(),
                                delete.getRow(),
                                r,
                                table2.getNamespaceAsString()));
                }
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
        final TableName table2 = observerContext.getEnvironment()
            .getRegion()
            .getRegionInfo()
            .getTable();
        String table = table2.getNameAsString();
        if (table.endsWith(":" + this.getTableSource())) {
            AbstractPageProcessor.LOGGER.info("[COPROC][{}] Activate coprocessor ", this.getCoprocName());
            if (this.isAllowedForTableSource(put)) {
                List<Cell> cells = put.getFamilyCellMap()
                    .get(this.getTableSourceFamily());
                if (cells != null) {
                    try (
                        Table metaDataTable = this.hbaseConnection.getTable(
                            TableName.valueOf(table2.getNamespaceAsString() + ":" + this.getTableMetaData()))) {
                        cells.stream()
                            .map((c) -> CellUtil.cloneQualifier(c))
                            .filter((c) -> (c != null) && (c.length > 0))
                            .map((c) -> this.getMetaData(c))
                            .peek(
                                (c) -> AbstractPageProcessor.LOGGER
                                    .info("[COPROC][{}] processing metadata {}", this.getCoprocName(), c))
                            .forEach((metaData) -> {

                                this.createEntryInTableMetaData(metaDataTable, metaData);
                                this.createSecundaryIndex(
                                    observerContext.getEnvironment()
                                        .getRegion(),
                                    table2.getNamespaceAsString(),
                                    metaData,
                                    put.getRow());
                                this.updateTableOfImagesOfMetaDataOnInsert(
                                    put,
                                    table2.getNamespaceAsString(),
                                    this.getTableImagesOfMetaData(),
                                    metaData);
                            });

                        ;
                    }
                } else {
                    AbstractPageProcessor.LOGGER.info(
                        "[COPROC][{}] unable to process put of row {} because no cells in source family {}",
                        this.getCoprocName(),
                        AbstractPageProcessor.toHexString(put.getRow()),
                        this.getTableSourceFamily());

                }

            } else {
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] unable to process put of row {} because it is not allowed",
                    this.getCoprocName(),
                    AbstractPageProcessor.toHexString(put.getRow()));
            }
        } else if (table.endsWith(":" + this.getTablePageForMetadata())) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
            if (cells != null) {
                AbstractPageProcessor.LOGGER.info(
                    "[COPROC][{}] PostPut, cells are {}, {}, row is {} ",
                    this.getCoprocName(),
                    table,
                    cells,
                    AbstractPageProcessor.toHexString(put.getRow()));
                try (
                    Table tablePage = this.hbaseConnection.getTable(
                        TableName.valueOf(table2.getNamespaceAsString() + ':' + this.getTablePageForMetadata()))) {
                    this.updateTablePage(
                        tablePage,
                        observerContext.getEnvironment()
                            .getRegion(),
                        table2.getNamespaceAsString(),
                        put.getRow(),
                        this.extractMetadataValue(put.getRow()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

    protected abstract void createSecundaryIndex(Region region, String namespaceAsString, T metaData, byte[] row);

    protected abstract void deleteSecundaryIndex(Region region, String namespaceAsString, T metaData, byte[] row);

    protected abstract boolean shouldFamilyInsourceBeRecordedInMetadata(byte[] familySource);

    protected abstract byte[] buildTableMetaDataRowKey(T metaData, byte[] row);

    protected abstract ByteArrayComparable getByteArrayComparable(T t);

    protected abstract byte[] toRowKey(T t, long pageNumber);

    protected abstract long extractPageNumber(byte[] rowKey);

    protected abstract long extractDateOfRowKeyToIndex(byte[] rowKey);

    protected abstract T extractMetadataValue(byte[] rowKey);

    protected abstract String getCoprocName();

    protected abstract String getTablePageForMetadata();

    protected abstract byte[] getTableSourceFamily();

    protected abstract String getTableSource();

    protected abstract String getTableImagesOfMetaData();

    protected abstract String getTableMetaData();

    protected abstract boolean isAllowedForTableSource(Put put);

    protected abstract T getMetaData(byte[] cloneQualifier);

    protected static byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    protected static String toHexString(byte[] row) {
        StringBuilder strBuilder = new StringBuilder("[ ");
        strBuilder.append(" - l=");
        strBuilder.append(row.length);
        strBuilder.append(" - ");
        for (byte b : row) {
            strBuilder.append(Integer.toHexString((b & 0x000000ff)));
        }
        strBuilder.append(" ]");
        return strBuilder.toString();
    }

}
