package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Optional;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetadataLongCoprocessor extends AbstractPageProcessor<Long> {

    protected static Logger     LOGGER              = LoggerFactory.getLogger(AbstractMetadataLongCoprocessor.class);
    protected static final long PAGE_SIZE           = 1000L;
    public static final int     FIXED_WIDTH_RATINGS = 8;

    @Override
    public Optional<RegionObserver> getRegionObserver() { return Optional.of(this); }

    protected byte[] getLastRow(Table table, long pageNumber) throws IOException {
        Get get = new Get(AbstractPageProcessor.convert(pageNumber));
        get.addFamily(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
        Result res = table.get(get);
        NavigableMap<byte[], byte[]> elements = res.getFamilyMap(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY);
        if (elements != null) { return elements.lastEntry()
            .getKey(); }

        return null;
    }

    @Override
    protected void createSecundaryIndex(Region region, String namespace, Long metaData, byte[] rowToIndex) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + this.getTablePageForMetadata()))) {
            long pageNumber = this.findFirstPageWithAvailablePlace(tablePage, rowToIndex, metaData);
            Put put = new Put(this.toTablePageRowKey(metaData, pageNumber));
            put.addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, rowToIndex, new byte[] { 1 });
            tablePage.put(put);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void deleteSecundaryIndex(Region region, String namespaceAsString, Long metaData, byte[] rowToIndex) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespaceAsString + ':' + this.getTablePageForMetadata()))) {
            final Optional<Long> findPageOf = this.findPageOf(tablePage, rowToIndex);
            if (!findPageOf.isPresent()) {
                AbstractMetadataLongCoprocessor.LOGGER.info(
                    "[COPROC][{}] deleteSecundaryIndex page not found for  {}",
                    this.getCoprocName(),
                    AbstractPageProcessor.toHexString(rowToIndex));
            }
            findPageOf.ifPresent((pageNumber) -> {
                Delete delete = new Delete(this.toTablePageRowKey(metaData, pageNumber));
                delete.addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, rowToIndex);
                try {
                    tablePage.delete(delete);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected RowFilter getRowFilter(Long metaData) {
        AbstractMetadataStringCoprocessor.LOGGER.info("Creating row filter for {}", metaData);
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new LongComparator(metaData));
        return rowFilter;
    }

    @Override
    protected Long getMetaData(byte[] cloneQualifier) { return Bytes.toLong(cloneQualifier); }

    @Override
    protected ByteArrayComparable getByteArrayComparable(Long t) { return new LongComparator(t); }

    @Override
    protected byte[] buildTableMetaDataRowKey(Long metaData, byte[] row) {
        byte[] metaDataAsBytes = AbstractPageProcessor.convert(metaData);
        byte[] retValue = new byte[AbstractPageProcessor.FIXED_WIDTH_REGION_SALT
            + AbstractMetadataLongCoprocessor.FIXED_WIDTH_RATINGS + row.length];
        System.arraycopy(
            metaDataAsBytes,
            0,
            retValue,
            AbstractPageProcessor.FIXED_WIDTH_REGION_SALT,
            metaDataAsBytes.length);
        System.arraycopy(row, 0, retValue, 0, AbstractPageProcessor.FIXED_WIDTH_REGION_SALT);
        System.arraycopy(
            row,
            0,
            retValue,
            AbstractMetadataLongCoprocessor.FIXED_WIDTH_RATINGS + AbstractPageProcessor.FIXED_WIDTH_REGION_SALT,
            row.length);
        return retValue;
    }

}