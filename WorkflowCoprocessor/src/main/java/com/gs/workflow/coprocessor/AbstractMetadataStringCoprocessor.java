package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.NavigableMap;
import java.util.Optional;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetadataStringCoprocessor extends AbstractPageProcessor<String> {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractMetadataStringCoprocessor.class);

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
    protected void createSecundaryIndex(Region region, String namespace, String metaData, byte[] rowToIndex) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespace + ':' + this.getTablePageForMetadata()))) {

            long pageNumber = this.findFirstPageWithAvailablePlace(tablePage, rowToIndex, metaData);
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] createSecundaryIndex, region is {}, metadata is {},  row put is {}, page number found {}",
                this.getCoprocName(),
                region,
                metaData,
                AbstractPageProcessor.toHexString(rowToIndex),
                pageNumber);

            Put put = new Put(this.toRowKey(metaData, pageNumber));
            put.addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, rowToIndex, new byte[] { 1 });
            tablePage.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void deleteSecundaryIndex(Region region, String namespaceAsString, String metaData, byte[] rowToIndex) {
        try (
            Table tablePage = this.hbaseConnection
                .getTable(TableName.valueOf(namespaceAsString + ':' + this.getTablePageForMetadata()))) {
            AbstractPageProcessor.LOGGER.info(
                "[COPROC][{}] deleteSecundaryIndex, region is {}, metadata is {},  row put is {}",
                this.getCoprocName(),
                region,
                metaData,
                AbstractPageProcessor.toHexString(rowToIndex));
            final Optional<Long> findPageOf = this.findPageOf(tablePage, rowToIndex);
            if (!findPageOf.isPresent()) {
                AbstractMetadataLongCoprocessor.LOGGER.info(
                    "[COPROC][{}] deleteSecundaryIndex page not found for  {}",
                    this.getCoprocName(),
                    AbstractPageProcessor.toHexString(rowToIndex));
            }
            findPageOf.ifPresent((pageNumber) -> {
                try {
                    Delete delete = new Delete(this.toRowKey(metaData, pageNumber));
                    delete.addColumn(AbstractPageProcessor.TABLE_PAGE_LIST_COLUMN_FAMILY, rowToIndex);
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
    protected RowFilter getRowFilter(String metaData) {
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(metaData));
        return rowFilter;
    }

    @Override
    protected ByteArrayComparable getByteArrayComparable(String t) { return new RegexStringComparator(t); }

    @Override
    protected String getMetaData(byte[] cloneQualifier) { return new String(cloneQualifier, Charset.forName("UTF-8")); }

}