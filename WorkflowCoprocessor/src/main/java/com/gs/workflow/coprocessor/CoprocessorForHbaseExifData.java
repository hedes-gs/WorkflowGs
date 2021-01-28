package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoprocessorForHbaseExifData implements RegionCoprocessor, RegionObserver {
    protected RegionCoprocessorEnvironment env;

    protected static final int             FIXED_WIDTH_SALT_TAG   = 2;
    protected static final int             FIXED_WIDTH_IMAGE_ID   = 64;
    protected static final int             FIXED_WIDTH_EXIF_TAG   = 2;
    protected static final int             FIXED_WIDTH_EXIF_VALUE = 128;
    protected static final int             FIXED_WIDTH_EXIF_PATH  = 10;

    protected static Logger                LOGGER                 = LoggerFactory
        .getLogger(AbstractPageProcessor.class);
    protected Connection                   hbaseConnection;

    protected byte[]                       FAMILY_TO_EXCLUDE      = "thb".getBytes(Charset.forName("UTF-8"));

    protected Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(HBaseConfiguration.create());
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
        AbstractPageProcessor.LOGGER.info("STARTING Coprocessor CoprocessorForHbaseExifData ");
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() { return Optional.of(this); }

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
        if (table.endsWith(":image_exif")) {

            Put putInTableMetaData = new Put(this.buildTableMetaDataRowKey(put.getRow()));
            try (
                Table tableSecundary = this.hbaseConnection
                    .getTable(TableName.valueOf(table2.getNamespaceAsString() + ":image_exif_data_of_image"))) {
                put.getFamilyCellMap()
                    .entrySet()
                    .stream()
                    .filter((e) -> this.shouldFamilyInsourceBeRecordedInMetadata(e.getKey()))
                    .forEach(
                        (k) -> k.getValue()
                            .forEach(
                                (c) -> putInTableMetaData
                                    .addColumn(k.getKey(), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c))));
                tableSecundary.put(putInTableMetaData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean shouldFamilyInsourceBeRecordedInMetadata(byte[] key) {
        return !Objects.deepEquals(key, this.FAMILY_TO_EXCLUDE);
    }

    private byte[] buildTableMetaDataRowKey(byte[] row) {

        byte[] retValue = new byte[row.length];
        // Copy salt to HbaseExifDataOfImages
        System.arraycopy(row, 0, retValue, 0, CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG);

        // Copy imageId to HbaseExifDataOfImages
        System.arraycopy(
            row,
            CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG + CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_TAG
                + CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_PATH,
            retValue,
            CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG,
            CoprocessorForHbaseExifData.FIXED_WIDTH_IMAGE_ID);

        // Copy exif tag to HbaseExifDataOfImages
        System.arraycopy(
            row,
            CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG,
            retValue,
            CoprocessorForHbaseExifData.FIXED_WIDTH_IMAGE_ID + CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG,
            CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_TAG);

        // Copy exif path tag to HbaseExifDataOfImages
        System.arraycopy(
            row,
            CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG + CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_TAG,
            retValue,
            CoprocessorForHbaseExifData.FIXED_WIDTH_IMAGE_ID + CoprocessorForHbaseExifData.FIXED_WIDTH_SALT_TAG
                + CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_TAG,
            CoprocessorForHbaseExifData.FIXED_WIDTH_EXIF_PATH);

        return retValue;
    }

}
