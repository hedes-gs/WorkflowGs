package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;

import com.gsphotos.worflow.hbasefilters.FilterRowFindNextRowWithinAMetaData;
import com.workflow.model.HbaseData;
import com.workflow.model.ModelConstants;

public abstract class HbaseImagesOfMetadataDAO<T1 extends HbaseData, T2> extends GenericDAO<T1>
    implements IHbaseImagesOfMetadataDAO<T1, T2> {

    protected static final byte[] FAMILY_SZ_AS_BYTES  = "sz".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_THB_AS_BYTES = "thb".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] FAMILY_IMG_AS_BYTES = "img".getBytes(Charset.forName("UTF-8"));

    @Override
    public Optional<T1> getPrevious(T1 metaData) throws IOException {
        byte[] key = this.getKey(metaData);
        return this.getPreviousMetaDataOf(key)
            .stream()
            .sorted((t1, t2) -> this.compare(t1, t2))
            .findFirst();
    }

    @Override
    public Optional<T1> getNext(T1 metaData) throws IOException {
        byte[] key = this.getKey(metaData);
        return this.getPreviousMetaDataOf(key)
            .stream()
            .sorted((t1, t2) -> this.compare(t2, t1))
            .findFirst();
    }

    protected List<T1> getPreviousMetaDataOf(byte[] keyOfMetadata) {
        List<T1> retValue = new ArrayList<>();
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {

            Scan scan = this.createScanToGetAllColumns();
            scan.withStartRow(keyOfMetadata);
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithinAMetaData(keyOfMetadata,
                        this.getLengthOfMetaDataKey(),
                        this.getOffsetOfImageId(),
                        ModelConstants.FIXED_WIDTH_IMAGE_ID),
                    new PageFilter(1)));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                T1 instance;
                try {
                    instance = this.hbaseDataInformation.newInstance();
                    this.hbaseDataInformation.build(instance, t);
                    retValue.add(instance);
                } catch (
                    InstantiationException |
                    IllegalAccessException |
                    IllegalArgumentException |
                    InvocationTargetException |
                    NoSuchMethodException |
                    SecurityException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    protected List<T1> getNextMetaDataOf(byte[] keyOfMetadata) {
        List<T1> retValue = new ArrayList<>();
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            Scan scan = this.createScanToGetAllColumns();
            scan.setReversed(true);
            scan.withStartRow(keyOfMetadata);
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithinAMetaData(keyOfMetadata,
                        this.getLengthOfMetaDataKey(),
                        this.getOffsetOfImageId(),
                        ModelConstants.FIXED_WIDTH_IMAGE_ID),
                    new PageFilter(1)));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                T1 instance;
                try {
                    instance = this.hbaseDataInformation.newInstance();
                    this.getHbaseDataInformation()
                        .build(instance, t);
                    retValue.add(instance);
                } catch (
                    IOException |
                    InstantiationException |
                    IllegalAccessException |
                    IllegalArgumentException |
                    InvocationTargetException |
                    NoSuchMethodException |
                    SecurityException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    protected abstract Scan createScanToGetAllColumns();

    protected abstract int getOffsetOfImageId();

    protected abstract int getLengthOfMetaDataKey();

    protected abstract int compare(T1 t1, T1 t2);

}
