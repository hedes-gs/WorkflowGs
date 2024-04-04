package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifData;

import io.micrometer.core.annotation.Timed;

@Component
@TimedBean
public class HbaseExifDataDAO extends GenericDAO<HbaseExifData> {

    public HbaseExifDataDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

    protected HbaseDataInformation<HbaseExifData> hbaseDataInformation;

    protected byte[][] buildSplitKey(HbaseDataInformation<HbaseExifData> hdi) {
        byte[][] splitKeys = new byte[32][];
        BigInteger bi = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
        BigInteger intervall = bi.divide(BigInteger.valueOf(32));
        BigInteger biInit = BigInteger.valueOf(0L);
        for (short k = 0; k < AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE; k++) {
            splitKeys[k] = new byte[hdi.getKeyLength()];
            biInit = biInit.add(intervall);
            String rowImageId = biInit.toString(16);
            HbaseExifData currentRow = HbaseExifData.builder()
                .withCreationDate(
                    ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                        .toEpochSecond())
                .withRegionSalt(k)
                .withExifTag((short) 0xff)
                .withExifPath(new short[] { 0xff, 0xff })
                .withImageId(rowImageId)
                .build();
            hdi.buildKey(currentRow, splitKeys[k]);
        }
        return splitKeys;
    }

    @Override
    protected void createTablesIfNeeded(HbaseDataInformation<HbaseExifData> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
            byte[][] splitKeys = this.buildSplitKey(hdi);
            TableName tn = AbstractDAO.createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies(), splitKeys);
            hdi.setTable(tn);
            if (hdi.getPageTableName() != null) {
                TableName pageTn = this.createPageTableIfNeeded(admin, hdi.getPageTableName());
                hdi.setPageTable(pageTn);
            }
        }

    }

    @Timed
    public void put(Collection<HbaseExifData> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    @Override
    @Timed
    public void flush() throws IOException { super.flush(); }

    @Override
    public void put(HbaseExifData hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifData[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifData hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    public long count() throws Throwable { return super.countWithCoprocessorJob(this.getHbaseDataInformation()); }

    public HbaseExifData get(HbaseExifData hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

    @Override
    public void delete(HbaseExifData hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}
