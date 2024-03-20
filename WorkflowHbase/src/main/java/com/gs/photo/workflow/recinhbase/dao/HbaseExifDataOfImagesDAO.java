package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifDataOfImages;

@Component
public class HbaseExifDataOfImagesDAO extends GenericDAO<HbaseExifDataOfImages> {

    public HbaseExifDataOfImagesDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

    protected byte[][] buildSplitKey(HbaseDataInformation<HbaseExifDataOfImages> hdi) {
        byte[][] splitKeys = new byte[32][];
        BigInteger bi = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
        BigInteger intervall = bi.divide(BigInteger.valueOf(32));
        BigInteger biInit = BigInteger.valueOf(0L);
        for (short k = 0; k < AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE; k++) {
            splitKeys[k] = new byte[hdi.getKeyLength()];
            biInit = biInit.add(intervall);
            String rowImageId = biInit.toString(16);
            HbaseExifDataOfImages currentRow = HbaseExifDataOfImages.builder()
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
    protected void createTablesIfNeeded(HbaseDataInformation<HbaseExifDataOfImages> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.LOGGER.info("Creating table {} and {}", hdi.getTableName(), hdi.getPageTableName());
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

    public void put(Collection<HbaseExifDataOfImages> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    @Override
    public void put(HbaseExifDataOfImages hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifDataOfImages[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifDataOfImages hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    public long count() throws Throwable { return super.countWithCoprocessorJob(this.getHbaseDataInformation()); }

    public HbaseExifDataOfImages get(HbaseExifDataOfImages hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

    @Override
    public void delete(HbaseExifDataOfImages hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                        // stub
    }

}
