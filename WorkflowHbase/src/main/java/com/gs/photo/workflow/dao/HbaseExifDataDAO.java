package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.gs.photo.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifData;
import com.workflow.model.ModelConstants;

@Component
public class HbaseExifDataDAO extends GenericDAO<HbaseExifData> {

    protected HbaseDataInformation<HbaseExifData> hbaseDataInformation;

    @Override
    protected void createTablesIfNeeded(HbaseDataInformation<HbaseExifData> hdi) throws IOException { // TODO
                                                                                                      // Auto-generated
                                                                                                      // method
                                                                                                      // stub
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());

            HbaseExifData first = HbaseExifData.builder()
                .withExifTag((short) 0)
                .withExifPath(new short[] { 0, 0 })
                .withImageId("")
                .build();
            HbaseExifData last = HbaseExifData.builder()
                .withExifTag((short) 0xFFFF)
                .withExifPath(new short[] { (short) 0xFFFF, (short) 0xFFFF })
                .withImageId("")
                .build();
            byte[] firstRow = new byte[ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID
                + ModelConstants.FIXED_WIDTH_EXIF_PATH];
            hdi.buildKey(first, firstRow);
            byte[] lastRow = new byte[ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID
                + ModelConstants.FIXED_WIDTH_EXIF_PATH];
            hdi.buildKey(last, lastRow);
            TableName tn = AbstractDAO
                .createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies(), firstRow, lastRow, 24);
            hdi.setTable(tn);
            if (hdi.getPageTableName() != null) {
                TableName pageTn = this.createPageTableIfNeeded(admin, hdi.getPageTableName());
                hdi.setPageTable(pageTn);
            }
        }

    }

    public void put(Collection<HbaseExifData> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

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

}
