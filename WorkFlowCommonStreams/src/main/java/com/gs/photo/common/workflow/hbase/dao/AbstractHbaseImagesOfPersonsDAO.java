package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfPersons;
import com.workflow.model.ModelConstants;

import reactor.core.publisher.Flux;

public abstract class AbstractHbaseImagesOfPersonsDAO
    extends AbstractHbaseImagesOfMetadataDAO<HbaseImagesOfPersons, String> implements IImagesOfPersonsDAO {

    protected static Logger       LOGGER               = LoggerFactory.getLogger(AbstractHbaseImagesOfPersonsDAO.class);
    protected static final String METADATA_FAMILY_NAME = "persons";

    @Autowired
    protected IImageThumbnailDAO  hbaseImageThumbnailDAO;

    @Autowired
    protected IPersonsDAO         hbasePersonDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, String metaData) {
        try {
            hbi.getPersons()
                .clear();
            hbi.getPersons()
                .add(metaData);
            this.hbaseImageThumbnailDAO.put(hbi);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteMetaData(HbaseImageThumbnail hbi, String metaData) {
        try {
            byte[] keyValue = this.hbaseImageThumbnailDAO.createKey(hbi);
            Delete del = new Delete(keyValue).addColumns(
                HbaseImageThumbnail.TABLE_FAMILY_PERSONS_AS_BYTES,
                metaData.getBytes(Charset.forName("UTF-8")));
            this.hbaseImageThumbnailDAO.delete(del);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbasePersonDAO.flush();
    }

    @Override
    public Flux<HbaseImageThumbnail> getAllImagesOfMetadata(String person, int pageNumber, int pageSize) {
        try {
            Table pageTable = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getPageTable());
            Table thumbTable = this.connection.getTable(this.hbaseImageThumbnailDAO.getTableName());
            return super.buildPageAsflux(
                AbstractHbaseImagesOfPersonsDAO.METADATA_FAMILY_NAME,
                person,
                pageSize,
                pageNumber,
                pageTable,
                thumbTable).map((x) -> this.hbaseImageThumbnailDAO.get(x))
                    .doOnCancel(() -> { this.closeTables(pageTable, thumbTable); })
                    .doOnComplete(() -> { this.closeTables(pageTable, thumbTable); });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected void closeTables(Table pageTable, Table thumbTable) {
        try {
            pageTable.close();
            thumbTable.close();
        } catch (IOException e) {

            e.printStackTrace();
        }
    }

    @Override
    public Flux<HbaseImageThumbnail> getPrevious(String meta, HbaseImageThumbnail hbi) throws IOException {
        return super.getPrevious(meta, hbi);
    }

    @Override
    public Flux<HbaseImageThumbnail> getNext(String meta, HbaseImageThumbnail hbi) throws IOException {
        return super.getNext(meta, hbi);
    }

    @Override
    public void delete(HbaseImagesOfPersons hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                       // stub
    }

    @Override
    protected byte[] getRowKey(String t, HbaseImageThumbnail hbi) {
        try {
            byte[] rowKey = this.hbaseImageThumbnailDAO.getKey(hbi);
            byte[] retValue = new byte[rowKey.length + ModelConstants.FIXED_WIDTH_PERSON_NAME];
            final byte[] bytes = t.getBytes("UTF-8");
            System.arraycopy(rowKey, 0, retValue, ModelConstants.FIXED_WIDTH_PERSON_NAME, rowKey.length);
            System.arraycopy(bytes, 0, retValue, 0, bytes.length);
            return retValue;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected int getIndexOfImageRowKeyInTablePage() { return 0; }

    @Override
    protected TableName getMetaDataTable() { return this.hbasePersonDAO.getTableName(); }

    @Override
    protected byte[] getRowKeyForMetaDataTable(String metadata) {
        return Arrays.copyOf(metadata.getBytes(Charset.forName("UTF-8")), ModelConstants.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected HbaseImageThumbnail toHbaseImageThumbnail(byte[] rowKey) {
        return this.hbaseImageThumbnailDAO.get(rowKey);
    }

    @Override
    protected HbaseImagesOfPersons build(Integer salt, String meta, HbaseImageThumbnail t) {
        return HbaseImagesOfPersons.builder()
            .withThumbNailImage(t)
            .withPerson(meta)
            .build();

    }

    @Override
    protected byte[] toHbaseKey(HbaseImagesOfPersons him) { return this.getHbaseDataInformation()
        .buildKey(him); }

    @Override
    protected Get createGetQueryForTablePage(String metaData, Integer x) {
        try {
            byte[] rowKey = new byte[8 + ModelConstants.FIXED_WIDTH_PERSON_NAME];
            byte[] metaDataAsbytes = metaData.getBytes("UTF-8");
            byte[] xAsbyte = new byte[8];
            Bytes.putLong(xAsbyte, 0, x);

            Arrays.fill(rowKey, (byte) 0x20);
            System.arraycopy(metaDataAsbytes, 0, rowKey, 0, metaDataAsbytes.length);
            System.arraycopy(xAsbyte, 0, rowKey, ModelConstants.FIXED_WIDTH_PERSON_NAME, 8);
            Get get = new Get(rowKey);
            return get;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
