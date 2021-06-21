package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
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
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.HbaseRatings;
import com.workflow.model.ModelConstants;

import reactor.core.publisher.Flux;

public abstract class AbstractHbaseImagesOfRatingsDAO
    extends AbstractHbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Long> implements IImagesOfRatingsDAO {

    protected Logger              LOGGER               = LoggerFactory.getLogger(AbstractHbaseImagesOfRatingsDAO.class);
    protected static final String METADATA_FAMILY_NAME = "ratings";

    @Autowired
    protected IImageThumbnailDAO  hbaseImageThumbnailDAO;
    @Autowired
    protected IRatingsDAO         hbaseRatingsDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseRatingsDAO.flush();
    }

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, Long metaData) {
        try {
            hbi.getRatings()
                .clear();
            hbi.getRatings()
                .add(metaData);
            this.hbaseImageThumbnailDAO.put(hbi);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteMetaData(HbaseImageThumbnail hbi, Long metaData) {
        byte[] v = new byte[8];
        Bytes.putLong(v, 0, metaData);
        try {
            byte[] keyValue = this.hbaseImageThumbnailDAO.createKey(hbi);
            Delete del = new Delete(keyValue).addColumn(HbaseImageThumbnail.TABLE_FAMILY_RATINGS_AS_BYTES, v);
            this.hbaseImageThumbnailDAO.delete(del);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Flux<HbaseImageThumbnail> getAllImagesOfMetadata(Long rating, int pageNumber, int pageSize) {
        try {
            Table pageTable = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getPageTable());
            Table thumbTable = this.connection.getTable(this.hbaseImageThumbnailDAO.getTableName());
            return super.buildPageAsflux(
                AbstractHbaseImagesOfRatingsDAO.METADATA_FAMILY_NAME,
                rating,
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
    public Flux<HbaseImageThumbnail> getPrevious(Long meta, HbaseImageThumbnail hbi) throws IOException {
        return super.getPrevious(meta, hbi);
    }

    @Override
    public Flux<HbaseImageThumbnail> getNext(Long meta, HbaseImageThumbnail hbi) throws IOException {
        return super.getNext(meta, hbi);
    }

    @Override
    public void delete(HbaseImagesOfRatings hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                       // stub
    }

    @Override
    protected byte[] getRowKey(Long t, HbaseImageThumbnail hbi) {
        byte[] rowKey = this.hbaseImageThumbnailDAO.getKey(hbi);
        byte[] retValue = new byte[rowKey.length + ModelConstants.FIXED_WIDTH_RATINGS];
        final byte[] bytes = Bytes.toBytes(t);
        System.arraycopy(rowKey, 0, retValue, ModelConstants.FIXED_WIDTH_RATINGS, rowKey.length);
        System.arraycopy(bytes, 0, retValue, 0, bytes.length);
        return retValue;
    }

    @Override
    protected int getIndexOfImageRowKeyInTablePage() { return 0; }

    @Override
    protected TableName getMetaDataTable() { return this.hbaseRatingsDAO.getTableName(); }

    @Override
    protected byte[] getRowKeyForMetaDataTable(Long key) { return AbstractHbaseImagesOfMetadataDAO.convert(key); }

    @Override
    protected HbaseImageThumbnail toHbaseImageThumbnail(byte[] rowKey) {
        return this.hbaseImageThumbnailDAO.get(rowKey);
    }

    @Override
    protected HbaseImagesOfRatings build(Integer salt, Long meta, HbaseImageThumbnail t) {
        return HbaseImagesOfRatings.builder()
            .withThumbNailImage(t)
            .withRatings(meta)
            .build();

    }

    @Override
    protected byte[] toHbaseKey(HbaseImagesOfRatings him) { return this.getHbaseDataInformation()
        .buildKey(him); }

    @Override
    protected Get createGetQueryForTablePage(Long metaData, Integer x) {
        byte[] rowKey = new byte[8 + ModelConstants.FIXED_WIDTH_RATINGS];
        byte[] metaDataAsbytes = new byte[8];
        Bytes.putLong(metaDataAsbytes, 0, metaData);
        byte[] xAsbyte = new byte[8];
        Bytes.putLong(xAsbyte, 0, x);

        Arrays.fill(rowKey, (byte) 0x20);
        System.arraycopy(metaDataAsbytes, 0, rowKey, 0, metaDataAsbytes.length);
        System.arraycopy(xAsbyte, 0, rowKey, ModelConstants.FIXED_WIDTH_RATINGS, 8);
        Get get = new Get(rowKey);
        return get;
    }

    @Override
    protected byte[] metadataColumnValuetoByte(Long metaData) {
        final byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, metaData);
        return retValue;
    }

    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    public long countAll(HbaseImagesOfRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatingValue());
    }

    public long countAll(HbaseRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatings());
    }

}