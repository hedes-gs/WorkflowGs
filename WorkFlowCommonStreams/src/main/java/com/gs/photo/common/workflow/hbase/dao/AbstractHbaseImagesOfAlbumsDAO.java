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
import com.workflow.model.HbaseImagesOfAlbum;
import com.workflow.model.ModelConstants;

import reactor.core.publisher.Flux;

public abstract class AbstractHbaseImagesOfAlbumsDAO
    extends AbstractHbaseImagesOfMetadataDAO<HbaseImagesOfAlbum, String> implements IImagesOfAlbumDAO {

    protected static Logger       LOGGER                              = LoggerFactory
        .getLogger(AbstractHbaseImagesOfAlbumsDAO.class);
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements"
        .getBytes(Charset.forName("UTF-8"));
    protected static final String METADATA_FAMILY_NAME                = "albums";
    @Autowired
    protected IImageThumbnailDAO  hbaseImageThumbnailDAO;

    @Autowired
    protected IAlbumDAO           hbaseAlbumDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, String metaData) {
        try {
            hbi.getAlbums()
                .clear();
            hbi.getAlbums()
                .add(metaData);
            this.hbaseImageThumbnailDAO.put(hbi, AbstractHbaseImagesOfAlbumsDAO.METADATA_FAMILY_NAME);
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
                HbaseImageThumbnail.TABLE_FAMILY_ALBUMS_AS_BYTES,
                metaData.getBytes(Charset.forName("UTF-8")));
            this.hbaseImageThumbnailDAO.delete(del);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Flux<HbaseImageThumbnail> getAllImagesOfMetadata(String album, int pageNumber, int pageSize) {
        try {
            Table pageTable = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getPageTable());
            Table thumbTable = this.connection.getTable(this.hbaseImageThumbnailDAO.getTableName());
            return super.buildPageAsflux(
                AbstractHbaseImagesOfAlbumsDAO.METADATA_FAMILY_NAME,
                album,
                pageSize,
                pageNumber,
                pageTable,
                thumbTable).map((x) -> this.getFromHbaseImageThumbNail(x))
                    .doOnCancel(() -> { this.closeTables(pageTable, thumbTable); })
                    .doOnComplete(() -> { this.closeTables(pageTable, thumbTable); });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private HbaseImageThumbnail getFromHbaseImageThumbNail(byte[] x) {
        AbstractHbaseImagesOfAlbumsDAO.LOGGER.info("Get from {}", x);
        return this.hbaseImageThumbnailDAO.get(x);
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
    public void flush() throws IOException {
        super.flush();
        this.hbaseAlbumDAO.flush();
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
    public void delete(HbaseImagesOfAlbum hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

    @Override
    protected byte[] getRowKey(String t, HbaseImageThumbnail hbi) {
        try {
            byte[] rowKey = this.hbaseImageThumbnailDAO.getKey(hbi);
            byte[] retValue = new byte[rowKey.length + ModelConstants.FIXED_WIDTH_ALBUM_NAME];
            final byte[] bytes = t.getBytes("UTF-8");
            System.arraycopy(rowKey, 0, retValue, ModelConstants.FIXED_WIDTH_ALBUM_NAME, rowKey.length);
            System.arraycopy(bytes, 0, retValue, 0, bytes.length);
            return retValue;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected int getIndexOfImageRowKeyInTablePage() { return 0; }

    @Override
    protected TableName getMetaDataTable() { return this.hbaseAlbumDAO.getTableName(); }

    @Override
    protected byte[] getRowKeyForMetaDataTable(String metadata) {
        return Arrays.copyOf(metadata.getBytes(Charset.forName("UTF-8")), ModelConstants.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected HbaseImageThumbnail toHbaseImageThumbnail(byte[] rowKey) {
        return this.getFromHbaseImageThumbNail(rowKey);
    }

    @Override
    protected HbaseImagesOfAlbum build(Integer salt, String meta, HbaseImageThumbnail t) {
        return HbaseImagesOfAlbum.builder()
            .withThumbNailImage(t)
            .withAlbumName(meta)
            .build();

    }

    @Override
    protected byte[] toHbaseKey(HbaseImagesOfAlbum him) { return this.getHbaseDataInformation()
        .buildKey(him); }

    @Override
    protected Get createGetQueryForTablePage(String metaData, Integer x) {
        try {
            AbstractHbaseImagesOfAlbumsDAO.LOGGER.info("Create Get query for album {} and page {} ,", metaData, x);
            byte[] rowKey = new byte[8 + ModelConstants.FIXED_WIDTH_ALBUM_NAME];
            byte[] metaDataAsbytes = metaData.getBytes("UTF-8");
            byte[] xAsbyte = new byte[8];
            Bytes.putLong(xAsbyte, 0, x);
            System.arraycopy(metaDataAsbytes, 0, rowKey, 0, metaDataAsbytes.length);
            System.arraycopy(xAsbyte, 0, rowKey, ModelConstants.FIXED_WIDTH_ALBUM_NAME, 8);
            Get get = new Get(rowKey);
            return get;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
