package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfAlbum;
import com.workflow.model.ModelConstants;

public abstract class AbstractHbaseImagesOfAlbumDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfAlbum, String>
    implements IImagesOfAlbumDAO {

    protected static Logger           LOGGER = LoggerFactory.getLogger(AbstractHbaseImagesOfAlbumDAO.class);

    @Autowired
    protected IImageThumbnailDAO hbaseImageThumbnailDAO;

    @Autowired
    protected IAlbumDAO               hbaseAlbumDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, String metaData) {
        try {
            hbi.getAlbums()
                .clear();
            hbi.getAlbums()
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
            Delete del = new Delete(keyValue).addColumn(
                HbaseImageThumbnail.TABLE_FAMILY_ALBUMS_AS_BYTES,
                metaData.getBytes(Charset.forName("UTF-8")));
            this.hbaseImageThumbnailDAO.delete(del);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<HbaseImagesOfAlbum> getAllImagesOfMetadata(String album) {
        List<HbaseImagesOfAlbum> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Filter f = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(album + ".*"));

            HbaseImagesOfAlbum hbt = HbaseImagesOfAlbum.builder()
                .withAlbumName(album)
                .withCreationDate(0)
                .withImageId("")
                .build();
            byte[] keyStartValue = new byte[this.hbaseDataInformation.getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            Scan scan = new Scan().withStartRow(keyStartValue)
                .setFilter(f);

            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> this.buildImageOfHbaseImagesOfalbum(retValue, t));
        } catch (IOException e) {
            AbstractHbaseImagesOfAlbumDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        AbstractHbaseImagesOfAlbumDAO.LOGGER.info("-> end of getAllImagesOfAlbum for {} ", album);

        return retValue;

    }

    protected void buildImageOfHbaseImagesOfalbum(List<HbaseImagesOfAlbum> retValue, Result t) {
        HbaseImagesOfAlbum instance = new HbaseImagesOfAlbum();
        this.getHbaseDataInformation()
            .build(instance, t);
        retValue.add(instance);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseAlbumDAO.flush();
    }

    @Override
    protected int getOffsetOfImageId() {
        return ModelConstants.FIXED_WIDTH_ALBUM_NAME + ModelConstants.FIXED_WIDTH_CREATION_DATE;
    }

    @Override
    protected int getLengthOfMetaDataKey() { return ModelConstants.FIXED_WIDTH_ALBUM_NAME; }

    @Override
    protected int compare(HbaseImagesOfAlbum t1, HbaseImagesOfAlbum t2) {

        final long cmpOfCreationDate = t1.getCreationDate() - t2.getCreationDate();
        if (cmpOfCreationDate == 0) { return t1.getImageId()
            .compareTo(t2.getImageId()); }
        return (int) cmpOfCreationDate;
    }

    @Override
    public List<HbaseImagesOfAlbum> getAllImagesOfMetadata(String key, int first, int pageSize) { // TODO Auto-generated
                                                                                                  // method stub
        return null;
    }

    @Override
    protected byte[] getMinRowProvider() {
        try {
            return this.getKey(
                HbaseImagesOfAlbum.builder()
                    .withAlbumName("")
                    .withCreationDate(0)
                    .withImageId(" ")
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected byte[] getMaxRowProvider() {
        try {
            return this.getKey(
                HbaseImagesOfAlbum.builder()
                    .withAlbumName("")
                    .withCreationDate(Long.MAX_VALUE)
                    .withImageId(" ")
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected long getNbOfElements(String key) {
        try {
            return (int) this.hbaseAlbumDAO.countAll(key);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter getFilterFor(String key) {
        return new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(key, Pattern.CASE_INSENSITIVE));
    }

}
