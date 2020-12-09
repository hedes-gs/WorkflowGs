package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.workflow.dao.IImageThumbnailDAO;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset.TypeValue;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.ModelConstants;

public abstract class AbstractHbaseImagesOfRatingsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Long>
    implements IImagesOfRatingsDAO {

    protected Logger                  LOGGER = LoggerFactory.getLogger(AbstractHbaseImagesOfRatingsDAO.class);
    @Autowired
    protected IImageThumbnailDAO hbaseImageThumbnailDAO;
    @Autowired
    protected IRatingsDAO             hbaseRatingsDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public List<HbaseImagesOfRatings> getAllImagesOfMetadata(Long keyword) {
        List<HbaseImagesOfRatings> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Filter f = new FilterRowByLongAtAGivenOffset(0, keyword.longValue(), keyword.longValue(), TypeValue.LONG);
            byte[] startKey = null;

            Scan scan = new Scan().withStartRow(startKey);
            scan.setFilter(f);
            HbaseImagesOfRatings hbt = HbaseImagesOfRatings.builder()
                .withRatings(keyword)
                .withCreationDate(0)
                .withImageId("")
                .build();
            byte[] keyStartValue = new byte[this.hbaseDataInformation.getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            scan.withStartRow(keyStartValue);

            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> this.buildImagesOfKeyword(retValue, t));
        } catch (IOException e) {
            this.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        this.LOGGER.info("-> end of getAllImagesOfAlbum for {} ", keyword);

        return retValue;

    }

    protected void buildImagesOfKeyword(List<HbaseImagesOfRatings> retValue, Result t) {
        HbaseImagesOfRatings instance = new HbaseImagesOfRatings();
        this.getHbaseDataInformation()
            .build(instance, t);
        retValue.add(instance);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseRatingsDAO.flush();
    }

    @Override
    protected int getOffsetOfImageId() {
        return ModelConstants.FIXED_WIDTH_RATINGS + ModelConstants.FIXED_WIDTH_CREATION_DATE;
    }

    @Override
    protected int getLengthOfMetaDataKey() { return ModelConstants.FIXED_WIDTH_RATINGS; }

    @Override
    protected int compare(HbaseImagesOfRatings t1, HbaseImagesOfRatings t2) {
        final long cmpOfCreationDate = t1.getCreationDate() - t2.getCreationDate();
        if (cmpOfCreationDate == 0) { return t1.getImageId()
            .compareTo(t2.getImageId()); }
        return (int) cmpOfCreationDate;
    }

    @Override
    protected byte[] getMinRowProvider() {
        try {
            return this.getKey(
                HbaseImagesOfRatings.builder()
                    .withRatings(0L)
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
                HbaseImagesOfRatings.builder()
                    .withRatings(Long.MAX_VALUE)
                    .withCreationDate(Long.MAX_VALUE)
                    .withImageId(" ")
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    protected long getNbOfElements(Long key) {
        try {
            return this.hbaseRatingsDAO.countAll(key);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter getFilterFor(Long key) { return new PrefixFilter(Bytes.toBytes(key)); }

}