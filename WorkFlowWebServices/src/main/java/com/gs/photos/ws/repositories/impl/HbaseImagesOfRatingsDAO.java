package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.HbaseImagesOfMetadataDAO;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset.TypeValue;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.ModelConstants;

@Component
public class HbaseImagesOfRatingsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Long>
    implements IHbaseImagesOfRatingsDAO {

    protected Logger           LOGGER = LoggerFactory.getLogger(HbaseImagesOfRatingsDAO.class);

    @Autowired
    protected IHbaseRatingsDAO hbaseRatingsDAO;

    @Override
    public List<HbaseImagesOfRatings> getAllImagesOfMetadata(Long keyword) {
        List<HbaseImagesOfRatings> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Filter f = new FilterRowByLongAtAGivenOffset(0, keyword.longValue(), keyword.longValue(), TypeValue.INT);
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
    public Map<String, Long> countAllPerRatings() throws IOException, Throwable {
        Map<String, Long> retValue = new HashMap<>();
        for (long k = 1; k <= 5; k++) {
            retValue.put(Long.toString(k), this.hbaseRatingsDAO.countAll(k));
        }
        return retValue;
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
    public void addMetaData(HbaseImageThumbnail hbi, Long medataData) {}

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

    @Override
    public void deleteMetaData(HbaseImageThumbnail hbi, Long metaData) { // TODO Auto-generated method stub
    }

    @Override
    protected byte[] getMinRowProvider() { // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected byte[] getMaxRowProvider() { // TODO Auto-generated method stub
        return null;
    }

}