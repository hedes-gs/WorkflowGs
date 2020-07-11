package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.HbaseImagesOfMetadataDAO;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset.TypeValue;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfRatings;
import com.workflow.model.ModelConstants;

@Component
public class HbaseImagesOfRatingsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Integer>
    implements IHbaseImagesOfRatingsDAO {

    protected Logger           LOGGER = LoggerFactory.getLogger(HbaseImagesOfRatingsDAO.class);

    @Autowired
    protected IHbaseRatingsDAO hbaseRatingsDAO;

    @Override
    public void updateMetadata(HbaseImageThumbnail hbi, HbaseImageThumbnail previous) throws IOException {
        HbaseImagesOfRatings hba = HbaseImagesOfRatings.builder()
            .withRatings(hbi.getRatings())
            .withCreationDate(hbi.getCreationDate())
            .withHeight(hbi.getHeight())
            .withImageId(hbi.getImageId())
            .withImageName(hbi.getImageName())
            .withImportDate(hbi.getImportDate())
            .withOrientation(hbi.getOrientation())
            .withOriginalHeight(hbi.getOriginalHeight())
            .withOriginalWidth(hbi.getOriginalWidth())
            .withPath(hbi.getPath())
            .withThumbnail(hbi.getThumbnail())
            .withThumbName(hbi.getThumbName())
            .withVersion(hbi.getVersion())
            .withWidth(hbi.getWidth())
            .build();
        try {
            super.put(hba, super.getHbaseDataInformation());
            this.incrementNbOfImages(hba.getRatings());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void incrementNbOfImages(int key) throws IOException { this.hbaseRatingsDAO.incrementNbOfImages(key); }

    @Override
    public List<HbaseImagesOfRatings> getAllImagesOfMetadata(Integer keyword) {
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
                .withVersion((short) 0)
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
        try {
            HbaseImagesOfRatings instance = new HbaseImagesOfRatings();
            this.getHbaseDataInformation()
                .build(instance, t);
            retValue.add(instance);
        } catch (IOException e) {
            this.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseRatingsDAO.flush();
    }

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        this.hbaseRatingsDAO.truncate();
    }

    @Override
    protected Scan createScanToGetAllColumns() {
        Scan scan = new Scan().addFamily(HbaseImagesOfMetadataDAO.FAMILY_IMG_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_THB_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_SZ_AS_BYTES);
        return scan;
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

}