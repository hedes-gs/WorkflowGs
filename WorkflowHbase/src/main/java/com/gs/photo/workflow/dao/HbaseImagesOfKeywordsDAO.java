package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.HbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfKeywords;
import com.workflow.model.ModelConstants;

@Component
public class HbaseImagesOfKeywordsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfKeywords, String>
    implements IHbaseImagesOfKeyWordsDAO {

    @Autowired
    protected IHbaseKeywordsDAO hbaseKeywordsDAO;

    @Override
    public void updateMetadata(HbaseImageThumbnail hbi, HbaseImageThumbnail previous) throws IOException {
        List<HbaseImagesOfKeywords> savedValue = hbi.getKeyWords()
            .stream()
            .map(
                (a) -> HbaseImagesOfKeywords.builder()
                    .withKeyword(a)
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
                    .build())
            .collect(Collectors.toList());
        savedValue.forEach((hba) -> {
            try {
                super.put(hba, super.getHbaseDataInformation());
                this.incrementNbOfImages(hba.getKeyword());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void incrementNbOfImages(String album) throws IOException {
        this.hbaseKeywordsDAO.incrementNbOfImages(album);
    }

    @Override
    public List<HbaseImagesOfKeywords> getAllImagesOfMetadata(String keyword) {
        List<HbaseImagesOfKeywords> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Filter f = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(keyword + ".*"));
            byte[] startKey = null;

            Scan scan = new Scan().withStartRow(startKey);
            scan.setFilter(f);
            HbaseImagesOfKeywords hbt = HbaseImagesOfKeywords.builder()
                .withKeyword(keyword)
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
            HbaseImagesOfAlbumDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        HbaseImagesOfAlbumDAO.LOGGER.info("-> end of getAllImagesOfAlbum for {} ", keyword);

        return retValue;

    }

    protected void buildImagesOfKeyword(List<HbaseImagesOfKeywords> retValue, Result t) {
        try {
            HbaseImagesOfKeywords instance = new HbaseImagesOfKeywords();
            this.getHbaseDataInformation()
                .build(instance, t);
            retValue.add(instance);
        } catch (IOException e) {
            HbaseImagesOfAlbumDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseKeywordsDAO.flush();
    }

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        this.hbaseKeywordsDAO.truncate();
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
        return ModelConstants.FIXED_WIDTH_KEYWORD + ModelConstants.FIXED_WIDTH_CREATION_DATE;
    }

    @Override
    protected int getLengthOfMetaDataKey() { return ModelConstants.FIXED_WIDTH_KEYWORD; }

    @Override
    protected int compare(HbaseImagesOfKeywords t1, HbaseImagesOfKeywords t2) {

        final long cmpOfCreationDate = t1.getCreationDate() - t2.getCreationDate();
        if (cmpOfCreationDate == 0) { return t1.getImageId()
            .compareTo(t2.getImageId()); }
        return (int) cmpOfCreationDate;
    }

}