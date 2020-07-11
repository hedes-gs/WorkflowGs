package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.HbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfKeywords;
import com.workflow.model.ModelConstants;

@Component
public class HbaseImagesOfKeywordsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfKeywords, String>
    implements IHbaseImagesOfKeywordsDAO {

    protected static Logger     LOGGER = LoggerFactory.getLogger(HbaseImagesOfKeywordsDAO.class);

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

    @Override
    public void updateMetadata(HbaseImageThumbnail hbi, String metaData) {
        try {
            HbaseImagesOfKeywords keywordOfHbi = HbaseImagesOfKeywords.builder()
                .withKeyword(metaData)
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
            super.put(keywordOfHbi, super.getHbaseDataInformation());
            this.incrementNbOfImages(keywordOfHbi.getKeyword());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeFromMetadata(HbaseImageThumbnail hbi, String keyword) throws IOException {
        HbaseImagesOfKeywords keywordOfHbi = HbaseImagesOfKeywords.builder()
            .withKeyword(keyword)
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
        super.delete(keywordOfHbi, super.getHbaseDataInformation());
        this.decrementNbOfImages(keywordOfHbi.getKeyword());
    }

    protected void incrementNbOfImages(String album) throws IOException {
        this.hbaseKeywordsDAO.incrementNbOfImages(album);
    }

    protected void decrementNbOfImages(String album) throws IOException {
        this.hbaseKeywordsDAO.decrementNbOfImages(album);
    }

    @Override
    public List<HbaseImagesOfKeywords> getAllImagesOfMetadata(String keyword) {
        List<HbaseImagesOfKeywords> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(keyword.getBytes(Charset.forName("UTF-8")));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> this.buildImagesOfKeyword(retValue, t));
        } catch (IOException e) {
            HbaseImagesOfKeywordsDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        HbaseImagesOfKeywordsDAO.LOGGER.info("-> end of getAllImagesOfAlbum for {} ", keyword);

        return retValue;

    }

    @Override
    public List<HbaseImagesOfKeywords> getAllImagesOfMetadata(String key, int first, int pageSize) { return null; }

    protected void buildImagesOfKeyword(List<HbaseImagesOfKeywords> retValue, Result t) {
        try {
            HbaseImagesOfKeywords instance = new HbaseImagesOfKeywords();
            this.getHbaseDataInformation()
                .build(instance, t);
            retValue.add(instance);
        } catch (IOException e) {
            HbaseImagesOfKeywordsDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbaseKeywordsDAO.flush();
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
    protected int compare(HbaseImagesOfKeywords t1, HbaseImagesOfKeywords t2) {
        final long cmpOfCreationDate = t1.getCreationDate() - t2.getCreationDate();
        if (cmpOfCreationDate == 0) { return t1.getImageId()
            .compareTo(t2.getImageId()); }
        return (int) cmpOfCreationDate;
    }

}