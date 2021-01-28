package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfKeywords;
import com.workflow.model.ModelConstants;

public abstract class AbstractHbaseImagesOfKeywordsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfKeywords, String>
    implements IImagesOfKeyWordsDAO {

    private static Logger        LOGGER = LoggerFactory.getLogger(AbstractHbaseImagesOfKeywordsDAO.class);

    @Autowired
    protected IImageThumbnailDAO hbaseImageThumbnailDAO;
    @Autowired
    protected IKeywordsDAO       hbaseKeywordsDAO;

    @Override
    protected void initializePageTable(Table table) throws IOException {}

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, String metaData) {
        try {
            hbi.getKeyWords()
                .clear();
            hbi.getKeyWords()
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
                HbaseImageThumbnail.TABLE_FAMILY_KEYWORDS_AS_BYTES,
                metaData.getBytes(Charset.forName("UTF-8")));
            this.hbaseImageThumbnailDAO.delete(del);
            this.hbaseImageThumbnailDAO.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
                .build();
            byte[] keyStartValue = new byte[this.hbaseDataInformation.getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            scan.withStartRow(keyStartValue);

            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> this.buildImagesOfKeyword(retValue, t));
        } catch (IOException e) {
            AbstractHbaseImagesOfKeywordsDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        AbstractHbaseImagesOfKeywordsDAO.LOGGER.info("-> end of getAllImagesOfMetadata for {} ", keyword);

        return retValue;

    }

    protected void buildImagesOfKeyword(List<HbaseImagesOfKeywords> retValue, Result t) {
        HbaseImagesOfKeywords instance = new HbaseImagesOfKeywords();
        this.getHbaseDataInformation()
            .build(instance, t);
        retValue.add(instance);
    }

    @Override
    public void flush() throws IOException {
        try {
            super.flush();
            this.hbaseKeywordsDAO.flush();
        } catch (RetriesExhaustedWithDetailsException e) {
            AbstractHbaseImagesOfKeywordsDAO.LOGGER.error(" Retries error ! ", ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    @Override
    protected byte[] getMinRowProvider() {
        return this.getKey(
            HbaseImagesOfKeywords.builder()
                .withKeyword("")
                .withCreationDate(0)
                .withImageId(" ")
                .build());
    }

    @Override
    protected byte[] getMaxRowProvider() {
        return this.getKey(
            HbaseImagesOfKeywords.builder()
                .withKeyword("")
                .withCreationDate(Long.MAX_VALUE)
                .withImageId(" ")
                .build());
    }

    @Override
    protected long getNbOfElements(String key) {
        try {
            return this.hbaseKeywordsDAO.countAll(key);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter getFilterFor(String key) {
        return new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(key, Pattern.CASE_INSENSITIVE));
    }

}