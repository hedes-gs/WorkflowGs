package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CompareOperator;
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
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.HbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfPersons;
import com.workflow.model.ModelConstants;
import com.workflow.model.dtos.ImageDto;

@Component
public class HbaseImagesOfPersonsDAO extends HbaseImagesOfMetadataDAO<HbaseImagesOfPersons, String>
    implements IHbaseImagesOfPersonsDAO {

    protected static Logger    LOGGER = LoggerFactory.getLogger(HbaseImagesOfPersonsDAO.class);

    @Autowired
    protected IHbasePersonsDAO hbasePersonsDAO;

    @Override
    public List<HbaseImagesOfPersons> getAllImagesOfMetadata(String keyword) {
        List<HbaseImagesOfPersons> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(keyword.getBytes(Charset.forName("UTF-8")));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> this.buildImagesOfKeyword(retValue, t));
        } catch (IOException e) {
            HbaseImagesOfPersonsDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
        HbaseImagesOfPersonsDAO.LOGGER.info("-> end of getAllImagesOfAlbum for {} ", keyword);

        return retValue;

    }

    protected void buildImagesOfKeyword(List<HbaseImagesOfPersons> retValue, Result t) {
        HbaseImagesOfPersons instance = new HbaseImagesOfPersons();
        this.getHbaseDataInformation()
            .build(instance, t);
        retValue.add(instance);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
        this.hbasePersonsDAO.flush();
    }

    @Override
    protected int getOffsetOfImageId() {
        return ModelConstants.FIXED_WIDTH_PERSON_NAME + ModelConstants.FIXED_WIDTH_CREATION_DATE;
    }

    @Override
    protected int getLengthOfMetaDataKey() { return ModelConstants.FIXED_WIDTH_PERSON_NAME; }

    @Override
    protected int compare(HbaseImagesOfPersons t1, HbaseImagesOfPersons t2) {
        final long cmpOfCreationDate = t1.getCreationDate() - t2.getCreationDate();
        if (cmpOfCreationDate == 0) { return t1.getImageId()
            .compareTo(t2.getImageId()); }
        return (int) cmpOfCreationDate;
    }

    @Override
    public void deleteReferences(ImageDto imageToDelete) { // TODO Auto-generated method stub
    }

    @Override
    protected byte[] getMinRowProvider() { return this.getHbaseDataInformation()
        .getMinKey(); }

    @Override
    protected byte[] getMaxRowProvider() { return this.getHbaseDataInformation()
        .getMaxKey(); }

    @Override
    protected long getNbOfElements(String key) {
        try {
            return this.hbasePersonsDAO.countAll(key);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Filter getFilterFor(String key) {
        return new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(key, Pattern.CASE_INSENSITIVE));
    }

    @Override
    public void addMetaData(HbaseImageThumbnail hbi, String medataData) { // TODO Auto-generated method stub
    }

    @Override
    public void deleteMetaData(HbaseImageThumbnail hbi, String metaData) { // TODO Auto-generated method stub
    }

}