package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseKeywordsDAO;
import com.workflow.model.HbaseKeywords;

@Component
public class HbaseKeywordsDAO extends AbstractHbaseKeywordsDAO implements IHbaseKeywordsDAO {

    protected static Logger LOGGER = LoggerFactory.getLogger(HbaseKeywordsDAO.class);

    @Override
    protected byte[] createKey(String keyword) throws IOException {
        HbaseKeywords hbaseKeywords = HbaseKeywords.builder()
            .withKeyword(keyword)
            .build();

        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(hbaseKeywords, keyValue);
        return keyValue;
    }

    @Override
    public List<HbaseKeywords> getAllKeywordsLike(String keyword) throws IOException {
        HbaseKeywordsDAO.LOGGER.info(" Getting all keywords like {} ", keyword);
        List<HbaseKeywords> retValue = new ArrayList<>();
        Scan scan = new Scan();
        Filter f = new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(".*" + keyword + ".*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL));
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        try (
            Table t = this.connection.getTable(tableName)) {
            scan.setFilter(f);
            try (
                ResultScanner scanner = t.getScanner(scan)) {
                for (Result res : scanner) {
                    HbaseKeywords keyWord = new HbaseKeywords();
                    this.getHbaseDataInformation()
                        .build(keyWord, res);
                    retValue.add(keyWord);
                }
            }
        }
        HbaseKeywordsDAO.LOGGER.info(" End of Getting all keywords like {} , found : ", keyword, retValue);
        return retValue;
    }

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public long countAll(HbaseKeywords metaData) throws IOException, Throwable {
        return super.countAll(metaData.getKeyword());
    }

}
