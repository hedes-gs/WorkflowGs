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

import com.gs.photo.common.workflow.hbase.dao.AbstractHbasePersonsDAO;
import com.workflow.model.HbasePersons;

@Component
public class HbasePersonsDAO extends AbstractHbasePersonsDAO implements IHbasePersonsDAO {

    protected static Logger LOGGER = LoggerFactory.getLogger(HbasePersonsDAO.class);

    @Override
    public List<HbasePersons> getAllPersonsLike(String keyword) throws IOException {
        HbasePersonsDAO.LOGGER.info(" Getting all Persons like {} ", keyword);
        List<HbasePersons> retValue = new ArrayList<>();
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
                    HbasePersons keyWord = new HbasePersons();
                    this.getHbaseDataInformation()
                        .build(keyWord, res);
                    retValue.add(keyWord);
                }
            }
        }
        HbasePersonsDAO.LOGGER.info(" End of Getting all Persons like {} , found : ", keyword, retValue);
        return retValue;
    }

    @Override
    public long countAll() throws IOException, Throwable { // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long countAll(HbasePersons metaData) throws IOException, Throwable { // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void delete(HbasePersons hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}
