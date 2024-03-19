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
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseAlbumDAO;
import com.workflow.model.HbaseAlbum;

@Component
public class HbaseAlbumDAO extends AbstractHbaseAlbumDAO implements IHbaseAlbumDAO {

    @Override
    public List<HbaseAlbum> getAllAlbumsLike(String keyword) throws IOException {
        HbasePersonsDAO.LOGGER.info(" Getting all Persons like {} ", keyword);
        List<HbaseAlbum> retValue = new ArrayList<>();
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
                    HbaseAlbum keyWord = new HbaseAlbum();
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
    public long countAll() throws IOException, Throwable { return 0; }

    @Override
    public void delete(HbaseAlbum hbaseData, String family, String column) { // TODO Auto-generated method stub
    }
}