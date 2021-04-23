package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImportDAO;
import com.workflow.model.HbaseImport;

@Component
public class HbaseImportDAO extends AbstractHbaseImportDAO implements IHbaseImportDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    public void delete(HbaseImport hbaseData, String family, String column) {}

}