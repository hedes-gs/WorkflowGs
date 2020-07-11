package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Collection;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifData;

@Component
public class HbaseExifDataDAO extends GenericDAO<HbaseExifData> {

    protected HbaseDataInformation<HbaseExifData> hbaseDataInformation;

    public void put(Collection<HbaseExifData> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void put(HbaseExifData hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifData[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifData hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    public int count() throws Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    public HbaseExifData get(HbaseExifData hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

}
