package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Collection;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifDataOfImages;

@Component
public class HbaseExifDataOfImagesDAO extends GenericDAO<HbaseExifDataOfImages> {

    public void put(Collection<HbaseExifDataOfImages> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void put(HbaseExifDataOfImages hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifDataOfImages[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseExifDataOfImages hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
    }

    public int count() throws Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    public HbaseExifDataOfImages get(HbaseExifDataOfImages hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

}
