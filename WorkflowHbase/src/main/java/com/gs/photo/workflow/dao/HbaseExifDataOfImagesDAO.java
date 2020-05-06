package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Collection;

import org.springframework.stereotype.Component;

import com.workflow.model.HbaseExifDataOfImages;

@Component
public class HbaseExifDataOfImagesDAO extends GenericDAO<HbaseExifDataOfImages> {

    public void put(Collection<HbaseExifDataOfImages> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public void put(HbaseExifDataOfImages hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public void delete(HbaseExifDataOfImages[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public void delete(HbaseExifDataOfImages hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public int count() throws Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

    public HbaseExifDataOfImages get(HbaseExifDataOfImages hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation(HbaseExifDataOfImages.class));
    }

}
