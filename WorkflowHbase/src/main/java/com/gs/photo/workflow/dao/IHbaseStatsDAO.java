package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.List;

import com.workflow.model.HbaseImageThumbnailKey;

public interface IHbaseStatsDAO {

    void incrementDateInterval(String dateIntervall, HbaseImageThumbnailKey hbaseImageThumbnailKey) throws IOException;

    long countImages(String dateIntervall) throws IOException;

    List<HbaseImageThumbnailKey> getImages(String dateIntervall, int maxSize) throws IOException;

    void truncate() throws IOException;

    void flush() throws IOException;

}
