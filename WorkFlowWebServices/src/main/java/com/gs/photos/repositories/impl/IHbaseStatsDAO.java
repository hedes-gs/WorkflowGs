package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.dtos.MinMaxDatesDto;

public interface IHbaseStatsDAO {

    long countImages(KeyEnumType type) throws IOException;

    List<HbaseImageThumbnailKey> getImages(String dateIntervall, int maxSize) throws IOException;

    public long countImages(String min, String max) throws IOException;

    long countImages(OffsetDateTime min, OffsetDateTime max, KeyEnumType type) throws IOException;

    public MinMaxDatesDto getMinMaxDates();

    List<MinMaxDatesDto> getDatesBetween(OffsetDateTime startTime, OffsetDateTime stopDate, KeyEnumType year)
        throws IOException;
}
