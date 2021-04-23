package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfRatingsDAO;

@Component
public class HbaseImagesOfRatingsDAO extends AbstractHbaseImagesOfRatingsDAO implements IHbaseImagesOfRatingsDAO {

    @Override
    public Map<String, Long> countAllPerRatings() throws IOException, Throwable { return null; }

}