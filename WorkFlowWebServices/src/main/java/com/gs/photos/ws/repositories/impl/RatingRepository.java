package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.ws.repositories.IRatingRepository;

@Repository
public class RatingRepository implements IRatingRepository {

    @Autowired
    protected IHbaseRatingsDAO         hbaseRatingsDAO;

    @Autowired
    protected IHbaseImagesOfRatingsDAO hbaseImagesOfRatingsDAO;

    @Override
    public long count(long rating) throws IOException, Throwable { return this.hbaseRatingsDAO.countAll(rating); }

    @Override
    public Map<String, Long> countAll() throws IOException, Throwable {
        return this.hbaseImagesOfRatingsDAO.countAllPerRatings();
    }

}
