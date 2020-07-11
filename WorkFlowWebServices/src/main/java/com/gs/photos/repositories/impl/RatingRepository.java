package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.repositories.IRatingRepository;

@Repository
public class RatingRepository implements IRatingRepository {

    @Autowired
    protected IHbaseRatingsDAO         hbaseRatingsDAO;

    @Autowired
    protected IHbaseImagesOfRatingsDAO hbaseImagesOfRatingsDAO;

    @Override
    public long count(int rating) throws IOException, Throwable { return this.hbaseRatingsDAO.countAll(rating); }

    @Override
    public Map<String, Integer> countAll() throws IOException, Throwable {
        return this.hbaseImagesOfRatingsDAO.countAllPerRatings();
    }

}
