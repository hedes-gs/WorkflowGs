package com.gs.photos.repositories;

import java.io.IOException;
import java.util.Map;

public interface IRatingRepository {

    long count(int rating) throws IOException, Throwable;

    Map<String, Integer> countAll() throws IOException, Throwable;
}
