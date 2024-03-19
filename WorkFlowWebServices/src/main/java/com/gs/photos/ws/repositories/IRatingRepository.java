package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.util.Map;

public interface IRatingRepository {

    long count(long rating) throws IOException, Throwable;

    Map<String, Long> countAll() throws IOException, Throwable;
}
