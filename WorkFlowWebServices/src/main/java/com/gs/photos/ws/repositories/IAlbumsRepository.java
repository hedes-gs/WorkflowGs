package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.util.List;

public interface IAlbumsRepository {

    List<String> getAllAlbums() throws IOException;

    List<String> getAllAlbumsLike(String person) throws IOException;

}
