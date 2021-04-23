package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.ws.repositories.IAlbumsRepository;

@Repository
public class AlbumsRepository implements IAlbumsRepository {

    @Autowired
    protected IHbaseAlbumDAO AlbumsDAO;

    @Override
    public List<String> getAllAlbums() throws IOException { // TODO Auto-generated method stub
        return this.AlbumsDAO.getAll()
            .stream()
            .map((t) -> t.getAlbumName())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllAlbumsLike(String keyword) throws IOException {
        return this.AlbumsDAO.getAllAlbumsLike(keyword)
            .stream()
            .map((t) -> t.getAlbumName())
            .collect(Collectors.toList());
    }

}
