package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.repositories.IKeywordsRepository;

@Repository
public class KeywordsRepository implements IKeywordsRepository {

    @Autowired
    protected IHbaseKeywordsDAO keywordsDAO;

    @Override
    public List<String> getAllKeywords() throws IOException { // TODO Auto-generated method stub
        return this.keywordsDAO.getAll()
            .stream()
            .map((t) -> t.getKeyword())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllKeywordsLike(String keyword) throws IOException {
        return this.keywordsDAO.getAllKeywordsLike(keyword)
            .stream()
            .map((t) -> t.getKeyword())
            .collect(Collectors.toList());
    }

}
