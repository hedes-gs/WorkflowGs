package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.repositories.IPersonsRepository;

@Repository
public class PersonsRepository implements IPersonsRepository {

    @Autowired
    protected IHbasePersonsDAO personsDAO;

    @Override
    public List<String> getAllPersons() throws IOException { // TODO Auto-generated method stub
        return this.personsDAO.getAll()
            .stream()
            .map((t) -> t.getPerson())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllPersonsLike(String keyword) throws IOException {
        return this.personsDAO.getAllPersonsLike(keyword)
            .stream()
            .map((t) -> t.getPerson())
            .collect(Collectors.toList());
    }

}
