package com.gs.photos.ws.controllers;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gs.photos.ws.repositories.IPersonsRepository;
import com.gs.photos.ws.web.assembler.PersonsAssembler;

@RestController
@RequestMapping("/api/gs/persons")
public class PersonsController {

    protected static Logger      LOGGER = LoggerFactory.getLogger(PersonsController.class);

    @Autowired
    protected PersonsAssembler   personsAssembler;

    @Autowired
    protected IPersonsRepository personsRepository;

    @GetMapping(path = "/all")
    public CollectionModel<EntityModel<String>> getAllPersons() throws IOException {
        return this.personsAssembler.toCollectionModel(this.personsRepository.getAllPersons());
    }

    @GetMapping(path = "/getPersonsLike/{person}")
    public List<String> getAllKeywords(@PathVariable String person) throws IOException {
        return this.personsRepository.getAllPersonsLike(person);
    }
}
