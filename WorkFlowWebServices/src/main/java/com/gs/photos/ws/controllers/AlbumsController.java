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

import com.gs.photos.ws.repositories.IAlbumsRepository;
import com.gs.photos.ws.web.assembler.AlbumsAssembler;

@RestController
@RequestMapping("/api/gs/albums")
public class AlbumsController {

    protected static Logger     LOGGER = LoggerFactory.getLogger(AlbumsController.class);

    @Autowired
    protected AlbumsAssembler   personsAssembler;

    @Autowired
    protected IAlbumsRepository albumsRepository;

    @GetMapping(path = "/all")
    public CollectionModel<EntityModel<String>> getAllPersons() throws IOException {
        return this.personsAssembler.toCollectionModel(this.albumsRepository.getAllAlbums());
    }

    @GetMapping(path = "/getAlbumsLike/{album}")
    public List<String> getAllKeywords(@PathVariable String album) throws IOException {
        return this.albumsRepository.getAllAlbumsLike(album);
    }
}
