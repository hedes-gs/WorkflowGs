package com.gs.photos.controllers;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gs.photos.repositories.IKeywordsRepository;

@RestController
@RequestMapping("/api/gs/keywords")
@CrossOrigin(origins = "*")
public class KeywordsController {

    protected static Logger       LOGGER = LoggerFactory.getLogger(KeywordsController.class);

    @Autowired
    protected IKeywordsRepository keywordsRepository;

    @GetMapping(path = "/all")
    public List<String> getAllKeywords() throws IOException { return this.keywordsRepository.getAllKeywords(); }

    @GetMapping(path = "/getKeywordsLike/{keyword}")
    public List<String> getAllKeywords(@PathVariable String keyword) throws IOException {
        return this.keywordsRepository.getAllKeywordsLike(keyword);
    }
}
