package com.gs.photos.ws.controllers;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.gs.photos.ws.repositories.IRatingRepository;

@RestController
@RequestMapping("/api/gs")
public class RatingControllers {

    @Autowired
    protected IRatingRepository ratingRepository;

    @GetMapping("/ratings/count/{rating}")
    public @ResponseBody ResponseEntity<Long> count(@PathVariable int rating) throws IOException, Throwable {
        return ResponseEntity.ok(this.ratingRepository.count(rating));
    }

    @GetMapping("/ratings/count/all")
    public @ResponseBody ResponseEntity<Map<String, Long>> countAll() throws IOException, Throwable {
        return ResponseEntity.ok(this.ratingRepository.countAll());
    }

}
