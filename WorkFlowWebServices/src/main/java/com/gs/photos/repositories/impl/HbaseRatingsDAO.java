package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.gs.photo.workflow.hbase.dao.AbstractMetaDataDAO;
import com.workflow.model.HbaseRatings;

@Component
public class HbaseRatingsDAO extends AbstractMetaDataDAO<HbaseRatings, Integer> implements IHbaseRatingsDAO {

    @Autowired
    protected SimpMessagingTemplate template;

    @Override
    protected byte[] createKey(Integer rating) throws IOException {
        HbaseRatings HbaseKeywords = com.workflow.model.HbaseRatings.builder()
            .withRatings(rating)
            .build();

        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(HbaseKeywords, keyValue);
        return keyValue;
    }

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public void incrementNbOfImages(HbaseRatings metaData) throws IOException {
        super.incrementNbOfImages(metaData.getRatings());
        Map<String, Integer> retValue = this.countAllPerRatings();
        this.template.convertAndSend("/topic/ratingsStatus", new Gson().toJson(retValue));
    }

    @Override
    public void decrementNbOfImages(HbaseRatings metaData) throws IOException {
        super.decrementNbOfImages(metaData.getRatings());
        Map<String, Integer> retValue = this.countAllPerRatings();
        ObjectMapper objectMapper = new ObjectMapper();

        this.template.convertAndSend("/topic/ratingsStatus", objectMapper.writeValueAsString(retValue));
    }

    @Override
    public void incrementNbOfImages(Integer key) throws IOException { // TODO Auto-generated method stub
        super.incrementNbOfImages(key);
        Map<String, Integer> retValue = this.countAllPerRatings();
        ObjectMapper objectMapper = new ObjectMapper();

        this.template.convertAndSend("/topic/ratingsStatus", objectMapper.writeValueAsString(retValue));
        AbstractMetaDataDAO.LOGGER
            .info("Increment ratings {} , new value is {} ", key, objectMapper.writeValueAsString(retValue));
    }

    @Override
    public void decrementNbOfImages(Integer key) throws IOException { // TODO Auto-generated method stub
        super.decrementNbOfImages(key);
        Map<String, Integer> retValue = this.countAllPerRatings();
        ObjectMapper objectMapper = new ObjectMapper();

        this.template.convertAndSend("/topic/ratingsStatus", objectMapper.writeValueAsString(retValue));
        AbstractMetaDataDAO.LOGGER
            .info("Decrement ratings {} , new value is {} ", key, objectMapper.writeValueAsString(retValue));
    }

    @Override
    public long countAll(HbaseRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatings());
    }

    protected Map<String, Integer> countAllPerRatings() throws IOException {
        try {
            Map<String, Integer> retValue = new HashMap<>();
            for (int k = 1; k <= 5; k++) {
                retValue.put(Integer.toString(k), (int) this.countAll(k));
            }
            return retValue;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return new HashMap<>();
    }

}