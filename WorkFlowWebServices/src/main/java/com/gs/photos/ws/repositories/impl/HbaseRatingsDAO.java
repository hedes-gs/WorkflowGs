package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractMetaDataDAO;
import com.workflow.model.HbaseRatings;

@Component
public class HbaseRatingsDAO extends AbstractMetaDataDAO<HbaseRatings, Long> implements IHbaseRatingsDAO {

    // @Autowired
    protected SimpMessagingTemplate template;

    @Override
    protected byte[] createKey(Long rating) throws IOException {
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
    public long countAll(HbaseRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatings());
    }

    protected Map<String, Long> countAllPerRatings() throws IOException {
        try {
            Map<String, Long> retValue = new HashMap<>();
            for (long k = 1; k <= 5; k++) {
                retValue.put(Long.toString(k), this.countAll(k));
            }
            return retValue;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return new HashMap<>();
    }

    @Override
    public void delete(HbaseRatings hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}