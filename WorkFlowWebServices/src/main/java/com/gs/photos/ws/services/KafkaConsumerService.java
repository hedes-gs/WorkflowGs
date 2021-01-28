package com.gs.photos.ws.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photos.ws.repositories.IHbaseImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.dtos.ImageDto;

@Service
public class KafkaConsumerService {

    protected static Logger               LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    protected IHbaseImageThumbnailDAO     hbaseImageThumbnailDAO;

    @Autowired
    @Qualifier("kafkaByteArrayTemplate")
    private KafkaTemplate<String, byte[]> kafkaByteArrayTemplate;

    public void checkout(ImageDto imgDto) {
        HbaseImageThumbnail hbi = HbaseImageThumbnail.builder()
            .withImageId(imgDto.getImageId())
            .withCreationDate(
                DateTimeHelper.toEpochMillis(
                    imgDto.getData()
                        .getCreationDate()))
            .withRegionSalt(
                imgDto.getData()
                    .getSalt())
            .build();
        KafkaConsumerService.LOGGER.info("Receive {} to checkout ", imgDto);

        this.kafkaByteArrayTemplate.send("topic-checkout", this.hbaseImageThumbnailDAO.getKey(hbi));
    }

}