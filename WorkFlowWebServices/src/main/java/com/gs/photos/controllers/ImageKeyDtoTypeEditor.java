package com.gs.photos.controllers;

import java.beans.PropertyEditorSupport;

import com.gs.photo.workflow.DateTimeHelper;
import com.workflow.model.dtos.ImageKeyDto;

public class ImageKeyDtoTypeEditor extends PropertyEditorSupport {
    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        // creationDate,2020-05-19T06:06:50.152Z,version,0,imageId,string
        String[] splits = text.split(",");
        ImageKeyDto.Builder imageDTObuilder = ImageKeyDto.builder();
        for (int k = 0; k < splits.length; k = k + 2) {
            switch (splits[k]) {
                case "creationDate":
                    imageDTObuilder.withCreationDate(
                        DateTimeHelper
                            .toOffsetDateTime(splits[k + 1], DateTimeHelper.SPRING_VALUE_DATE_TIME_FORMATTER));
                    break;
                case "version":
                    imageDTObuilder.withVersion(Integer.parseUnsignedInt(splits[k + 1]));
                    break;
                case "imageId":
                    imageDTObuilder.withImageId(splits[k + 1]);
                    break;
            }
        }
        ImageKeyDto imageDTO = imageDTObuilder.build();
        this.setValue(imageDTO);
    }
}
