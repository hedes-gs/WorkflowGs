package com.gs.photo.common.workflow.exif;

public interface Tag {

    public FieldType getFieldType();

    public String getName();

    public short getValue();

}