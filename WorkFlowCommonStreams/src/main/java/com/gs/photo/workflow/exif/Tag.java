package com.gs.photo.workflow.exif;

public interface Tag {

    public FieldType getFieldType();

    public String getName();

    public short getValue();

}