package com.gs.photos.workflow.metadata;

import com.workflow.model.FieldType;

public interface Tag {
	public String getFieldAsString(Object value);

	public FieldType getFieldType();

	public String getName();

	public short getValue();

	public boolean isCritical();

	public boolean mayContainSomeSimpleFields();
}