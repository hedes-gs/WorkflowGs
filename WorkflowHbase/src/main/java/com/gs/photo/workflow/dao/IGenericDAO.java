package com.gs.photo.workflow.dao;

import com.workflow.model.HbaseData;

public interface IGenericDAO {

	public <T extends HbaseData> void put(T hbaseData, Class<T> cl);

	public <T extends HbaseData> void put(T[] hbaseData, Class<T> cl);

}
