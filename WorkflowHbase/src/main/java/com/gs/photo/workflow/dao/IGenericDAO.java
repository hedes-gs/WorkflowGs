package com.gs.photo.workflow.dao;

import java.util.Collection;

import com.workflow.model.HbaseData;

public interface IGenericDAO<T extends HbaseData> {

	public void put(T hbaseData, Class<T> cl);

	public void put(T[] hbaseData, Class<T> cl);

	public void put(Collection<T> hbaseData, Class<T> cl);

	T get(T hbaseData, Class<T> cl);

	void delete(T[] hbaseData, Class<T> cl);

	void delete(T hbaseData, Class<T> cl);

}
