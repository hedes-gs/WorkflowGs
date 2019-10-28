package com.gs.photo.workflow.impl;

import java.nio.ByteBuffer;

public interface IIgniteDAO {

	public void save(String key, ByteBuffer rawFile);

}
