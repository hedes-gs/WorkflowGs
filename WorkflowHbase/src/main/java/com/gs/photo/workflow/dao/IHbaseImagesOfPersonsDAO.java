package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IImagesOfPersonsDAO;

public interface IHbaseImagesOfPersonsDAO extends IImagesOfPersonsDAO { void truncate() throws IOException; }
