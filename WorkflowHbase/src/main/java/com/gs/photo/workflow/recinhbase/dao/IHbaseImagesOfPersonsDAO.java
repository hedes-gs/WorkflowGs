package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfPersonsDAO;

public interface IHbaseImagesOfPersonsDAO extends IImagesOfPersonsDAO { void truncate() throws IOException; }
