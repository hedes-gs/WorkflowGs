package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfRatingsDAO;

public interface IHbaseImagesOfRatingsDAO extends IImagesOfRatingsDAO { void truncate() throws IOException; }
