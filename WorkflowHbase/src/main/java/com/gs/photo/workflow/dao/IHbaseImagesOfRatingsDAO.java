package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IImagesOfRatingsDAO;

public interface IHbaseImagesOfRatingsDAO extends IImagesOfRatingsDAO { void truncate() throws IOException; }
