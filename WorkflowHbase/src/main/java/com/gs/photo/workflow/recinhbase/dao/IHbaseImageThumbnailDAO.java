package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;

public interface IHbaseImageThumbnailDAO extends IImageThumbnailDAO { public void truncate() throws IOException; }
