package com.gs.photo.workflow.dao;

import java.io.IOException;

public interface IHbaseImageThumbnailDAO extends IImageThumbnailDAO { public void truncate() throws IOException; }
