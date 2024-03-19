package com.gs.photos.ws.repositories.impl;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfAlbumDAO;
import com.gs.photos.ws.repositories.IMetaDataService;
import com.workflow.model.HbaseAlbum;
import com.workflow.model.HbaseImagesOfAlbum;

public interface IHbaseImagesOfAlbumsDAO extends IImagesOfAlbumDAO, IMetaDataService<HbaseImagesOfAlbum, HbaseAlbum> {

}