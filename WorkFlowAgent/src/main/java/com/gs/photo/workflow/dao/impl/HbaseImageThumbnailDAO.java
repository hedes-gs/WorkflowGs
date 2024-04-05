package com.gs.photo.workflow.dao.impl;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;
import com.gs.photo.workflow.dao.IHbaseImageThumbnailDAO;

@Component
public class HbaseImageThumbnailDAO extends AbstractHbaseImageThumbnailDAO implements IHbaseImageThumbnailDAO {

    public HbaseImageThumbnailDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
