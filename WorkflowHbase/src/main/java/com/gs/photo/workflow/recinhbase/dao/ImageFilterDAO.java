package com.gs.photo.workflow.recinhbase.dao;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.workflow.model.HbaseData;

@Component
public class ImageFilterDAO extends AbstractDAO<HbaseData> {

    public ImageFilterDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
