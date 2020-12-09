package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.nio.charset.Charset;

import com.gs.photo.workflow.hbase.dao.IGenericDAO;
import com.workflow.model.HbaseImageThumbnail;

public interface IImageThumbnailDAO extends IGenericDAO<HbaseImageThumbnail> {

    void flush() throws IOException;

    final String TABLE_PAGE                          = "page_image_thumbnail";
    final byte[] TABLE_PAGE_DESC_COLUMN_FAMILY       = "max_min".getBytes(Charset.forName("UTF-8"));
    final byte[] TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER = "max".getBytes(Charset.forName("UTF-8"));
    final byte[] TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER = "min".getBytes(Charset.forName("UTF-8"));
    final byte[] TABLE_PAGE_LIST_COLUMN_FAMILY       = "list".getBytes(Charset.forName("UTF-8"));
    final byte[] TABLE_PAGE_INFOS_COLUMN_FAMILY      = "infos".getBytes(Charset.forName("UTF-8"));
    final byte[] TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS = "nbOfElements".getBytes(Charset.forName("UTF-8"));
    final long   PAGE_SIZE                           = 1000L;
    final byte[] FAMILY_THB_BYTES                    = "thb".getBytes();
    final byte[] FAMILY_SZ_BYTES                     = "sz".getBytes();
    final byte[] FAMILY_IMG_BYTES                    = "img".getBytes();

    final byte[] HEIGHT_BYTES                        = "height".getBytes();
    final byte[] WIDTH_BYTES                         = "width".getBytes();
    final byte[] PATH_BYTES                          = "path".getBytes();
    final byte[] TUMB_NAME_BYTES                     = "thumb_name".getBytes();
    final byte[] IMAGE_NAME_BYTES                    = "image_name".getBytes();
    final byte[] TUMBNAIL_BYTES                      = "thumbnail".getBytes();

    final byte[] FAMILY_TECH_BYTES                   = "tech".getBytes();
    final byte[] FAMILY_META_BYTES                   = "meta".getBytes();
    final byte[] FAMILY_ALBUMS_BYTES                 = "albums".getBytes();
    final byte[] FAMILY_KEYWORDS_BYTES               = "keywords".getBytes();
    final byte[] FAMILY_PERSONS_BYTES                = "persons".getBytes();

}
