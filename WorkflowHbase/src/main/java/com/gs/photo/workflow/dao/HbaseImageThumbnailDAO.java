package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.stereotype.Component;

import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImageThumbnailDAO extends GenericDAO<HbaseImageThumbnail> {

    private static final byte[] FAMILY_THB_BYTES = "thb".getBytes();
    private static final byte[] FAMILY_SZ_BYTES  = "sz".getBytes();
    private static final byte[] FAMILY_IMG_BYTES = "img".getBytes();

    private static final byte[] HEIGHT_BYTES     = "height".getBytes();
    private static final byte[] WIDTH_BYTES      = "width".getBytes();
    private static final byte[] PATH_BYTES       = "path".getBytes();
    private static final byte[] TUMB_NAME_BYTES  = "thumb_name".getBytes();
    private static final byte[] IMAGE_NAME_BYTES = "image_name".getBytes();
    private static final byte[] TUMBNAIL_BYTES   = "thumbnail".getBytes();

    public void put(Collection<HbaseImageThumbnail> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public void put(HbaseImageThumbnail hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public void delete(HbaseImageThumbnail[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public void delete(HbaseImageThumbnail hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public int count() throws Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public HbaseImageThumbnail get(HbaseImageThumbnail hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation(HbaseImageThumbnail.class));
    }

    public List<HbaseImageThumbnail> getThumbNailsByDate(
        LocalDateTime firstDate,
        LocalDateTime lastDate,
        long minWidth,
        long minHeight
    ) {
        List<HbaseImageThumbnail> retValue = new ArrayList<>();
        final long firstDateEpochMillis = firstDate.toInstant(ZoneOffset.ofTotalSeconds(0))
            .toEpochMilli();
        final long mastDateEpochMilli = lastDate.toInstant(ZoneOffset.ofTotalSeconds(0))
            .toEpochMilli();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation(HbaseImageThumbnail.class)
                    .getTable())) {
            Scan scan = new Scan();
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_IMG_BYTES, HbaseImageThumbnailDAO.IMAGE_NAME_BYTES);
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_IMG_BYTES, HbaseImageThumbnailDAO.TUMB_NAME_BYTES);
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_IMG_BYTES, HbaseImageThumbnailDAO.PATH_BYTES);
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_SZ_BYTES, HbaseImageThumbnailDAO.WIDTH_BYTES);
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_SZ_BYTES, HbaseImageThumbnailDAO.HEIGHT_BYTES);
            scan.addColumn(HbaseImageThumbnailDAO.FAMILY_THB_BYTES, HbaseImageThumbnailDAO.TUMBNAIL_BYTES);

            scan.setFilter(new FilterRowByLongAtAGivenOffset(0, firstDateEpochMillis, mastDateEpochMilli));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                try {
                    this.getHbaseDataInformation(HbaseImageThumbnail.class)
                        .build(instance, t);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;

    }

}
