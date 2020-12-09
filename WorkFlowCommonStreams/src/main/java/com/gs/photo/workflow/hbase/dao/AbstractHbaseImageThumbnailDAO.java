package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.gs.photo.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.ModelConstants;

public abstract class AbstractHbaseImageThumbnailDAO extends GenericDAO<HbaseImageThumbnail>
    implements IImageThumbnailDAO {

    public void put(Collection<HbaseImageThumbnail> hbaseData) throws IOException {
        super.put(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseImageThumbnail[] hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public void delete(HbaseImageThumbnail hbaseData) throws IOException {
        super.delete(hbaseData, this.getHbaseDataInformation());
    }

    public long count() throws Throwable { return super.countWithCoprocessorJob(this.getHbaseDataInformation()); }

    public HbaseImageThumbnail get(HbaseImageThumbnail hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

    @Override
    protected void createTablesIfNeeded(HbaseDataInformation<HbaseImageThumbnail> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());

            HbaseImageThumbnail first = HbaseImageThumbnail.builder()
                .withCreationDate(
                    ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                        .toEpochSecond())
                .withImageId("")
                .build();
            HbaseImageThumbnail last = HbaseImageThumbnail.builder()
                .withCreationDate(
                    ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                        .toEpochSecond())
                .withImageId("")
                .build();
            byte[] firstRow = new byte[ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID];
            hdi.buildKey(first, firstRow);
            byte[] lastRow = new byte[ModelConstants.FIXED_WIDTH_CREATION_DATE + ModelConstants.FIXED_WIDTH_IMAGE_ID];
            hdi.buildKey(last, lastRow);
            TableName tn = AbstractDAO
                .createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies(), firstRow, lastRow, 24);
            hdi.setTable(tn);
            if (hdi.getPageTableName() != null) {
                TableName pageTn = this.createPageTableIfNeeded(admin, hdi.getPageTableName());
                hdi.setPageTable(pageTn);
            }
        }

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
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = new Scan();
            scan.addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.IMAGE_NAME_BYTES);
            scan.addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.TUMB_NAME_BYTES);
            scan.addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.PATH_BYTES);
            scan.addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.WIDTH_BYTES);
            scan.addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.HEIGHT_BYTES);
            scan.addColumn(IImageThumbnailDAO.FAMILY_THB_BYTES, IImageThumbnailDAO.TUMBNAIL_BYTES);

            scan.setFilter(new FilterRowByLongAtAGivenOffset(0, firstDateEpochMillis, mastDateEpochMilli));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                this.getHbaseDataInformation()
                    .build(instance, t);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;

    }

    public List<HbaseImageThumbnail> getNextThumbNailOf(HbaseImageThumbnail initialKey) {
        List<HbaseImageThumbnail> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            byte[] key = GenericDAO.getKey(initialKey, this.getHbaseDataInformation());

            Scan scan = new Scan().addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.IMAGE_NAME_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.TUMB_NAME_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.PATH_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.WIDTH_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.HEIGHT_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_THB_BYTES, IImageThumbnailDAO.TUMBNAIL_BYTES)
                .setLimit(1)
                .withStartRow(key, false);
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                this.getHbaseDataInformation()
                    .build(instance, t);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;

    }

    public List<HbaseImageThumbnail> getPreviousThumbNailOf(HbaseImageThumbnail initialKey) {
        List<HbaseImageThumbnail> retValue = new ArrayList<>();
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {

            byte[] key = GenericDAO.getKey(initialKey, this.getHbaseDataInformation());
            Bytes.putLong(key, 0, initialKey.getCreationDate());
            Scan scan = new Scan().addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.IMAGE_NAME_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.TUMB_NAME_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_IMG_BYTES, IImageThumbnailDAO.PATH_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.WIDTH_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_SZ_BYTES, IImageThumbnailDAO.HEIGHT_BYTES)
                .addColumn(IImageThumbnailDAO.FAMILY_THB_BYTES, IImageThumbnailDAO.TUMBNAIL_BYTES)
                .withStartRow(key, false)
                .setLimit(1)
                .setReversed(true);
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {
                HbaseImageThumbnail instance = new HbaseImageThumbnail();
                retValue.add(instance);
                this.getHbaseDataInformation()
                    .build(instance, t);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

}
