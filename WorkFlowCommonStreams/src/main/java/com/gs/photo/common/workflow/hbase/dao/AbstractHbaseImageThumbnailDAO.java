package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.math.BigInteger;
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

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.ModelConstants;

public abstract class AbstractHbaseImageThumbnailDAO extends GenericDAO<HbaseImageThumbnail>
    implements IImageThumbnailDAO {

    public static final int IMAGES_SALT_SIZE = 32;

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

    @Override
    public HbaseImageThumbnail get(HbaseImageThumbnail hbaseData) throws IOException {
        return super.get(hbaseData, this.getHbaseDataInformation());
    }

    protected byte[][] buildSplitKey(HbaseDataInformation<HbaseImageThumbnail> hdi) {
        byte[][] splitKeys = new byte[32][];
        BigInteger bi = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
        BigInteger intervall = bi.divide(BigInteger.valueOf(32));
        BigInteger biInit = BigInteger.valueOf(0L);
        for (short k = 0; k < AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE; k++) {
            splitKeys[k] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
                + ModelConstants.FIXED_WIDTH_IMAGE_ID];
            biInit = biInit.add(intervall);
            String rowImageId = biInit.toString(16);
            HbaseImageThumbnail currentRow = HbaseImageThumbnail.builder()
                .withCreationDate(
                    ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                        .toEpochSecond())
                .withRegionSalt(k)
                .withImageId(rowImageId)
                .build();
            hdi.buildKey(currentRow, splitKeys[k]);
        }
        return splitKeys;
    }

    protected byte[][] buildStartOfStopKeys(HbaseDataInformation<HbaseImageThumbnail> hdi, int key) {
        byte[][] splitKeys = new byte[2][];
        BigInteger bi = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
        BigInteger intervall = bi.divide(BigInteger.valueOf(32));
        BigInteger biInit = BigInteger.valueOf(0L);
        for (short k = 0; k <= key; k++) {
            biInit = biInit.add(intervall);
            String rowImageId = biInit.toString(16);
            if (k == (key - 1)) {
                HbaseImageThumbnail currentRow = HbaseImageThumbnail.builder()
                    .withCreationDate(
                        ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                            .toEpochSecond())
                    .withRegionSalt(k)
                    .withImageId(rowImageId)
                    .build();
                splitKeys[0] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
                    + ModelConstants.FIXED_WIDTH_IMAGE_ID];
                hdi.buildKey(currentRow, splitKeys[0]);
            } else if (k == key) {
                splitKeys[1] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
                    + ModelConstants.FIXED_WIDTH_IMAGE_ID];
                HbaseImageThumbnail currentRow = HbaseImageThumbnail.builder()
                    .withCreationDate(
                        ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Paris"))
                            .toEpochSecond())
                    .withRegionSalt(k)
                    .withImageId(rowImageId)
                    .build();
                hdi.buildKey(currentRow, splitKeys[1]);
            }
        }
        return splitKeys;
    }

    protected byte[][] buildStartOfStopKeys(
        HbaseDataInformation<HbaseImageThumbnail> hdi,
        int key,
        long startDate,
        long endDate
    ) {
        String lastImageId = "ffffffffffffffff";
        byte[][] splitKeys = new byte[2][];
        HbaseImageThumbnail currentRow = HbaseImageThumbnail.builder()
            .withCreationDate(startDate)
            .withRegionSalt((short) key)
            .withImageId("")
            .build();
        splitKeys[0] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
            + ModelConstants.FIXED_WIDTH_IMAGE_ID];
        hdi.buildKey(currentRow, splitKeys[0]);
        splitKeys[1] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
            + ModelConstants.FIXED_WIDTH_IMAGE_ID];
        currentRow = HbaseImageThumbnail.builder()
            .withCreationDate(endDate)
            .withRegionSalt((short) key)
            .withImageId(lastImageId)
            .build();
        hdi.buildKey(currentRow, splitKeys[1]);
        return splitKeys;
    }

    protected byte[][] buildStartOfStopKeys(
        HbaseDataInformation<HbaseImageThumbnail> hdi,
        long startDate,
        long endDate
    ) {
        String lastImageId = "ffffffffffffffff";
        byte[][] splitKeys = new byte[2][];
        HbaseImageThumbnail currentRow = HbaseImageThumbnail.builder()
            .withCreationDate(startDate)
            .withRegionSalt((short) 0)
            .withImageId("")
            .build();
        splitKeys[0] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
            + ModelConstants.FIXED_WIDTH_IMAGE_ID];
        hdi.buildKey(currentRow, splitKeys[0]);
        splitKeys[1] = new byte[ModelConstants.FIXED_WIDTH_SHORT + ModelConstants.FIXED_WIDTH_CREATION_DATE
            + ModelConstants.FIXED_WIDTH_IMAGE_ID];
        currentRow = HbaseImageThumbnail.builder()
            .withCreationDate(endDate)
            .withRegionSalt((short) AbstractHbaseImageThumbnailDAO.IMAGES_SALT_SIZE)
            .withImageId(lastImageId)
            .build();
        hdi.buildKey(currentRow, splitKeys[1]);
        return splitKeys;
    }

    @Override
    protected void createTablesIfNeeded(HbaseDataInformation<HbaseImageThumbnail> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
            byte[][] splitKeys = this.buildSplitKey(hdi);

            TableName tn = AbstractDAO.createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies(), splitKeys);
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
