package com.gs.workflow.coprocessor;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlbumMetadataStringCoprocessor extends AbstractMetadataStringCoprocessor {
    protected static Logger     LOGGER                    = LoggerFactory
        .getLogger(AlbumMetadataStringCoprocessor.class);

    private static final byte[] SOURCE_FAMILY_AS_BYTES    = "albums".getBytes(Charset.forName("UTF-8"));
    public static final int     FIXED_WIDTH_KEYWORD       = 64;
    public static final int     FIXED_WIDTH_IMAGE_ID      = 64;
    public static final int     FIXED_WIDTH_CREATION_DATE = 8;

    @Override
    protected String getTablePageForMetadata() { return "page_images_album"; }

    @Override
    protected String getTableSource() { return "image_thumbnail"; }

    @Override
    protected String getTableImagesOfMetaData() { return "images_album"; }

    @Override
    protected String getTableMetaData() { return "album"; }

    @Override
    protected byte[] getTableSourceFamily() { return AlbumMetadataStringCoprocessor.SOURCE_FAMILY_AS_BYTES; }

    @Override
    protected byte[] getRowKeyForMetaDataTable(String metadata) {
        return Arrays
            .copyOf(metadata.getBytes(Charset.forName("UTF-8")), AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected byte[] toRowKey(String t, long pageNumber) {
        try {
            byte[] retValue = Arrays.copyOf(
                t.getBytes(Charset.forName("UTF-8")),
                AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD
                    + AlbumMetadataStringCoprocessor.FIXED_WIDTH_CREATION_DATE);
            Arrays.fill(retValue, t.getBytes().length, AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD, (byte) 0x20);
            Bytes.putLong(retValue, AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD, pageNumber);
            return retValue;
        } catch (IllegalArgumentException e) {
            AbstractPageProcessor.LOGGER.error(
                "[COPROC][{}] error in toRowKey, metadata is '{}', pageNumber is {}",
                this.getCoprocName(),
                t,
                pageNumber);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected long extractPageNumber(byte[] rowKey) {

        return Bytes.toLong(rowKey, AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected long extractDateOfRowKeyToIndex(byte[] rowKey) {
        return Bytes.toLong(rowKey, AbstractPageProcessor.FIXED_WIDTH_REGION_SALT);
    }

    @Override
    protected String extractMetadataValue(byte[] rowKey) {
        return new String(rowKey, 0, AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected byte[] buildTableMetaDataRowKey(String metaData, byte[] row) {

        byte[] retValue = Arrays.copyOf(
            metaData.getBytes(Charset.forName("UTF-8")),
            AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD + row.length);
        System.arraycopy(row, 0, retValue, AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD, row.length);
        return retValue;
    }

    @Override
    protected boolean isAllowedForTableSource(Put put) {
        List<Cell> albumCells = put.getFamilyCellMap()
            .get(AlbumMetadataStringCoprocessor.SOURCE_FAMILY_AS_BYTES);
        if ((albumCells != null) && (albumCells.size() > 0)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(AbstractPageProcessor.TABLE_SOURCE_THUMBNAIL);

            if ((cells != null) && (cells.size() > 0)) {
                return cells.stream()
                    .peek(
                        (c) -> AlbumMetadataStringCoprocessor.LOGGER
                            .info("[COPROC][{}]Exam {} ", this.getCoprocName(), new String(CellUtil.cloneQualifier(c))))
                    .filter((c) -> {
                        Integer key = Integer.parseInt(new String(CellUtil.cloneQualifier(c)));
                        return (key == 1);
                    })
                    .findFirst()
                    .isPresent();
            } else {
                AlbumMetadataStringCoprocessor.LOGGER
                    .warn("Unable to find some thumbnail for {} ", AbstractPageProcessor.toHexString(put.getRow()));
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean shouldFamilyInsourceBeRecordedInMetadata(byte[] familySource) {
        for (byte[] b : AbstractPageProcessor.SOURCE_FAMILIES_TO_EXCLUDE) {
            if (Objects.deepEquals(b, familySource)) { return false; }
        }
        return true;
    }

    @Override
    protected String getCoprocName() { return "COPROC-ALBUM"; }

}
