package com.gs.workflow.coprocessor;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PersonMetadataStringCoprocessor extends AbstractMetadataStringCoprocessor {

    private static final byte[] SOURCE_FAMILY             = "persons".getBytes(Charset.forName("UTF-8"));
    public static final int     FIXED_WIDTH_KEYWORD       = 64;
    public static final int     FIXED_WIDTH_IMAGE_ID      = 64;
    public static final int     FIXED_WIDTH_IMPORT_ID     = 64;
    public static final int     FIXED_WIDTH_PATH          = 128;
    public static final int     FIXED_WIDTH_CREATION_DATE = 8;

    @Override
    protected String getTablePageForMetadata() { return "page_images_persons"; }

    @Override
    protected String getTableSource() { return "image_thumbnail"; }

    @Override
    protected String getTableImagesOfMetaData() { return "images_persons"; }

    @Override
    protected String getTableMetaData() { return "persons"; }

    @Override
    protected byte[] getTableSourceFamily() { return PersonMetadataStringCoprocessor.SOURCE_FAMILY; }

    @Override
    protected byte[] toRowKey(String t, long pageNumber) {
        byte[] retValue = Arrays.copyOf(
            t.getBytes(Charset.forName("UTF-8")),
            PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD
                + PersonMetadataStringCoprocessor.FIXED_WIDTH_CREATION_DATE);
        Bytes.putLong(retValue, PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD, pageNumber);
        return retValue;
    }

    @Override
    protected byte[] getRowKeyForMetaDataTable(String metadata) { // TODO Auto-generated method stub
        return Arrays
            .copyOf(metadata.getBytes(Charset.forName("UTF-8")), AlbumMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected long extractPageNumber(byte[] rowKey) {
        return Bytes.toLong(rowKey, PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected long extractDateOfRowKeyToIndex(byte[] rowKey) { return Bytes.toLong(rowKey); }

    @Override
    protected String extractMetadataValue(byte[] rowKey) {
        return new String(rowKey, 0, PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD);
    }

    @Override
    protected byte[] buildTableMetaDataRowKey(String metaData, byte[] row) {
        byte[] retValue = Arrays.copyOf(
            metaData.getBytes(Charset.forName("UTF-8")),
            PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD + row.length);
        System.arraycopy(row, 0, retValue, PersonMetadataStringCoprocessor.FIXED_WIDTH_KEYWORD, row.length);
        return retValue;
    }

    @Override
    protected boolean isAllowedForTableSource(Put put) {
        List<Cell> albumCells = put.getFamilyCellMap()
            .get(PersonMetadataStringCoprocessor.SOURCE_FAMILY);
        if ((albumCells != null) && (albumCells.size() > 0)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(PaginationCoprocessor.TABLE_SOURCE_THUMBNAIL);

            if ((cells != null) && (cells.size() > 0)) {
                return cells.stream()
                    .peek(
                        (c) -> AlbumMetadataStringCoprocessor.LOGGER
                            .info("Exam {} ", new String(CellUtil.cloneQualifier(c))))
                    .filter((c) -> {
                        Integer key = Integer.parseInt(new String(CellUtil.cloneQualifier(c)));
                        return (key == 1);
                    })
                    .findFirst()
                    .isPresent();
            } else {
                PaginationCoprocessor.LOGGER
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
    protected String getCoprocName() { return "COPROC-PERSON"; }

}
