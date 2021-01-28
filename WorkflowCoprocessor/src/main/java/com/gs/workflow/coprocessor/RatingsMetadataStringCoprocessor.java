package com.gs.workflow.coprocessor;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RatingsMetadataStringCoprocessor extends AbstractMetadataLongCoprocessor {

    private static final byte[] SOURCE_FAMILY             = "ratings".getBytes(Charset.forName("UTF-8"));
    public static final int     FIXED_WIDTH_RATINGS       = 8;
    public static final int     FIXED_WIDTH_IMAGE_ID      = 64;
    public static final int     FIXED_WIDTH_CREATION_DATE = 8;

    @Override
    protected String getTablePageForMetadata() { return "page_images_ratings"; }

    @Override
    protected byte[] getTableSourceFamily() { return RatingsMetadataStringCoprocessor.SOURCE_FAMILY; }

    @Override
    protected String getTableSource() { return "image_thumbnail"; }

    @Override
    protected String getTableImagesOfMetaData() { return "images_ratings"; }

    @Override
    protected String getTableMetaData() { return "ratings"; }

    @Override
    protected byte[] getRowKeyForMetaDataTable(Long metadata) { // TODO Auto-generated method stub
        return AbstractPageProcessor.convert(metadata);
    }

    @Override
    protected byte[] toRowKey(Long metaData, long pageNumber) {
        byte[] retValue = Arrays.copyOf(
            AbstractPageProcessor.convert(metaData),
            RatingsMetadataStringCoprocessor.FIXED_WIDTH_RATINGS
                + RatingsMetadataStringCoprocessor.FIXED_WIDTH_CREATION_DATE);
        Bytes.putLong(retValue, RatingsMetadataStringCoprocessor.FIXED_WIDTH_RATINGS, pageNumber);
        return retValue;
    }

    @Override
    protected long extractPageNumber(byte[] rowKey) {
        return Bytes.toLong(rowKey, RatingsMetadataStringCoprocessor.FIXED_WIDTH_RATINGS);
    }

    @Override
    protected long extractDateOfRowKeyToIndex(byte[] rowKey) {
        return Bytes.toLong(rowKey, AbstractPageProcessor.FIXED_WIDTH_REGION_SALT);
    }

    @Override
    protected Long extractMetadataValue(byte[] rowKey) { return Bytes.toLong(rowKey, 0); }

    @Override
    protected boolean isAllowedForTableSource(Put put) {
        List<Cell> albumCells = put.getFamilyCellMap()
            .get(RatingsMetadataStringCoprocessor.SOURCE_FAMILY);
        if ((albumCells != null) && (albumCells.size() > 0)) {
            List<Cell> cells = put.getFamilyCellMap()
                .get(AbstractPageProcessor.TABLE_SOURCE_THUMBNAIL);

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
    protected byte[] buildTableMetaDataRowKey(Long metaData, byte[] row) {
        byte[] metaDataAsBytes = AbstractPageProcessor.convert(metaData);
        byte[] retValue = Arrays
            .copyOf(metaDataAsBytes, RatingsMetadataStringCoprocessor.FIXED_WIDTH_RATINGS + row.length);
        System.arraycopy(row, 0, retValue, RatingsMetadataStringCoprocessor.FIXED_WIDTH_RATINGS, row.length);
        return retValue;
    }

    @Override
    protected boolean shouldFamilyInsourceBeRecordedInMetadata(byte[] familySource) {
        for (byte[] b : AbstractPageProcessor.SOURCE_FAMILIES_TO_EXCLUDE) {
            if (Objects.deepEquals(b, familySource)) { return false; }
        }
        return true;
    }

    @Override
    protected String getCoprocName() { return "COPROC-RATINGS"; }

}
