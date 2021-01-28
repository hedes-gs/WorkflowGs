package com.gsphotos.worflow.hbasefilters;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.gs.workflow.protobuf.generated.GsFilterProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowFindNextRow extends FilterBase {

    // !!!!!!!!!!!!!!!!!!! NOT TESTED !////////////
    private boolean filterOutRow = false;
    private byte[]  keyByteArray;
    private int     lengthOfMetaDataKey;
    private int     offsetOfImageId;
    private int     lengthOfImageId;

    private boolean nextRowIsFound;
    private boolean stopFiltering;

    public FilterRowFindNextRow(
        byte[] keyByteArray,
        int lengthOfMetaDataKey,
        int offsetOfImageId,
        int lengthOfImageId
    ) {
        super();
        this.keyByteArray = keyByteArray;
        this.lengthOfMetaDataKey = lengthOfMetaDataKey;
        this.offsetOfImageId = offsetOfImageId;
        this.lengthOfImageId = lengthOfImageId;
        this.nextRowIsFound = false;
        this.filterOutRow = false;
    }

    public FilterRowFindNextRow() { super(); }

    @Override
    public void reset() { this.filterOutRow = false; }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        System.out.println("   filterKeyValue  nextRowIsFound = " + this.nextRowIsFound);
        if (this.nextRowIsFound) { return ReturnCode.INCLUDE; }
        return ReturnCode.NEXT_ROW;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {

        if (this.nextRowIsFound) {
            this.filterOutRow = true;
            this.stopFiltering = true;
            return true;
        }
        boolean byteArrayIsEqual = true;
        if (!this.nextRowIsFound) {
            byteArrayIsEqual = FilterRowFindNextRow
                .compareByteArrays(data, offset, this.keyByteArray, 0, this.lengthOfMetaDataKey);
            if (!byteArrayIsEqual) {
                this.nextRowIsFound = !FilterRowFindNextRow
                    .compareByteArrays(data, offset, this.keyByteArray, this.offsetOfImageId, this.lengthOfImageId);
            }
        }
        return this.filterOutRow;
    }

    protected static boolean compareByteArrays(
        byte[] data,
        int offset,
        byte[] metaDataKey,
        int offsetInMetDataKey,
        int lengthInMetDataKey
    ) {
        boolean byteArrayIsEqual = true;
        for (int k = 0; (k < lengthInMetDataKey) && byteArrayIsEqual; k++) {
            byteArrayIsEqual = metaDataKey[k + offsetInMetDataKey] == data[offset + offsetInMetDataKey + k];
        }
        return byteArrayIsEqual;
    }

    @Override
    public boolean filterAllRemaining() throws IOException { return this.stopFiltering; }

    GsFilterProtos.FilterRowFindNextRowWithinAMetaData convert() {
        GsFilterProtos.FilterRowFindNextRowWithinAMetaData.Builder builder = GsFilterProtos.FilterRowFindNextRowWithinAMetaData
            .newBuilder();
        builder.setKeyByteArray(ByteString.copyFrom(this.keyByteArray))
            .setLengthOfMetaDataKey(this.lengthOfMetaDataKey)
            .setLengthOfImageId(this.lengthOfImageId)
            .setOffsetOfImageId(this.offsetOfImageId);
        return builder.build();

    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        byte[] keyByteArray = filterArguments.get(0);
        int lengthOfMetaDataKey = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
        int offsetOfImageId = ParseFilter.convertByteArrayToInt(filterArguments.get(2));
        int lengthOfImageId = ParseFilter.convertByteArrayToInt(filterArguments.get(3));
        return new FilterRowFindNextRow(keyByteArray, lengthOfMetaDataKey, offsetOfImageId, lengthOfImageId);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterRowFindNextRowWithinAMetaData.Builder builder = GsFilterProtos.FilterRowFindNextRowWithinAMetaData
            .newBuilder();
        builder.setKeyByteArray(ByteString.copyFrom(this.keyByteArray))
            .setLengthOfMetaDataKey(this.lengthOfMetaDataKey)
            .setLengthOfImageId(this.lengthOfImageId)
            .setOffsetOfImageId(this.offsetOfImageId);
        return builder.build()
            .toByteArray();
    }

    /**
     * @param pbBytes
     *            A pb serialized {@link RowFilter} instance
     * @return An instance of {@link RowFilter} made from <code>bytes</code>
     * @throws DeserializationException
     * @see #toByteArray
     */
    public static FilterRowFindNextRow parseFrom(final byte[] pbBytes) throws DeserializationException {
        GsFilterProtos.FilterRowFindNextRowWithinAMetaData proto;
        try {
            proto = GsFilterProtos.FilterRowFindNextRowWithinAMetaData.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new FilterRowFindNextRow(proto.getKeyByteArray()
            .toByteArray(), proto.getLengthOfMetaDataKey(), proto.getOffsetOfImageId(), proto.getLengthOfImageId());
    }

}
