package com.gsphotos.worflow.hbasefilters;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.gs.workflow.protobuf.generated.GsFilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowFindNextRowWithTwoFields extends FilterBase {

    private boolean filterOutRow = false;
    private int     offsetOfByteArray;
    private byte[]  byteArraytoCompare;
    private int     offsetOfShortValue;
    private short   shortValue;

    private boolean nextRowIsFound;
    private boolean stopFiltering;

    public FilterRowFindNextRowWithTwoFields(
        int offsetOfByteArray,
        byte[] byteArraytoCompare,
        int offsetOfShortValue,
        short shortValue
    ) {
        super();
        this.offsetOfByteArray = offsetOfByteArray;
        this.byteArraytoCompare = byteArraytoCompare;
        this.nextRowIsFound = false;
        this.filterOutRow = false;
        this.offsetOfShortValue = offsetOfShortValue;
        this.shortValue = shortValue;
    }

    public FilterRowFindNextRowWithTwoFields() { super(); }

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
        System.out.println(
            "->   Filtering byte array, offset is " + this.offsetOfByteArray + " : "
                + new String(this.byteArraytoCompare, Charset.forName("UTF-8")) + " length : " + length);
        if (this.nextRowIsFound) {
            this.filterOutRow = true;
            this.stopFiltering = true;
            return true;
        }
        boolean byteArrayIsEqual = true;
        if (!this.nextRowIsFound) {
            byteArrayIsEqual = this.compareByteArrays(data, offset);
            if (!byteArrayIsEqual) {
                this.nextRowIsFound = Bytes.toShort(data, offset + this.offsetOfShortValue) == this.shortValue;
            }
        }

        System.out.println(
            "     Filtering byte array, offset is " + this.offsetOfByteArray + " : "
                + new String(this.byteArraytoCompare, Charset.forName("UTF-8")) + " : filterOutRow = "
                + this.filterOutRow + ", nextRowIsFound = " + this.nextRowIsFound);
        return this.filterOutRow;
    }

    protected boolean compareByteArrays(byte[] data, int offset) {
        boolean byteArrayIsEqual = true;
        System.out.println(
            "     Filtering byte array, Comparing " + new String(this.byteArraytoCompare, Charset.forName("UTF-8"))
                + " to " + new String(data, offset + this.offsetOfByteArray, this.byteArraytoCompare.length));
        for (int k = 0; (k < this.byteArraytoCompare.length) && byteArrayIsEqual; k++) {
            byteArrayIsEqual = this.byteArraytoCompare[k] == data[offset + this.offsetOfByteArray + k];
        }
        return byteArrayIsEqual;
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        System.out.println("   filterAllRemaining  stopFiltering = " + this.stopFiltering);

        return this.stopFiltering;
    }

    GsFilterProtos.FilterRowFindNextRowWithTwoFields convert() {
        GsFilterProtos.FilterRowFindNextRowWithTwoFields.Builder builder = GsFilterProtos.FilterRowFindNextRowWithTwoFields
            .newBuilder();
        builder.setLongOffset(this.offsetOfByteArray);
        builder.setByteArraytoCompare(ByteString.copyFrom(this.byteArraytoCompare));
        builder.setOffsetOfShortValue(this.offsetOfShortValue);
        builder.setShortValue(this.shortValue);
        return builder.build();
    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        int longOffset = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
        byte[] byteArrayToCompare = filterArguments.get(1);
        int offsetOfShortValue = ParseFilter.convertByteArrayToInt(filterArguments.get(2));
        short shortValue = (short) ParseFilter.convertByteArrayToInt(filterArguments.get(3));
        return new FilterRowFindNextRowWithTwoFields(longOffset, byteArrayToCompare, offsetOfShortValue, shortValue);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterRowFindNextRowWithTwoFields.Builder builder = GsFilterProtos.FilterRowFindNextRowWithTwoFields
            .newBuilder();
        builder.setLongOffset(this.offsetOfByteArray);
        builder.setByteArraytoCompare(ByteString.copyFrom(this.byteArraytoCompare));
        builder.setOffsetOfShortValue(this.offsetOfShortValue);
        builder.setShortValue(this.shortValue);
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
    public static FilterRowFindNextRowWithTwoFields parseFrom(final byte[] pbBytes) throws DeserializationException {
        GsFilterProtos.FilterRowFindNextRowWithTwoFields proto;
        try {
            proto = GsFilterProtos.FilterRowFindNextRowWithTwoFields.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new FilterRowFindNextRowWithTwoFields(proto.getLongOffset(),
            proto.getByteArraytoCompare()
                .toByteArray(),
            proto.getOffsetOfShortValue(),
            (short) proto.getShortValue());
    }

}
