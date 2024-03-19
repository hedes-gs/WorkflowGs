package com.gsphotos.worflow.hbasefilters;

import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.gs.workflow.protobuf.generated.GsFilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowByLongAtAGivenOffset extends FilterBase {

    public static enum TypeValue {

        BYTE(
            0
        ), USHORT(
            1
        ), INT(
            2
        ), LONG(
            3
        );

        private int value;

        public int getValue() { return this.value; }

        public static TypeValue toTypeValue(int v) {
            switch (v) {
                case 0:
                    return BYTE;
                case 1:
                    return USHORT;
                case 2:
                    return INT;
                case 3:
                    return LONG;
            }
            throw new IllegalArgumentException(" " + v);
        }

        TypeValue(int v) { this.value = v; }

    }

    private boolean   filterOutRow = false;
    private long      longOffset;
    private long      firstLong;
    private long      lastLong;
    private TypeValue typeValue;

    public FilterRowByLongAtAGivenOffset(
        long longOffset,
        long firstLong,
        long lastLong
    ) {
        super();
        this.longOffset = longOffset;
        this.firstLong = firstLong;
        this.lastLong = lastLong;
        this.typeValue = TypeValue.LONG;
    }

    public FilterRowByLongAtAGivenOffset(
        long longOffset,
        long firstLong,
        long lastLong,
        TypeValue typeValue
    ) {
        super();
        this.longOffset = longOffset;
        this.firstLong = firstLong;
        this.lastLong = lastLong;
        this.typeValue = typeValue;
    }

    public FilterRowByLongAtAGivenOffset() { super(); }

    @Override
    public void reset() { this.filterOutRow = false; }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (this.filterOutRow) { return ReturnCode.NEXT_ROW; }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        switch (this.typeValue) {
            case BYTE: {
                byte value = data[offset + (int) this.longOffset];
                this.filterOutRow = !((value >= (byte) this.firstLong) && (value <= (byte) this.lastLong));
                break;
            }
            case USHORT: {
                short value = Bytes.toShort(data, offset + (int) this.longOffset);
                this.filterOutRow = !((value >= (short) this.firstLong) && (value <= (short) this.lastLong));
                break;
            }
            case INT: {
                int value = Bytes.toInt(data, offset + (int) this.longOffset);
                this.filterOutRow = !((value >= (int) this.firstLong) && (value <= (int) this.lastLong));
                break;
            }
            case LONG: {
                long value = Bytes.toLong(data, offset + (int) this.longOffset);
                this.filterOutRow = !((value >= this.firstLong) && (value <= this.lastLong));
                break;
            }
        }
        return this.filterOutRow;
    }

    GsFilterProtos.FilterRowByLongAtAGivenOffset convert() {
        GsFilterProtos.FilterRowByLongAtAGivenOffset.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffset
            .newBuilder();
        builder.setFirstLong(this.firstLong);
        builder.setLastLong(this.lastLong);
        builder.setLongOffset(this.longOffset);
        builder.setTypeValue(GsFilterProtos.FilterRowByLongAtAGivenOffset.TypeValue.valueOf(this.typeValue.getValue()));
        return builder.build();
    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        long longOffset = ParseFilter.convertByteArrayToLong(filterArguments.get(0));
        long firstLong = ParseFilter.convertByteArrayToLong(filterArguments.get(1));
        long lastLong = ParseFilter.convertByteArrayToLong(filterArguments.get(2));
        TypeValue typeValue = TypeValue.toTypeValue(ParseFilter.convertByteArrayToInt(filterArguments.get(3)));

        return new FilterRowByLongAtAGivenOffset(longOffset, firstLong, lastLong, typeValue);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterRowByLongAtAGivenOffset.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffset
            .newBuilder();
        builder.setFirstLong(this.firstLong);
        builder.setLastLong(this.lastLong);
        builder.setLongOffset(this.longOffset);
        builder.setTypeValue(GsFilterProtos.FilterRowByLongAtAGivenOffset.TypeValue.valueOf(this.typeValue.getValue()));
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
    public static FilterRowByLongAtAGivenOffset parseFrom(final byte[] pbBytes) throws DeserializationException {
        GsFilterProtos.FilterRowByLongAtAGivenOffset proto;
        try {
            proto = GsFilterProtos.FilterRowByLongAtAGivenOffset.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        return new FilterRowByLongAtAGivenOffset(proto.getLongOffset(),
            proto.getFirstLong(),
            proto.getLastLong(),
            TypeValue.toTypeValue(
                proto.getTypeValue()
                    .getNumber()));
    }

    @Override
    public String toString() {
        return "FilterRowByLongAtAGivenOffset [filterOutRow=" + this.filterOutRow + ", longOffset=" + this.longOffset
            + ", firstLong=" + this.firstLong + ", lastLong=" + this.lastLong + "]";
    }

}
