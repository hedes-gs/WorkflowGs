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
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowByLongAtAGivenOffsetWithPage extends FilterBase {

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
    private int       currentRowNumberInPage;
    private int       currentPageNumber;
    private int       pageNumber;
    private int       pageSize;
    private boolean   pageIsFound;
    private boolean   stopFiltering;
    private TypeValue typeValue;

    public FilterRowByLongAtAGivenOffsetWithPage(
        long longOffset,
        long firstLong,
        long lastLong,
        int pageSize,
        int pageNumber
    ) {
        super();
        this.longOffset = longOffset;
        this.firstLong = firstLong;
        this.lastLong = lastLong;
        this.typeValue = TypeValue.LONG;
        this.pageSize = pageSize;
        this.pageNumber = pageNumber;
    }

    public FilterRowByLongAtAGivenOffsetWithPage(
        long longOffset,
        long firstLong,
        long lastLong,
        TypeValue typeValue,
        int pageSize,
        int pageNumber
    ) {
        super();
        this.longOffset = longOffset;
        this.firstLong = firstLong;
        this.lastLong = lastLong;
        this.typeValue = typeValue;
        this.pageSize = pageSize;
        this.pageNumber = pageNumber;
    }

    public FilterRowByLongAtAGivenOffsetWithPage() { super(); }

    @Override
    public void reset() { this.filterOutRow = false; }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (this.filterOutRow) { return ReturnCode.NEXT_ROW; }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        if (this.stopFiltering) { return true; }
        boolean criteriaIsMatching = false;
        switch (this.typeValue) {
            case BYTE: {
                byte value = data[offset + (int) this.longOffset];
                criteriaIsMatching = ((value >= (byte) this.firstLong) && (value <= (byte) this.lastLong));
                break;
            }
            case USHORT: {
                short value = Bytes.toShort(data, offset + (int) this.longOffset);
                criteriaIsMatching = ((value >= (short) this.firstLong) && (value <= (short) this.lastLong));
                System.out.println(
                    this + " filterRowKey this.typeValue=" + this.typeValue + ", this.pageSize=" + this.pageSize
                        + " , this.pageNumber=" + this.pageNumber + ", this.currentPageNumber = "
                        + this.currentPageNumber + " // value = " + value + " - firstLong = " + this.firstLong
                        + " / lastLong=" + this.lastLong);
                break;
            }
            case INT: {
                int value = Bytes.toInt(data, offset + (int) this.longOffset);
                criteriaIsMatching = ((value >= (int) this.firstLong) && (value <= (int) this.lastLong));
                break;
            }
            case LONG: {
                long value = Bytes.toLong(data, offset + (int) this.longOffset);
                criteriaIsMatching = ((value >= this.firstLong) && (value <= this.lastLong));
                break;
            }
        }
        if (criteriaIsMatching) {
            this.pageIsFound = this.pageNumber == this.currentPageNumber;
            this.currentRowNumberInPage++;
            if (this.currentRowNumberInPage > this.pageSize) {
                System.out.println(this + " Move to next page " + this.currentRowNumberInPage);
                this.currentRowNumberInPage = 0;
                this.currentPageNumber++;
                this.pageIsFound = this.pageNumber == this.currentPageNumber;
                System.out.println(this + " Move to next page " + this.currentRowNumberInPage);

            }
            this.stopFiltering = (this.pageIsFound && (this.currentRowNumberInPage >= this.pageSize));
            this.filterOutRow = (!this.pageIsFound) || this.stopFiltering;
            System.out.println(this + " criteriaIsMatching..., stopFiltering is " + this.stopFiltering);
        } else {
            this.filterOutRow = true;
        }
        return this.filterOutRow;

    }

    @Override
    public boolean filterAllRemaining() throws IOException { return this.stopFiltering; }

    GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage convert() {
        GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage
            .newBuilder();
        builder.setFirstLong(this.firstLong)
            .setLastLong(this.lastLong)
            .setLongOffset(this.longOffset)
            .setPageSize(this.pageSize)
            .setStartRownumber(this.pageNumber)
            .setTypeValue(
                GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage.TypeValue.valueOf(this.typeValue.getValue()));
        return builder.build();
    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        long longOffset = ParseFilter.convertByteArrayToLong(filterArguments.get(0));
        long firstLong = ParseFilter.convertByteArrayToLong(filterArguments.get(1));
        long lastLong = ParseFilter.convertByteArrayToLong(filterArguments.get(2));
        TypeValue typeValue = TypeValue.toTypeValue(ParseFilter.convertByteArrayToInt(filterArguments.get(3)));
        int pageSize = ParseFilter.convertByteArrayToInt(filterArguments.get(4));
        int pageNumber = ParseFilter.convertByteArrayToInt(filterArguments.get(5));

        return new FilterRowByLongAtAGivenOffsetWithPage(longOffset,
            firstLong,
            lastLong,
            typeValue,
            pageSize,
            pageNumber);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage
            .newBuilder();
        builder.setFirstLong(this.firstLong)
            .setLastLong(this.lastLong)
            .setLongOffset(this.longOffset)
            .setTypeValue(
                GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage.TypeValue.valueOf(this.typeValue.getValue()))
            .setPageSize(this.pageSize)
            .setStartRownumber(this.pageNumber);
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
    public static FilterRowByLongAtAGivenOffsetWithPage parseFrom(final byte[] pbBytes)
        throws DeserializationException {
        GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage proto;
        try {
            proto = GsFilterProtos.FilterRowByLongAtAGivenOffsetWithPage.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        return new FilterRowByLongAtAGivenOffsetWithPage(proto.getLongOffset(),
            proto.getFirstLong(),
            proto.getLastLong(),
            TypeValue.toTypeValue(
                proto.getTypeValue()
                    .getNumber()),
            proto.getPageSize(),
            proto.getStartRownumber());
    }

    @Override
    public String toString() {
        return "FilterRowByLongAtAGivenOffsetWithPage [filterOutRow=" + this.filterOutRow + ", longOffset="
            + this.longOffset + ", firstLong=" + this.firstLong + ", lastLong=" + this.lastLong
            + ", currentRowNumberInPage=" + this.currentRowNumberInPage + ", currentPageNumber="
            + this.currentPageNumber + ", pageNumber=" + this.pageNumber + ", pageSize=" + this.pageSize
            + ", pageIsFound=" + this.pageIsFound + ", stopFiltering=" + this.stopFiltering + ", typeValue="
            + this.typeValue + "]";
    }

}
