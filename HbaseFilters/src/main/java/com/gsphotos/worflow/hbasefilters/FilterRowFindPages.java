package com.gsphotos.worflow.hbasefilters;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.generated.GsFilterProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowFindPages extends FilterBase {

    private boolean filterOutRow = false;
    private byte[]  keyByteArray;
    private int     currentRowNumberInPage;
    private int     currentPageNumber;
    private int     pageNumber;
    private int     pageSize;
    private boolean pageIsFound;
    private boolean stopFiltering;

    public FilterRowFindPages(
        byte[] keyByteArray,
        int pageNumber,
        int pageSize
    ) {
        super();
        this.keyByteArray = keyByteArray;
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.pageIsFound = false;
        this.filterOutRow = false;
        this.stopFiltering = false;
    }

    public FilterRowFindPages() { super(); }

    @Override
    public void reset() { this.filterOutRow = false; }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        System.out.println("   filterKeyValue  nextRowIsFound = " + this.pageIsFound);
        if (this.pageIsFound) { return ReturnCode.INCLUDE; }
        return ReturnCode.NEXT_ROW;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {

        if (this.stopFiltering) {
            this.filterOutRow = true;
            this.stopFiltering = true;
            return true;
        }
        boolean byteArrayIsEqual = FilterRowFindPages
            .compareByteArrays(data, offset, this.keyByteArray, 0, this.keyByteArray.length);
        if (byteArrayIsEqual) {
            this.pageIsFound = this.pageNumber == this.currentPageNumber;
            this.filterOutRow = false;
            this.currentRowNumberInPage++;
            if (this.currentRowNumberInPage > this.pageSize) {
                this.currentRowNumberInPage = 0;
                this.currentPageNumber++;
                this.pageIsFound = this.pageNumber == this.currentPageNumber;
            }
            this.stopFiltering = (this.pageIsFound && (this.currentRowNumberInPage == this.pageSize));
        } else if (this.pageIsFound) {
            this.filterOutRow = true;
            this.stopFiltering = true;
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

    GsFilterProtos.FilterRowFindPages convert() {
        GsFilterProtos.FilterRowFindPages.Builder builder = GsFilterProtos.FilterRowFindPages.newBuilder();
        builder.setKeyByteArrayToFind(ByteString.copyFrom(this.keyByteArray))
            .setPageSize(this.pageSize)
            .setStartRownumber(this.pageNumber);
        return builder.build();

    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        byte[] keyByteArray = filterArguments.get(0);
        int pageSize = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
        int pageNumber = ParseFilter.convertByteArrayToInt(filterArguments.get(2));
        return new FilterRowFindPages(keyByteArray, pageSize, pageNumber);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterRowFindPages.Builder builder = GsFilterProtos.FilterRowFindPages.newBuilder();
        builder.setKeyByteArrayToFind(ByteString.copyFrom(this.keyByteArray))
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
    public static FilterRowFindPages parseFrom(final byte[] pbBytes) throws DeserializationException {
        GsFilterProtos.FilterRowFindPages proto;
        try {
            proto = GsFilterProtos.FilterRowFindPages.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new FilterRowFindPages(proto.getKeyByteArrayToFind()
            .toByteArray(), proto.getStartRownumber(), proto.getPageSize());
    }

}
