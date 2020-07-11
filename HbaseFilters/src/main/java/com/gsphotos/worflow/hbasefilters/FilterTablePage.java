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

import com.google.protobuf.InvalidProtocolBufferException;

public class FilterTablePage extends FilterBase {

    private boolean filterOutRow = false;
    private int     currentRowNumberInPage;
    private int     currentPageNumber;
    private int     pageNumber;
    private int     pageSize;
    private boolean pageIsFound;
    private boolean stopFiltering;

    public FilterTablePage(
        int pageNumber,
        int pageSize
    ) {
        super();
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.pageIsFound = false;
        this.filterOutRow = false;
        this.stopFiltering = false;
    }

    public FilterTablePage() { super(); }

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
        this.filterOutRow = false;
        this.currentRowNumberInPage++;
        this.pageIsFound = this.pageNumber == this.currentPageNumber;

        if (this.currentRowNumberInPage > this.pageSize) {
            this.currentRowNumberInPage = 0;
            this.currentPageNumber++;
            this.pageIsFound = this.pageNumber == this.currentPageNumber;
        }
        this.stopFiltering = (this.pageIsFound && (this.currentRowNumberInPage == this.pageSize));
        return this.filterOutRow;
    }

    @Override
    public boolean filterAllRemaining() throws IOException { return this.stopFiltering; }

    GsFilterProtos.FilterTablePage convert() {
        GsFilterProtos.FilterTablePage.Builder builder = GsFilterProtos.FilterTablePage.newBuilder();
        builder.setPageSize(this.pageSize)
            .setStartRownumber(this.pageNumber);
        return builder.build();

    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
        @SuppressWarnings("rawtypes") // for arguments
        int pageSize = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
        int pageNumber = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
        return new FilterTablePage(pageSize, pageNumber);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        GsFilterProtos.FilterTablePage.Builder builder = GsFilterProtos.FilterTablePage.newBuilder();
        builder.setPageSize(this.pageSize)
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
    public static FilterTablePage parseFrom(final byte[] pbBytes) throws DeserializationException {
        GsFilterProtos.FilterTablePage proto;
        try {
            proto = GsFilterProtos.FilterTablePage.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new FilterTablePage(proto.getStartRownumber(), proto.getPageSize());
    }

}
