package com.gs.photo.common.workflow.hbase.dao;

import java.util.Comparator;
import java.util.List;

public class PageDescription<T> {
    protected long    hbasePageNumber;
    protected List<T> pageContent;

    public long getHbasePageNumber() { return this.hbasePageNumber; }

    public List<T> getPageContent() { return this.pageContent; }

    public int getPageSize() { return this.pageContent.size(); }

    public PageDescription(
        long hbasePageNumber,
        List<T> pageContent
    ) {
        super();
        this.hbasePageNumber = hbasePageNumber;
        this.pageContent = pageContent;
    }

    public void sort(Comparator<T> c) { this.pageContent.sort(c); }
}