package com.gs.photos.ws.controllers;

import java.io.Serializable;

import javax.annotation.Generated;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.workflow.model.dtos.ImageKeyDto;

public class GsPageRequest implements Pageable, Serializable {

    protected ImageKeyDto firstRow;
    protected ImageKeyDto lastRow;
    protected int         page;
    protected int         pageSize;
    protected Sort        sort;

    public GsPageRequest() {}

    public ImageKeyDto getFirstRow() { return this.firstRow; }

    public void setFirstRow(ImageKeyDto firstRow) { this.firstRow = firstRow; }

    public ImageKeyDto getLastRow() { return this.lastRow; }

    public void setLastRow(ImageKeyDto lastRow) { this.lastRow = lastRow; }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Generated("SparkTools")
    private GsPageRequest(Builder builder) {
        this.firstRow = builder.firstRow;
        this.lastRow = builder.lastRow;
        this.page = builder.page;
        this.pageSize = builder.pageSize;
        this.sort = builder.sort;
    }

    public int getPage() { return this.page; }

    public void setPage(int page) { this.page = page; }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.data.domain.Pageable#next()
     */
    @Override
    public Pageable next() {
        return new Builder().withFirstRow(this.firstRow)
            .withLastRow(this.lastRow)
            .withPage(this.page + 1)
            .withPageSize(this.pageSize)
            .build();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.data.domain.AbstractPageRequest#previous()
     */
    public Pageable previous() {
        return this.getPageNumber() == 0 ? this
            : new Builder().withFirstRow(this.firstRow)
                .withLastRow(this.lastRow)
                .withPage(this.page - 1)
                .withPageSize(this.pageSize)
                .build();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.data.domain.Pageable#hasPrevious()
     */
    @Override
    public boolean hasPrevious() { return this.page > 0; }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.data.domain.Pageable#previousOrFirst()
     */
    @Override
    public Pageable previousOrFirst() { return this.hasPrevious() ? this.previous() : this.first(); }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.data.domain.Pageable#first()
     */
    @Override
    public Pageable first() {
        return new Builder().withFirstRow(this.firstRow)
            .withLastRow(this.lastRow)
            .withPage(0)
            .withPageSize(this.pageSize)
            .build();
    }

    @Override
    public long getOffset() { return (long) this.page * this.pageSize; }

    @Override
    public int getPageNumber() { return this.page; }

    @Override
    public int getPageSize() { return this.pageSize; }

    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public void setSort(Sort sort) { this.sort = sort; }

    @Override
    public Sort getSort() { // TODO Auto-generated method stub
        return this.sort;
    }

    /**
     * Creates builder to build {@link GsPageRequest}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link GsPageRequest}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private ImageKeyDto firstRow;
        private ImageKeyDto lastRow;
        private int         page;
        private int         pageSize;
        private Sort        sort;

        private Builder() {}

        /**
         * Builder method for firstRow parameter.
         *
         * @param firstRow
         *            field to set
         * @return builder
         */
        public Builder withFirstRow(ImageKeyDto firstRow) {
            this.firstRow = firstRow;
            return this;
        }

        /**
         * Builder method for lastRow parameter.
         *
         * @param lastRow
         *            field to set
         * @return builder
         */
        public Builder withLastRow(ImageKeyDto lastRow) {
            this.lastRow = lastRow;
            return this;
        }

        /**
         * Builder method for page parameter.
         *
         * @param page
         *            field to set
         * @return builder
         */
        public Builder withPage(int page) {
            this.page = page;
            return this;
        }

        /**
         * Builder method for pageSize parameter.
         *
         * @param pageSize
         *            field to set
         * @return builder
         */
        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Builder method for sort parameter.
         *
         * @param sort
         *            field to set
         * @return builder
         */
        public Builder withSort(Sort sort) {
            this.sort = sort;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public GsPageRequest build() { return new GsPageRequest(this); }
    }

    @Override
    public Pageable withPage(int pageNumber) {

        return new Builder().withFirstRow(this.firstRow)
            .withLastRow(this.lastRow)
            .withPage(pageNumber - 1)
            .withPageSize(this.pageSize)
            .build();

    }

}
