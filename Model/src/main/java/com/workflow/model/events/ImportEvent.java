package com.workflow.model.events;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.annotation.Generated;

import org.apache.avro.reflect.Nullable;

import com.workflow.model.HbaseData;

public class ImportEvent extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    private List<String>      keyWords;
    private List<String>      scanners;
    private String            album;
    private long              importDate;
    private String            importName;

    @Generated("SparkTools")
    private ImportEvent(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.keyWords = builder.keyWords;
        this.scanners = builder.scanners;
        this.album = builder.album;
        this.importDate = builder.importDate;
        this.importName = builder.importName;
    }

    public List<String> getKeyWords() { return this.keyWords; }

    public void setKeyWords(List<String> keyWords) { this.keyWords = keyWords; }

    public List<String> getScanners() { return this.scanners; }

    public void setScanners(List<String> scanners) { this.scanners = scanners; }

    public String getAlbum() { return this.album; }

    public void setAlbum(String album) { this.album = album; }

    public long getImportDate() { return this.importDate; }

    public void setImportDate(long importDate) { this.importDate = importDate; }

    public String getImportName() { return this.importName; }

    public void setImportName(String importName) { this.importName = importName; }

    @Override
    public String toString() {
        return "ImportEvent [keyWords=" + this.keyWords + ", scanners=" + this.scanners + ", album=" + this.album
            + ", importDate=" + this.importDate + ", importName=" + this.importName + "]";
    }

    public ImportEvent() { super("<not set>",
        System.currentTimeMillis()); }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + ((this.album == null) ? 0 : this.album.hashCode());
        result = (prime * result) + (int) (this.importDate ^ (this.importDate >>> 32));
        result = (prime * result) + ((this.importName == null) ? 0 : this.importName.hashCode());
        result = (prime * result) + ((this.keyWords == null) ? 0 : this.keyWords.hashCode());
        result = (prime * result) + ((this.scanners == null) ? 0 : this.scanners.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        ImportEvent other = (ImportEvent) obj;
        if (this.album == null) {
            if (other.album != null) { return false; }
        } else if (!this.album.equals(other.album)) { return false; }
        if (this.importDate != other.importDate) { return false; }
        if (this.importName == null) {
            if (other.importName != null) { return false; }
        } else if (!this.importName.equals(other.importName)) { return false; }
        if (this.keyWords == null) {
            if (other.keyWords != null) { return false; }
        } else if (!this.keyWords.equals(other.keyWords)) { return false; }
        if (this.scanners == null) {
            if (other.scanners != null) { return false; }
        } else if (!this.scanners.equals(other.scanners)) { return false; }
        return true;
    }

    /**
     * Creates builder to build {@link ImportEvent}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImportEvent}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long         dataCreationDate;
        private String       dataId;
        private List<String> keyWords = Collections.emptyList();
        private List<String> scanners = Collections.emptyList();
        private String       album;
        private long         importDate;
        private String       importName;

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         *
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         *
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for keyWords parameter.
         *
         * @param keyWords
         *            field to set
         * @return builder
         */
        public Builder withKeyWords(List<String> keyWords) {
            this.keyWords = keyWords;
            return this;
        }

        /**
         * Builder method for scanners parameter.
         *
         * @param scanners
         *            field to set
         * @return builder
         */
        public Builder withScanners(List<String> scanners) {
            this.scanners = scanners;
            return this;
        }

        /**
         * Builder method for album parameter.
         *
         * @param album
         *            field to set
         * @return builder
         */
        public Builder withAlbum(String album) {
            this.album = album;
            return this;
        }

        /**
         * Builder method for importDate parameter.
         *
         * @param importDate
         *            field to set
         * @return builder
         */
        public Builder withImportDate(long importDate) {
            this.importDate = importDate;
            return this;
        }

        /**
         * Builder method for importName parameter.
         *
         * @param importName
         *            field to set
         * @return builder
         */
        public Builder withImportName(String importName) {
            this.importName = importName;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImportEvent build() { return new ImportEvent(this); }
    }

}
