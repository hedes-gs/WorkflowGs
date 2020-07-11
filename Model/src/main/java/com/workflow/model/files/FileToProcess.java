package com.workflow.model.files;

import java.io.Serializable;

import javax.annotation.Generated;

import org.apache.avro.reflect.Nullable;

import com.workflow.model.HbaseData;
import com.workflow.model.events.ImportEvent;

public class FileToProcess extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    private FileToProcess     parent;
    private String            name;
    private String            path;
    private String            host;
    private boolean           compressedFile;
    private long              importDate;
    private ImportEvent       importEvent;

    @Generated("SparkTools")
    private FileToProcess(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.parent = builder.parent;
        this.name = builder.name;
        this.path = builder.path;
        this.host = builder.host;
        this.compressedFile = builder.compressedFile;
        this.importDate = builder.importDate;
        this.importEvent = builder.importEvent;
    }

    public ImportEvent getImportEvent() { return this.importEvent; }

    public String getName() { return this.name; }

    public String getPath() { return this.path; }

    public String getHost() { return this.host; }

    public long getImportDate() { return this.importDate; }

    public boolean isCompressedFile() { return this.compressedFile; }

    @Override
    public String toString() {
        return "FileToProcess [parent=" + this.parent + ", name=" + this.name + ", path=" + this.path + ", host="
            + this.host + ", compressedFile=" + this.compressedFile + "]";
    }

    public FileToProcess() { super("<not set>",
        System.currentTimeMillis()); }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + ((this.host == null) ? 0 : this.host.hashCode());
        result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
        result = (prime * result) + ((this.parent == null) ? 0 : this.parent.hashCode());
        result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        FileToProcess other = (FileToProcess) obj;
        if (this.host == null) {
            if (other.host != null) { return false; }
        } else if (!this.host.equals(other.host)) { return false; }
        if (this.name == null) {
            if (other.name != null) { return false; }
        } else if (!this.name.equals(other.name)) { return false; }
        if (this.parent == null) {
            if (other.parent != null) { return false; }
        } else if (!this.parent.equals(other.parent)) { return false; }
        if (this.path == null) {
            if (other.path != null) { return false; }
        } else if (!this.path.equals(other.path)) { return false; }
        return true;
    }

    /**
     * Creates builder to build {@link FileToProcess}.
     * 
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link FileToProcess}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long          dataCreationDate;
        private String        dataId;
        private FileToProcess parent;
        private String        name;
        private String        path;
        private String        host;
        private boolean       compressedFile;
        private long          importDate;
        private ImportEvent   importEvent;

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
         * Builder method for parent parameter.
         * 
         * @param parent
         *            field to set
         * @return builder
         */
        public Builder withParent(FileToProcess parent) {
            this.parent = parent;
            return this;
        }

        /**
         * Builder method for name parameter.
         * 
         * @param name
         *            field to set
         * @return builder
         */
        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Builder method for path parameter.
         * 
         * @param path
         *            field to set
         * @return builder
         */
        public Builder withPath(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builder method for host parameter.
         * 
         * @param host
         *            field to set
         * @return builder
         */
        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Builder method for compressedFile parameter.
         * 
         * @param compressedFile
         *            field to set
         * @return builder
         */
        public Builder withCompressedFile(boolean compressedFile) {
            this.compressedFile = compressedFile;
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
         * Builder method for importEvent parameter.
         * 
         * @param importEvent
         *            field to set
         * @return builder
         */
        public Builder withImportEvent(ImportEvent importEvent) {
            this.importEvent = importEvent;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public FileToProcess build() { return new FileToProcess(this); }
    }

}
