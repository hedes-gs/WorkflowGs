package com.workflow.model.files;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Generated;

import org.apache.avro.reflect.Nullable;

import com.workflow.model.HbaseData;
import com.workflow.model.events.ImportEvent;

public class FileToProcess extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    private FileToProcess     parent;
    private String            name;
    private String            rootForNfs;
    private String            path;
    private String            host;
    @Nullable
    private String            imageId;
    private boolean           compressedFile;
    private long              importDate;
    private ImportEvent       importEvent;

    @Generated("SparkTools")
    private FileToProcess(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.parent = builder.parent;
        this.name = builder.name;
        this.rootForNfs = builder.rootForNfs;
        this.path = builder.path;
        this.host = builder.host;
        this.imageId = builder.imageId;
        this.compressedFile = builder.compressedFile;
        this.importDate = builder.importDate;
        this.importEvent = builder.importEvent;
    }

    public FileToProcess getParent() { return this.parent; }

    public String getRootForNfs() { return this.rootForNfs; }

    public ImportEvent getImportEvent() { return this.importEvent; }

    public String getName() { return this.name; }

    public String getPath() { return this.path; }

    public String getHost() { return this.host; }

    public long getImportDate() { return this.importDate; }

    public boolean isCompressedFile() { return this.compressedFile; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    @Override
    public String toString() {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("FileToProcess [parent=");
        builder2.append(this.parent);
        builder2.append(", name=");
        builder2.append(this.name);
        builder2.append(", rootForNfs=");
        builder2.append(this.rootForNfs);
        builder2.append(", path=");
        builder2.append(this.path);
        builder2.append(", host=");
        builder2.append(this.host);
        builder2.append(", imageId=");
        builder2.append(this.imageId);
        builder2.append(", compressedFile=");
        builder2.append(this.compressedFile);
        builder2.append(", importDate=");
        builder2.append(this.importDate);
        builder2.append(", importEvent=");
        builder2.append(this.importEvent);
        builder2.append("]");
        return builder2.toString();
    }

    public FileToProcess() { super("<not set>",
        System.currentTimeMillis()); }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(
            this.compressedFile,
            this.host,
            this.imageId,
            this.importDate,
            this.importEvent,
            this.name,
            this.parent,
            this.path,
            this.rootForNfs);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        FileToProcess other = (FileToProcess) obj;
        return (this.compressedFile == other.compressedFile) && Objects.equals(this.host, other.host)
            && Objects.equals(this.imageId, other.imageId) && (this.importDate == other.importDate)
            && Objects.equals(this.importEvent, other.importEvent) && Objects.equals(this.name, other.name)
            && Objects.equals(this.parent, other.parent) && Objects.equals(this.path, other.path)
            && Objects.equals(this.rootForNfs, other.rootForNfs);
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
        private String        rootForNfs;
        private String        path;
        private String        host;
        private String        imageId;
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
         * Builder method for rootForNfs parameter.
         *
         * @param rootForNfs
         *            field to set
         * @return builder
         */
        public Builder withRootForNfs(String rootForNfs) {
            this.rootForNfs = rootForNfs;
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
         * Builder method for imageId parameter.
         *
         * @param imageId
         *            field to set
         * @return builder
         */
        public Builder withImageId(String imageId) {
            this.imageId = imageId;
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
