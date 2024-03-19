package com.workflow.model.files;

import java.io.Serializable;

import org.apache.avro.reflect.Nullable;

import com.workflow.model.HbaseData;
import com.workflow.model.events.ImportEvent;

public class FileToProcess extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    private FileToProcess     parent;
    private String            url;
    private String            name;
    private Boolean           isLocal;
    @Nullable
    private String            imageId;
    private boolean           compressedFile;
    private long              importDate;
    private ImportEvent       importEvent;

    private FileToProcess(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.parent = builder.parent;
        this.url = builder.url;
        this.name = builder.name;
        this.isLocal = builder.isLocal;
        this.imageId = builder.imageId;
        this.compressedFile = builder.compressedFile;
        this.importDate = builder.importDate;
        this.importEvent = builder.importEvent;
    }

    public FileToProcess getParent() { return this.parent; }

    public String getUrl() { return this.url; }

    public void setUrl(String url) { this.url = url; }

    public ImportEvent getImportEvent() { return this.importEvent; }

    public long getImportDate() { return this.importDate; }

    public boolean isCompressedFile() { return this.compressedFile; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public String getName() { return this.name; }

    public void setName(String name) { this.name = name; }

    public Boolean getIsLocal() { return this.isLocal; }

    public void setIsLocal(Boolean isLocal) { this.isLocal = isLocal; }

    public void setParent(FileToProcess parent) { this.parent = parent; }

    public void setCompressedFile(boolean compressedFile) { this.compressedFile = compressedFile; }

    public void setImportDate(long importDate) { this.importDate = importDate; }

    public void setImportEvent(ImportEvent importEvent) { this.importEvent = importEvent; }

    @Override
    public String toString() {
        return "FileToProcess [parent=" + this.parent + ", url=" + this.url + ", name=" + this.name + ", isLocal="
            + this.isLocal + ", imageId=" + this.imageId + ", compressedFile=" + this.compressedFile + ", importDate="
            + this.importDate + ", importEvent=" + this.importEvent + "]";
    }

    public FileToProcess() { super("<not set>",
        System.currentTimeMillis()); }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + (this.compressedFile ? 1231 : 1237);
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        result = (prime * result) + (int) (this.importDate ^ (this.importDate >>> 32));
        result = (prime * result) + ((this.importEvent == null) ? 0 : this.importEvent.hashCode());
        result = (prime * result) + ((this.isLocal == null) ? 0 : this.isLocal.hashCode());
        result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
        result = (prime * result) + ((this.parent == null) ? 0 : this.parent.hashCode());
        result = (prime * result) + ((this.url == null) ? 0 : this.url.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        FileToProcess other = (FileToProcess) obj;
        if (this.compressedFile != other.compressedFile) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.importDate != other.importDate) { return false; }
        if (this.importEvent == null) {
            if (other.importEvent != null) { return false; }
        } else if (!this.importEvent.equals(other.importEvent)) { return false; }
        if (this.isLocal == null) {
            if (other.isLocal != null) { return false; }
        } else if (!this.isLocal.equals(other.isLocal)) { return false; }
        if (this.name == null) {
            if (other.name != null) { return false; }
        } else if (!this.name.equals(other.name)) { return false; }
        if (this.parent == null) {
            if (other.parent != null) { return false; }
        } else if (!this.parent.equals(other.parent)) { return false; }
        if (this.url == null) {
            if (other.url != null) { return false; }
        } else if (!this.url.equals(other.url)) { return false; }
        return true;
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private long          dataCreationDate;
        private String        dataId;
        private FileToProcess parent;
        private String        url;
        private String        name;
        private Boolean       isLocal;
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
         * Builder method for url parameter.
         *
         * @param url
         *            field to set
         * @return builder
         */
        public Builder withUrl(String url) {
            this.url = url;
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
         * Builder method for isLocal parameter.
         *
         * @param isLocal
         *            field to set
         * @return builder
         */
        public Builder withIsLocal(Boolean isLocal) {
            this.isLocal = isLocal;
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
