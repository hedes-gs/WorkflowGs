package com.workflow.model.events;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.avro.reflect.Nullable;

import com.workflow.model.HbaseData;

public class ComponentEvent extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    public static enum ComponentStatus {
        ALIVE, STOPPED
    }

    public static enum ComponentType {
        SCAN, WF_HBASE
    }

    protected ComponentStatus status;
    protected ComponentType   componentType;
    @Nullable
    protected String          message;
    @Nullable
    protected String[]        scannedFolder;

    protected String          componentName;

    private ComponentEvent(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.status = builder.status;
        this.componentType = builder.componentType;
        this.message = builder.message;
        this.scannedFolder = builder.scannedFolder;
        this.componentName = builder.componentName;
    }

    public ComponentEvent() { super(); }

    public ComponentEvent(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public String[] getScannedFolder() { return this.scannedFolder; }

    public void setScannedFolder(String[] scannedFolder) { this.scannedFolder = scannedFolder; }

    public String getComponentName() { return this.componentName; }

    public void setComponentName(String componentName) { this.componentName = componentName; }

    public ComponentStatus getStatus() { return this.status; }

    public void setStatus(ComponentStatus status) { this.status = status; }

    public ComponentType getComponentType() { return this.componentType; }

    public void setComponentType(ComponentType componentType) { this.componentType = componentType; }

    public String getMessage() { return this.message; }

    public void setMessage(String message) { this.message = message; }

    /**
     * Creates builder to build {@link ComponentEvent}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ComponentEvent}.
     */
    public static final class Builder {
        private long            dataCreationDate;
        private String          dataId;
        private ComponentStatus status;
        private ComponentType   componentType;
        private String          message;
        private String[]        scannedFolder;
        private String          componentName;

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
         * Builder method for status parameter.
         *
         * @param status
         *            field to set
         * @return builder
         */
        public Builder withStatus(ComponentStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Builder method for componentType parameter.
         *
         * @param componentType
         *            field to set
         * @return builder
         */
        public Builder withComponentType(ComponentType componentType) {
            this.componentType = componentType;
            return this;
        }

        /**
         * Builder method for message parameter.
         *
         * @param message
         *            field to set
         * @return builder
         */
        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        /**
         * Builder method for scannedFolder parameter.
         *
         * @param scannedFolder
         *            field to set
         * @return builder
         */
        public Builder withScannedFolder(String[] scannedFolder) {
            this.scannedFolder = scannedFolder;
            return this;
        }

        /**
         * Builder method for componentName parameter.
         *
         * @param componentName
         *            field to set
         * @return builder
         */
        public Builder withComponentName(String componentName) {
            this.componentName = componentName;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ComponentEvent build() { return new ComponentEvent(this); }
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder2 = new StringBuilder();
        builder2.append("ComponentEvent [status=");
        builder2.append(this.status);
        builder2.append(", componentType=");
        builder2.append(this.componentType);
        builder2.append(", message=");
        builder2.append(this.message);
        builder2.append(", scannedFolder=");
        builder2.append(
            this.scannedFolder != null ? Arrays.asList(this.scannedFolder)
                .subList(0, Math.min(this.scannedFolder.length, maxLen)) : null);
        builder2.append(", componentName=");
        builder2.append(this.componentName);
        builder2.append("]");
        return builder2.toString();
    }

}
