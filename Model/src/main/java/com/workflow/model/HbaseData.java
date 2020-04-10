package com.workflow.model;

import java.io.Serializable;
import java.util.Objects;

public class HbaseData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private long              dataCreationDate;
    private String            dataId;

    public String getDataId() { return this.dataId; }

    public void setDataId(String dataId) { this.dataId = dataId; }

    protected HbaseData(
        String dataId,
        long dataCreationDate
    ) {
        super();
        this.dataId = dataId;
        this.dataCreationDate = dataCreationDate;
    }

    @Override
    public int hashCode() { return Objects.hash(this.dataId); }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!(obj instanceof HbaseData)) { return false; }
        HbaseData other = (HbaseData) obj;
        return Objects.equals(this.dataId, other.dataId);
    }

}
