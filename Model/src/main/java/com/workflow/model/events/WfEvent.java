package com.workflow.model.events;

import java.io.Serializable;
import java.util.Objects;

import com.workflow.model.HbaseData;

public class WfEvent extends HbaseData implements Serializable, Comparable<WfEvent> {

    private static final long serialVersionUID = 1L;
    protected String          imgId;
    protected String          parentDataId;
    protected WfEventStep     step;

    public WfEvent() { super(null,
        0); }

    public String getParentDataId() { return this.parentDataId; }

    public WfEventStep getStep() { return this.step; }

    public String getImgId() { return this.imgId; }

    @Override
    public int compareTo(WfEvent o) { return this.imgId.compareTo(o.imgId); }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(this.imgId, this.parentDataId);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        WfEvent other = (WfEvent) obj;
        return Objects.equals(this.imgId, other.imgId) && Objects.equals(this.parentDataId, other.parentDataId)
            && Objects.equals(this.dataId, other.dataId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(
            this.getClass()
                .getSimpleName());
        builder.append(" -> [imgId=");
        builder.append(this.imgId);
        builder.append(", parentDataId=");
        builder.append(this.parentDataId);
        builder.append(", step=");
        builder.append(this.step);
        builder.append(", dataCreationDate=");
        builder.append(this.dataCreationDate);
        builder.append(", dataId=");
        builder.append(this.dataId);
        builder.append("]");
        return builder.toString();
    }

}
