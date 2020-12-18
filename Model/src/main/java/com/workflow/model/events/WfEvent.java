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
        return Objects.equals(this.imgId, other.imgId) && Objects.equals(this.parentDataId, other.parentDataId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(
            this.getClass()
                .getSimpleName())
            .append(" [imgId=")
            .append(this.imgId)
            .append(", [dataId=")
            .append(this.dataId)
            .append(", parentDataId=")
            .append(this.parentDataId)
            .append("]");
        return builder.toString();
    }

}
