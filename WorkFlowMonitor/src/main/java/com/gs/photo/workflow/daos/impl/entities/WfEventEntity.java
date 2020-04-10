package com.gs.photo.workflow.daos.impl.entities;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class WfEventEntity {
    @Id
    protected String id;

    protected String data;
    protected int    state;
    protected String path;

    public String getId() { return this.id; }

    public void setId(String id) { this.id = id; }

    public String getData() { return this.data; }

    public void setData(String data) { this.data = data; }

    public int getState() { return this.state; }

    public void setState(int state) { this.state = state; }

    public String getPath() { return this.path; }

    public void setPath(String path) { this.path = path; }

}
