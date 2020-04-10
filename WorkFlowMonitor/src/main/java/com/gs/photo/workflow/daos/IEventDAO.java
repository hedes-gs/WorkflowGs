package com.gs.photo.workflow.daos;

import com.workflow.model.events.WfEvents;

public interface IEventDAO {

    public void addOrCreate(WfEvents events);

    public boolean isRootNodeCompleted(String rootId);

    public int getNbOfEvents();

    public void truncate();

}
