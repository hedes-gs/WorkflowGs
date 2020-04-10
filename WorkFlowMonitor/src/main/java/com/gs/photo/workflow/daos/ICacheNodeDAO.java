package com.gs.photo.workflow.daos;

import com.workflow.model.events.WfEvents;

public interface ICacheNodeDAO {
    public void addOrCreate(WfEvents events);

    public boolean allEventsAreReceivedForAnImage(String imgId);
}
