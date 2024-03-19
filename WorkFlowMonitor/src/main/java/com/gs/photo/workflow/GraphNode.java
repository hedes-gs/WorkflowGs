package com.gs.photo.workflow;

import com.workflow.model.events.WfEvent;

public class GraphNode<T extends WfEvent> {

    protected T nodeContent;

    public GraphNode(T nodeContent) {
        super();
        this.nodeContent = nodeContent;
    }

}