package com.gs.photo.workflow.daos.impl;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringJoiner;

import javax.annotation.PostConstruct;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.daos.ICacheNodeDAO;
import com.gs.photo.workflow.daos.impl.tree.Node;
import com.gs.photo.workflow.daos.impl.tree.Node.HashKeyCompute;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

public class CacheNodeDAO implements ICacheNodeDAO<String, WfEvents> {

    protected static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(256);

    protected static class LocalHashKeyCompute implements HashKeyCompute<WfEventNode> {

        @Override
        public long compute(WfEventNode v) {
            long hashCode = CacheNodeDAO.HASH_FUNCTION.newHasher()
                .putString(v.event.getDataId(), Charset.forName("UTf-8"))
                .putString(v.event.getParentDataId(), Charset.forName("UTf-8"))
                .putString(v.event.getImgId(), Charset.forName("UTf-8"))
                .hash()
                .asLong();
            return hashCode;
        }
    }

    static protected HashKeyCompute<WfEventNode> HASH_KEY_COMPUTE = new LocalHashKeyCompute();

    protected static class WfEventNode implements Comparable<WfEventNode> {
        protected long    hashCode;
        protected WfEvent event;

        public WfEvent getEvent() { return this.event; }

        public void setEvent(WfEvent event) { this.event = event; }

        @Override
        public int hashCode() { return (int) this.hashCode; }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) { return true; }
            if (obj == null) { return false; }
            if (this.getClass() != obj.getClass()) { return false; }
            WfEventNode other = (WfEventNode) obj;
            if (this.hashCode != other.hashCode) { return false; }

            if (this.event == null) {
                if (other.event != null) { return false; }
            } else if (!this.eventAreSame(this.event, other.event)) { return false; }
            return true;
        }

        private boolean eventAreSame(WfEvent event2, WfEvent event3) {
            return event2.getImgId()
                .equals(event3.getImgId())
                && event2.getDataId()
                    .equals(event3.getDataId())
                && event2.getParentDataId()
                    .equals(event3.getParentDataId());
        }

        @Override
        public int compareTo(WfEventNode o) {
            final int compare = Long.compare(this.hashCode, o.hashCode);
            return compare == 0 ? (this.eventAreSame(this.event, o.event) ? 0 : 1) : compare;
        }

        public WfEventNode(WfEvent event) {
            super();
            this.event = event;
        }

    }

    protected Node<WfEventNode> treeNode;

    @Override
    @PostConstruct
    public void init() {
        WfEvent eventRoot = WfEventProduced.builder()
            .withDataId("")
            .withImgId("")
            .withParentDataId("")
            .build();
        this.treeNode = Node.getRoot(new WfEventNode(eventRoot), CacheNodeDAO.HASH_KEY_COMPUTE);
    }

    @Override
    public WfEvents addOrCreate(String key, WfEvents events) {
        events.getEvents()
            .forEach((evt) -> this.addOrCreate(evt));
        return events;
    }

    private void addOrCreate(WfEvent event) {
        WfEvent eventImgId = WfEventProduced.builder()
            .withDataId(event.getImgId())
            .withImgId(event.getImgId())
            .withParentDataId(event.getImgId())
            .build();
        WfEvent parentDataId = WfEventProduced.builder()
            .withDataId(event.getParentDataId())
            .withImgId(event.getImgId())
            .withParentDataId(event.getImgId())
            .build();

        Node<WfEventNode> imgIdNode = this.treeNode
            .addIfNotExist(new WfEventNode(eventImgId), Arrays.asList(this.treeNode));
        if (!event.getDataId()
            .equals(event.getParentDataId())) {
            Node<WfEventNode> parentNode = this.treeNode
                .addIfNotExist(new WfEventNode(parentDataId), Arrays.asList(this.treeNode, imgIdNode));
            Node<WfEventNode> insertedValue = this.treeNode
                .addIfNotExist(new WfEventNode(parentDataId), Arrays.asList(this.treeNode, imgIdNode, parentNode));
            insertedValue.getValue()
                .setEvent(event);
        } else {
            Node<WfEventNode> insertedValue = this.treeNode
                .addIfNotExist(new WfEventNode(event), Arrays.asList(this.treeNode, imgIdNode));
            insertedValue.getValue()
                .setEvent(event);
        }
    }

    @Override
    public boolean allEventsAreReceivedForAnImage(String imgId) {
        WfEvent eventImgId = WfEventProduced.builder()
            .withDataId(imgId)
            .withImgId(imgId)
            .withParentDataId(imgId)
            .build();
        final Optional<Node<WfEventNode>> imgNode = this.treeNode
            .find((o1, o2) -> o1.compareTo(o2), new WfEventNode(eventImgId));
        if (imgNode.isPresent()) {
            return this.treeNode.atLeastOneNodeIsPresent(
                imgNode.get(),
                (v) -> v.getEvent()
                    .getStep()
                    .getStep()
                    .equals(WfEventStep.CREATED_FROM_STEP_ARCHIVED_IN_HDFS)
                    || v.getEvent()
                        .getStep()
                        .getStep()
                        .equals(WfEventStep.CREATED_FROM_STEP_RECORDED_IN_HBASE));
        }
        return false;
    }

    protected String buildEvtPath(String rootNode, String parentNodeId) {
        StringJoiner retValue = new StringJoiner(".");
        retValue.add(rootNode)
            .add(parentNodeId);
        return retValue.toString();
    }

}
