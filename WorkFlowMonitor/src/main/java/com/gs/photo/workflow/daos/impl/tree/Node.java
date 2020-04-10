package com.gs.photo.workflow.daos.impl.tree;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import javax.annotation.Generated;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.collect.TreeMultimap;

public class Node<T extends Comparable<T>> implements Comparable<Node<T>> {

    public static interface HashKeyCompute<T> { public long compute(T v); }

    private static final ReentrantReadWriteLock         rwl       = new ReentrantReadWriteLock();
    private static final Lock                           readLock  = Node.rwl.readLock();
    private static final Lock                           writeLock = Node.rwl.writeLock();

    protected Node<T>                                   root;
    protected T                                         value;
    protected long                                      key;
    protected Multimap<Long, Node<T>>                   position;
    protected Table<Long, Node<T>, List<Node<T>>>       parentPath;
    protected Table<Long, Node<T>, Collection<Node<T>>> children;
    protected HashKeyCompute<T>                         hashKeyCompute;

    @Generated("SparkTools")
    private Node(Builder<T> builder) {
        this.value = builder.value;
        this.key = builder.key;
        this.position = builder.position;
        this.parentPath = builder.parentPath;
        this.root = builder.root;
    }

    private Node(T v) { this.value = v; }

    public static <T extends Comparable<T>> Node<T> getRoot(T v, HashKeyCompute<T> hashKeyCompute) {
        Node<T> root = new Node<>(v);
        root.position = TreeMultimap.create();
        root.parentPath = TreeBasedTable.create();
        root.children = TreeBasedTable.create();
        root.hashKeyCompute = hashKeyCompute;
        return root;
    }

    public Optional<Collection<Node<T>>> findChildren(Node<T> node) {
        return Optional.ofNullable(this.children.get(node.key, node));
    }

    public boolean atLeastOneNodeIsPresent(Node<T> node, Predicate<T> predicate) {
        Node.readLock.lock();
        try {
            return this._doAtLeastOneNodeIsPresent(node, predicate);
        } finally {
            Node.readLock.unlock();
        }
    }

    protected boolean _doAtLeastOneNodeIsPresent(Node<T> node, Predicate<T> predicate) {
        Optional<Collection<Node<T>>> children = this.findChildren(node);
        List<Node<T>> EMPTY_LIST = Collections.emptyList();
        boolean atLeastOneNodeIsPresent = children.orElse(EMPTY_LIST)
            .stream()
            .allMatch((n) -> predicate.test(n.value) && this._doAtLeastOneNodeIsPresent(n, predicate));
        return atLeastOneNodeIsPresent || !children.isPresent();
    }

    public Node<T> addIfNotExist(T v, List<Node<T>> path) {
        Node<T> retValue = null;
        Node.writeLock.lock();
        try {
            final Node<T> parent = path.get(path.size() - 1);
            final Builder<T> builder = Node.builder();
            Node<T> n = builder.withValue(v)
                .withKey(this.hashKeyCompute.compute(v))
                .withRoot(this)
                .build();
            boolean inserted = this.position.put(n.key, n);
            if (inserted) {
                this.parentPath.put(n.key, n, path);
                Collection<Node<T>> childrenOfCurrentParent = this.children.get(parent.key, parent);
                if (childrenOfCurrentParent == null) {
                    childrenOfCurrentParent = new TreeSet<>();
                    this.children.put(parent.key, parent, childrenOfCurrentParent);
                }
                childrenOfCurrentParent.add(n);
                retValue = n;
            } else {
                retValue = this.position.get(n.key)
                    .stream()
                    .filter((currNode) -> currNode.equals(n))
                    .findFirst()
                    .get();
            }
        } finally {
            Node.writeLock.unlock();
        }
        return retValue;
    }

    public void filter() {

    }

    public Optional<Node<T>> find(Comparator<T> cmp, T value) {
        Node.readLock.lock();
        try {
            Optional<Node<T>> currentNode = this.position.get(this.hashKeyCompute.compute(value))
                .stream()
                .filter((n) -> cmp.compare(n.value, value) == 0)
                .findFirst();
            return currentNode;
        } finally {
            Node.readLock.unlock();
        }
    }

    public Optional<List<Node<T>>> getPath(Comparator<T> cmp, T value) {
        long key = this.hashKeyCompute.compute(value);
        Node.readLock.lock();
        try {
            Optional<Node<T>> node = this.find(cmp, value);
            Optional<List<Node<T>>> path = node.map((n) -> this.parentPath.get(key, n));
            return path;
        } finally {
            Node.readLock.unlock();
        }
    }

    public T getValue() { return this.value; }

    @Override
    public int hashCode() { return (int) this.key; }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        Node<T> other = (Node<T>) obj;
        if (this.value == null) {
            if (other.value != null) { return false; }
        } else if (!this.value.equals(other.value)) { return false; }
        return true;
    }

    /**
     * Creates builder to build {@link Node}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static <V extends Comparable<V>> Builder<V> builder() { return new Builder<>(); }

    /**
     * Builder to build {@link Node}.
     */
    @Generated("SparkTools")
    public static final class Builder<U extends Comparable<U>> {
        private Node<U>                             root;
        private U                                   value;
        private long                                key;
        private Multimap<Long, Node<U>>             position;
        private Table<Long, Node<U>, List<Node<U>>> parentPath;

        private Builder() {}

        public Builder<U> withRoot(Node<U> root) {
            this.root = root;
            return this;
        }

        /**
         * Builder method for value parameter.
         *
         * @param value
         *            field to set
         * @return builder
         */
        public Builder<U> withValue(U value) {
            this.value = value;
            return this;
        }

        /**
         * Builder method for key parameter.
         *
         * @param key
         *            field to set
         * @return builder
         */
        public Builder<U> withKey(long key) {
            this.key = key;
            return this;
        }

        /**
         * Builder method for position parameter.
         *
         * @param position
         *            field to set
         * @return builder
         */
        public Builder<U> withPosition(Multimap<Long, Node<U>> position) {
            this.position = position;
            return this;
        }

        /**
         * Builder method for parentPath parameter.
         *
         * @param parentPath
         *            field to set
         * @return builder
         */
        public Builder<U> withParentPath(Table<Long, Node<U>, List<Node<U>>> parentPath) {
            this.parentPath = parentPath;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public Node<U> build() { return new Node<>(this); }
    }

    @Override
    public int compareTo(Node<T> o) { return this.value.compareTo(o.value); }

}
