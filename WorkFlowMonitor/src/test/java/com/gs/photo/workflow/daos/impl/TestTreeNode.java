package com.gs.photo.workflow.daos.impl;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.daos.impl.tree.Node;
import com.gs.photo.workflow.daos.impl.tree.Node.HashKeyCompute;

public class TestTreeNode {

    protected static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(256);

    protected static class LocalHashKeyCompute implements HashKeyCompute<String> {

        @Override
        public long compute(String v) {
            long hashCode = CacheNodeDAO.HASH_FUNCTION.newHasher()
                .putString(v, Charset.forName("UTF-8"))
                .hash()
                .asLong();
            return hashCode;
        }
    }

    static protected HashKeyCompute<String> HASH_KEY_COMPUTE = new LocalHashKeyCompute();

    @BeforeAll
    static void setUpBeforeClass() throws Exception {}

    protected Node<String> root;

    @BeforeEach
    void setUp() throws Exception { this.root = Node.getRoot("1", TestTreeNode.HASH_KEY_COMPUTE); }

    @Test
    void test001_Should_returnedEventValueIsEqualTo2_When_theCreatedNodehasTheEventValue2() {
        Node<String> addedNode = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Assert.assertEquals("2", addedNode.getValue());
    }

    @Test
    void test002_Should_returnedEventValueIsEqualTo3_When_theCreatedNodehasTheEventValue3AndLevelIs2() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Assert.assertEquals("3", addedNode_2.getValue());
    }

    @Test
    void test003_Should_returned2_When_Creating2Children() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));
        Assert.assertEquals(
            2,
            this.root.findChildren(addedNode_1)
                .orElse(Collections.emptyList())
                .size());
    }

    @Test
    void test004_Should_returnedValue4And3_When_Creating2Children() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));

        Collection<Node<String>> children = this.root.findChildren(addedNode_1)
            .orElse(Collections.emptyList());
        Assert.assertEquals(
            2,
            children.stream()
                .filter(
                    (v) -> v.getValue()
                        .equals("4")
                        || v.getValue()
                            .equals("3"))
                .count());
    }

    @Test
    void test005_GetAnodeWithValue5_When_FindingAValue5() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_4 = this.root.addIfNotExist("5", Arrays.asList(this.root, addedNode_3));

        this.root.find((o1, o2) -> o1.compareTo(o2), "5")
            .orElseThrow(() -> new IllegalArgumentException())
            .getValue()
            .equals("5");
    }

    @Test
    void test006_GetRightParentPath_When_FindingThePathForValue5() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_4 = this.root.addIfNotExist("5", Arrays.asList(this.root, addedNode_1, addedNode_3));

        Node<String> value = this.root.find((o1, o2) -> o1.compareTo(o2), "5")
            .orElseThrow(() -> new IllegalArgumentException());

        Optional<List<Node<String>>> retValue = this.root.getPath((o1, o2) -> o1.compareTo(o2), "5");
        Assert.assertArrayEquals(
            new String[] { "1", "2", "4" },
            Arrays.stream(
                retValue.orElseThrow(() -> new IllegalArgumentException())
                    .toArray())
                .map((v) -> ((Node) v).getValue())
                .toArray());
    }

    @Test
    void test007_shouldExploreTheWholeTree_When_MatchConditionIsTrue() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_4 = this.root.addIfNotExist("5", Arrays.asList(this.root, addedNode_1, addedNode_3));
        AtomicInteger ai = new AtomicInteger(0);

        this.root.atLeastOneNodeIsPresent(this.root, (v) -> {
            ai.incrementAndGet();
            return true;
        });
        Assert.assertEquals(4, ai.intValue());

    }

    @Test
    void test007_shouldExplorePartOfTheTree_When_MatchConditionIsFalse() {
        Node<String> addedNode_1 = this.root.addIfNotExist("2", Arrays.asList(this.root));
        Node<String> addedNode_2 = this.root.addIfNotExist("3", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_3 = this.root.addIfNotExist("4", Arrays.asList(this.root, addedNode_1));
        Node<String> addedNode_4 = this.root.addIfNotExist("5", Arrays.asList(this.root, addedNode_1, addedNode_3));
        AtomicInteger ai = new AtomicInteger(0);

        this.root.atLeastOneNodeIsPresent(this.root, (v) -> {
            ai.incrementAndGet();
            return !v.equals("3");
        });
        Assert.assertEquals(2, ai.intValue());

    }

}
