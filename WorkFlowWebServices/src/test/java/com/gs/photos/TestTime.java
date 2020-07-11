package com.gs.photos;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gs.photo.workflow.DateTimeHelper;

class TestTime {

    @BeforeAll
    static void setUpBeforeClass() throws Exception {}

    @BeforeEach
    void setUp() throws Exception {}

    @Test
    void test() {
        final long firstLong = System.currentTimeMillis() - (1000L * 3600L * 24L * 30L * 3L);
        System.out.println(".... " + DateTimeHelper.toDateTimeAsString(firstLong));
    }

}
