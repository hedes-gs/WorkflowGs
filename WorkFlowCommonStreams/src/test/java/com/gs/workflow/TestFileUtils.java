package com.gs.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gs.photo.workflow.impl.FileUtils;

class TestFileUtils {

    @BeforeAll
    static void setUpBeforeClass() throws Exception {}

    protected FileUtils fileUtils;

    @BeforeEach
    void setUp() throws Exception { this.fileUtils = new FileUtils(); }

    @Test
    void shouldFindFileTestFileUtilsWhenParsingFilesTree() throws IOException {
        try (
            Stream<File> streams = this.fileUtils.toStream(Paths.get("."), "class", "java")) {
            long nbOfFoundFiles = streams.filter(
                (f) -> f.getName()
                    .equalsIgnoreCase("TestFileUtils.java"))
                .count();
            Assert.assertEquals(1, nbOfFoundFiles);

        }
    }

}
