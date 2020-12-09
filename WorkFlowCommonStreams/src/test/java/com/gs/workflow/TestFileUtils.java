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
import com.workflow.model.files.FileToProcess;

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

    void shouldAccessNas() throws IOException {
        this.fileUtils.toStream("192.168.1.101:/nfs/Public", "/", ".arw")
            .forEach((s) -> System.out.println("Found " + s.getAbsolutePath() + " " + s.getName()));
    }

    void testCopy() throws IOException {
        this.fileUtils.copyRemoteToLocal(
            "192.168.1.101:/nfs/Public",
            "/Shared Pictures/à trier/2015/2015-03-08/DSC03637.ARW",
            Paths.get(new File("test.arw").getAbsolutePath()),
            4 * 1024 * 1024);
    }

    @Test
    void testReadfirstByte() throws IOException {
        FileToProcess ftp = FileToProcess.builder()
            .withHost("192.168.1.101")
            .withRootForNfs("/nfs/Public")
            .withPath("/Shared Pictures/à trier/2015/2015-03-08/DSC03637.ARW")
            .build();
        this.fileUtils.readFirstBytesOfFile(ftp);
    }

}
