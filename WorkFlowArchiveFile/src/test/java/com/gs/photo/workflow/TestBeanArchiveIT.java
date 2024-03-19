package com.gs.photo.workflow;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

// @RunWith(SpringRunner.class)
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//@SpringBootTest(classes = ApplicationConfig.class)

class TestBeanArchiveIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TestBeanArchiveIT.class);

    @BeforeAll
    static void setUpBeforeClass() throws Exception {}

    @Autowired
    protected FileSystem hdfsFileSystem;

    @BeforeEach
    void setUp() throws Exception {}

    // @Test
    @Ignore
    void test() {
        Path path = new Path("/wf_archive/2020-04-21/test_creation");
        final Path file = new Path(path, "test-file.txt");
        boolean isCreated, isDeleted;
        try {
            isCreated = this.hdfsFileSystem.mkdirs(path);
            Assert.assertTrue(isCreated);
            TestBeanArchiveIT.LOGGER.info("Path {} is created ", path);
            try (
                OutputStream os = this.hdfsFileSystem.create(file);
                DataOutputStream dos = new DataOutputStream(os);) {
                dos.writeUTF("Hello world!!");
            }
            TestBeanArchiveIT.LOGGER.info("file {} is created ", file);

            try (
                InputStream is = this.hdfsFileSystem.open(file);
                DataInputStream dis = new DataInputStream(is);) {
                BufferedReader br = new BufferedReader(new InputStreamReader(dis));
                String line = br.readLine();
                TestBeanArchiveIT.LOGGER.info("file {} is read with content", file, line);
                Assert.assertEquals("Hello world!!", line);
            }

            isDeleted = this.hdfsFileSystem.delete(path, true);
            Assert.assertTrue(isDeleted);
            TestBeanArchiveIT.LOGGER.info("file {} is deleted with content", file);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
