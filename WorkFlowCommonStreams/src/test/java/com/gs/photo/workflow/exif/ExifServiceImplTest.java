package com.gs.photo.workflow.exif;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExifServiceImplTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    ExifServiceImpl exifServiceImpl;

    @Before
    public void setUp() throws Exception {
        this.exifServiceImpl = new ExifServiceImpl(Arrays.asList("sony-exif.csv", "standard-exif.csv"));
        this.exifServiceImpl.init();
    }

    @Test
    public void test() {
        Assert.assertTrue(this.exifServiceImpl != null);
        // this.exifServiceImpl
        // .getExifDTOFrom(ExifDTO.builder(), (short) 0, (short) 0xA002, new int[] { 1,
        // 2 }, null, null);

    }

}
