package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.HbaseApplicationConfig;
import com.workflow.model.HbaseExifDataOfImages;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { HbaseApplicationConfig.class, HbaseExifDataOfImagesDAO.class })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HbaseExifDataOfImagesDAOTest {

    @Autowired
    protected HbaseExifDataOfImagesDAO hbaseImageThumbnailDAO;

    @Test
    public void testPutHbaseExifDataOfImages() throws IOException {
        HbaseExifDataOfImages hbaseData = HbaseExifDataOfImages.builder()
            .withCreationDate(
                Instant.now()
                    .toString())
            .withExifPath(new short[] { 1, 2, 3 })
            .withExifTag((short) 32769)
            .withExifValueAsByte(new byte[] { 0, 1, 2, 3 })
            .withExifValueAsInt(new int[] { 0, 1, 2, 3 })
            .withExifValueAsShort(new short[] { 0, 1, 2, 3 })
            .withHeight(1024)
            .withImageId("img id")
            .withThumbName("My thnumbname")
            .withWidth(768)
            .build();
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();
        HbaseExifDataOfImages hbaseData1 = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertTrue(Objects.deepEquals(hbaseData, hbaseData1));
        this.hbaseImageThumbnailDAO.truncate();
    }

    @Test
    public void testDeleteHbaseExifDataOfImages() throws IOException {
        HbaseExifDataOfImages hbaseData = HbaseExifDataOfImages.builder()
            .withCreationDate(
                Instant.now()
                    .toString())
            .withExifPath(new short[] { 1, 2, 3 })
            .withExifTag((short) 32769)
            .withExifValueAsByte(new byte[] { 0, 1, 2, 3 })
            .withExifValueAsInt(new int[] { 0, 1, 2, 3 })
            .withExifValueAsShort(new short[] { 0, 1, 2, 3 })
            .withHeight(1024)
            .withImageId("img id")
            .withThumbName("My thnumbname")
            .withWidth(768)
            .build();
        this.hbaseImageThumbnailDAO.put(hbaseData);
        this.hbaseImageThumbnailDAO.flush();
        HbaseExifDataOfImages hbaseData1 = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertTrue(Objects.deepEquals(hbaseData, hbaseData1));
        this.hbaseImageThumbnailDAO.delete(hbaseData1);
        hbaseData1 = this.hbaseImageThumbnailDAO.get(hbaseData);
        Assert.assertTrue(hbaseData1 == null);

    }

}
