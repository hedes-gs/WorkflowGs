package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.workflow.recinhbase.dao.IHbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;

// @SpringBootTest(classes = { HbaseStatsDAO.class, HbaseApplicationConfig.class })
public class TestHbaseStatsDAO {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TestHbaseStatsDAO.class);

    @Autowired
    protected IHbaseStatsDAO      hbaseStatsDAO;

    // @Test
    public void test001() throws IOException {

        this.hbaseStatsDAO.truncate();
        OffsetDateTime ldt = DateTimeHelper.toOffsetDateTime("2020:05:10 09:01:02");

        String keyYear = "Y:" + (long) ldt.getYear();
        String keyMonth = keyYear + "/M:" + (long) ldt.getMonthValue();
        String keyDay = keyMonth + "/D:" + (long) ldt.getDayOfMonth();
        String keyHour = keyDay + "/H:" + (long) ldt.getHour();
        String keyMinute = keyHour + "/Mn:" + (long) ldt.getMinute();
        String keySeconde = keyMinute + "/S:" + (long) ldt.getSecond();
        List<String> retValue = Arrays.asList(keyYear, keyMonth, keyDay, keyHour, keyMinute, keySeconde);
        List<HbaseImageThumbnailKey> imgs = Arrays.asList(
            HbaseImageThumbnailKey.builder()
                .withCreationDate(1L)
                .withVersion((short) 1)
                .withImageId("1")
                .build(),
            HbaseImageThumbnailKey.builder()
                .withCreationDate(2L)
                .withVersion((short) 2)
                .withImageId("2")
                .build(),
            HbaseImageThumbnailKey.builder()
                .withCreationDate(3L)
                .withVersion((short) 3)
                .withImageId("3")
                .build());
        imgs.forEach((imageId) -> retValue.forEach((k) -> {

        }));
        this.hbaseStatsDAO.flush();
        retValue.forEach((k) -> {
            try {
                Assertions.assertEquals(3, this.hbaseStatsDAO.countImages(k));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    public void test002() throws IOException {

        this.hbaseStatsDAO.truncate();
        OffsetDateTime ldt = DateTimeHelper.toOffsetDateTime("2020:05:10 09:01:02");

        String keyYear = "Y:" + (long) ldt.getYear();
        String keyMonth = keyYear + "/M:" + (long) ldt.getMonthValue();
        String keyDay = keyMonth + "/D:" + (long) ldt.getDayOfMonth();
        String keyHour = keyDay + "/H:" + (long) ldt.getHour();
        String keyMinute = keyHour + "/Mn:" + (long) ldt.getMinute();
        String keySeconde = keyMinute + "/S:" + (long) ldt.getSecond();
        List<String> retValue = Arrays.asList(keyYear, keyMonth, keyDay, keyHour, keyMinute, keySeconde);
        List<HbaseImageThumbnailKey> imgs = Arrays.asList(
            HbaseImageThumbnailKey.builder()
                .withCreationDate(1L)
                .withVersion((short) 1)
                .withImageId("1")
                .build(),
            HbaseImageThumbnailKey.builder()
                .withCreationDate(2L)
                .withVersion((short) 2)
                .withImageId("2")
                .build(),
            HbaseImageThumbnailKey.builder()
                .withCreationDate(3L)
                .withVersion((short) 3)
                .withImageId("3")
                .build());
        imgs.forEach((imageId) -> retValue.forEach((k) -> {

        }));
        this.hbaseStatsDAO.flush();

        retValue.forEach((k) -> {
            try {
                TestHbaseStatsDAO.LOGGER.info("For key {} found {}", k, this.hbaseStatsDAO.getImages(k, 5));
                Assertions.assertEquals(imgs, this.hbaseStatsDAO.getImages(k, 5));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

}
