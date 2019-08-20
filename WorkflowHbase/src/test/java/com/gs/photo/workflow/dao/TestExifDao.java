package com.gs.photo.workflow.dao;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.workflow.model.HbaseImageThumbnail;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestExifDao {

	protected GenericDAO exifDao;
	protected ImageFilterDAO imageFilterDAO;

	@Before
	public void init() {
		System.setProperty(
			"hadoop.home.dir",
			"D:/cygwin64/usr/local/hbase-1.4.4");
		exifDao = new GenericDAO();
		imageFilterDAO = new ImageFilterDAO();
	}

	@Test
	public void shouldRecordInHbase() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			LocalDateTime.now().toInstant(
				ZoneOffset.ofTotalSeconds(
					0)).toEpochMilli());
		hbaseData.setImageId(
			"ImageId");
		hbaseData.setImageName(
			"Mon Image");
		hbaseData.setPath(
			"Mon path");
		hbaseData.setThumbName(
			"Thumbnail.jpg");
		byte[] b = { 0, 1, 2, 3 };
		hbaseData.setThumbnail(
			b);
		exifDao.put(
			hbaseData,
			HbaseImageThumbnail.class);
	}

	@Test
	public void shouldGetRecordedData() {
		imageFilterDAO.getThumbNailsByDate(
			LocalDateTime.now().minusDays(
				2),
			LocalDateTime.now().plusDays(
				2),
			0,
			0,
			HbaseImageThumbnail.class);
	}
}
