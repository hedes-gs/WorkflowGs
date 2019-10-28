package com.gs.photo.workflow.dao;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.WorkflowHbaseApplication;
import com.workflow.model.HbaseImageThumbnail;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = WorkflowHbaseApplication.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestImageGenericDao {

	@Autowired
	protected HbaseImageThumbnailDAO hbaseImageThumbnailDAO;

	@Before
	public void init() {
	}

	@After
	public void clean() {
	}

	@Test
	public void test001_shouldRecordInHbase() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();

		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setImageName("Mon Image");
		hbaseData.setPath("Mon path");
		hbaseData.setThumbName("Thumbnail.jpg");
		byte[] b = { 0, 1, 2, 3 };
		hbaseData.setThumbnail(b);
		hbaseData.setWidth(1024);
		hbaseData.setHeight(512);
		this.hbaseImageThumbnailDAO.put(hbaseData);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
	}

	@Test
	public void test002_shouldRecordInHbaseWithKey1ABCDEFTrue() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		hbaseData.setImageName("Mon Image");
		hbaseData.setPath("Mon path");
		hbaseData.setThumbName("Thumbnail_true.jpg");
		byte[] b = { 0, 1, 2, 3, 4 };
		hbaseData.setThumbnail(b);
		hbaseData.setWidth(2048);
		hbaseData.setHeight(1024);
		this.hbaseImageThumbnailDAO.put(hbaseData);
	}

	@Test
	public void test003_shouldThumbNameEqualsToThumnailTruejpgWhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals("Thumbnail_true.jpg",
				hbaseData.getThumbName());
	}

	@Test
	public void test004_shouldThumbNameEqualsToThumnailjpgWhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals("Thumbnail.jpg",
				hbaseData.getThumbName());
	}

	@Test
	public void test005_shouldWidthEqualsTo1024WhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(1024,
				hbaseData.getWidth());
	}

	@Test
	public void test006_shouldHeightEqualsTo512WhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(512,
				hbaseData.getHeight());
	}

	@Test
	public void test007_shouldHeightEqualsTo1024WhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(1024,
				hbaseData.getHeight());
	}

	@Test
	public void test008_shouldThumbnailEqualsTo01234WhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 },
				hbaseData.getThumbnail());
	}

	@Test
	public void test009_shouldOriginalEqualsToTrueWhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(false,
				hbaseData.isOrignal());
	}

	@Test
	public void test010_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		this.hbaseImageThumbnailDAO.delete(hbaseData);
	}

	@Test
	public void test011_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		this.hbaseImageThumbnailDAO.delete(hbaseData);
	}

	@Test
	public void test012_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(true);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertNull(hbaseData);
	}

	@Test
	public void test013_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(1);
		hbaseData.setImageId("ABCDEF");
		hbaseData.setOrignal(false);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertNull(hbaseData);
	}

	@Test
	public void test014_shouldRecordBulkOf1000Data() {
		List<HbaseImageThumbnail> data = new ArrayList<>(10000);
		for (
				int k = 0;
				k < 10000;
				k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(k);
			hbaseData.setImageId("ABCDEF");
			hbaseData.setOrignal(true);
			data.add(hbaseData);
		}
		this.hbaseImageThumbnailDAO.put(data);
	}

	@Test
	public void test015_shouldReturn1000DataAfterBulkRecord() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (
				int k = 0;
				k < data.length;
				k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(k);
			hbaseData.setImageId("ABCDEF");
			hbaseData.setOrignal(true);
			hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
			Assert.assertNotNull(hbaseData);
			if (hbaseData != null) {
				nbOfDataFromHbase++;
			}
		}
		Assert.assertEquals(1000,
				nbOfDataFromHbase);

	}

	@Test
	public void test016_shouldDelete1000DataAfterBulkDelete() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		for (
				int k = 0;
				k < data.length;
				k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(k);
			hbaseData.setImageId("ABCDEF");
			hbaseData.setOrignal(true);
			data[k] = hbaseData;
		}
		this.hbaseImageThumbnailDAO.delete(data);
	}

	@Test
	public void test017_shouldReturn0DataAfterBulkDelete() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (
				int k = 0;
				k < data.length;
				k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(k);
			hbaseData.setImageId("ABCDEF");
			hbaseData.setOrignal(true);
			hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);

			if (hbaseData != null) {
				nbOfDataFromHbase++;
			}
			Assert.assertNull(hbaseData);
		}
		Assert.assertEquals(0,
				nbOfDataFromHbase);

	}

	@Test
	public void test018_shouldReturn1RecordWhenUsingFilter() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(LocalDateTime.now().toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli());

		hbaseData.setImageId("ABCDEF");
		hbaseData.setImageName("Mon Image");
		hbaseData.setPath("Mon path");
		hbaseData.setThumbName("Thumbnail.jpg");
		byte[] b = { 0, 1, 2, 3 };
		hbaseData.setThumbnail(b);
		hbaseData.setWidth(1024);
		hbaseData.setHeight(512);
		this.hbaseImageThumbnailDAO.put(hbaseData);

		List<HbaseImageThumbnail> scanValue = this.hbaseImageThumbnailDAO.getThumbNailsByDate(
				LocalDateTime.now().minusDays(2),
				LocalDateTime.now().plusDays(2),
				0,
				0);
		Assert.assertEquals(1,
				scanValue.size());
		this.hbaseImageThumbnailDAO.delete(hbaseData);

	}

	@Test
	public void test019_shouldReturn0RecordWhenTruncatingTable() {

	}
}
