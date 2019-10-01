package com.gs.photo.workflow.dao;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.After;
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

		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setImageName(
			"Mon Image");
		hbaseData.setPath(
			"Mon path");
		hbaseData.setThumbName(
			"Thumbnail.jpg");
		byte[] b = { 0, 1, 2, 3 };
		hbaseData.setThumbnail(
			b);
		hbaseData.setWidth(
			1024);
		hbaseData.setHeight(
			512);
		hbaseImageThumbnailDAO.put(
			hbaseData);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
	}

	@Test
	public void test002_shouldRecordInHbaseWithKey1ABCDEFTrue() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseData.setImageName(
			"Mon Image");
		hbaseData.setPath(
			"Mon path");
		hbaseData.setThumbName(
			"Thumbnail_true.jpg");
		byte[] b = { 0, 1, 2, 3, 4 };
		hbaseData.setThumbnail(
			b);
		hbaseData.setWidth(
			2048);
		hbaseData.setHeight(
			1024);
		hbaseImageThumbnailDAO.put(
			hbaseData);
	}

	@Test
	public void test003_shouldThumbNameEqualsToThumnailTruejpgWhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			"Thumbnail_true.jpg",
			hbaseData.getThumbName());
	}

	@Test
	public void test004_shouldThumbNameEqualsToThumnailjpgWhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			"Thumbnail.jpg",
			hbaseData.getThumbName());
	}

	@Test
	public void test005_shouldWidthEqualsTo1024WhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			1024,
			hbaseData.getWidth());
	}

	@Test
	public void test006_shouldHeightEqualsTo512WhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			512,
			hbaseData.getHeight());
	}

	@Test
	public void test007_shouldHeightEqualsTo1024WhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			1024,
			hbaseData.getHeight());
	}

	@Test
	public void test008_shouldThumbnailEqualsTo01234WhenKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertArrayEquals(
			new byte[] { 0, 1, 2, 3, 4 },
			hbaseData.getThumbnail());
	}

	@Test
	public void test009_shouldOriginalEqualsToTrueWhenKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertEquals(
			false,
			hbaseData.isOrignal());
	}

	@Test
	public void test010_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseImageThumbnailDAO.delete(
			hbaseData);
	}

	@Test
	public void test011_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseImageThumbnailDAO.delete(
			hbaseData);
	}

	@Test
	public void test012_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			true);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertNull(
			hbaseData);
	}

	@Test
	public void test013_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndNotOriginal() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			1);
		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setOrignal(
			false);
		hbaseData = hbaseImageThumbnailDAO.get(
			hbaseData);
		assertNull(
			hbaseData);
	}

	@Test
	public void test014_shouldRecordBulkOf1000Data() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[10000];
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(
				k);
			hbaseData.setImageId(
				"ABCDEF");
			hbaseData.setOrignal(
				true);
			data[k] = hbaseData;
		}
		hbaseImageThumbnailDAO.put(
			data);
	}

	@Test
	public void test015_shouldReturn1000DataAfterBulkRecord() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(
				k);
			hbaseData.setImageId(
				"ABCDEF");
			hbaseData.setOrignal(
				true);
			hbaseData = hbaseImageThumbnailDAO.get(
				hbaseData);
			assertNotNull(
				hbaseData);
			if (hbaseData != null) {
				nbOfDataFromHbase++;
			}
		}
		assertEquals(
			1000,
			nbOfDataFromHbase);

	}

	@Test
	public void test016_shouldDelete1000DataAfterBulkDelete() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(
				k);
			hbaseData.setImageId(
				"ABCDEF");
			hbaseData.setOrignal(
				true);
			data[k] = hbaseData;
		}
		hbaseImageThumbnailDAO.delete(
			data);
	}

	@Test
	public void test017_shouldReturn0DataAfterBulkDelete() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
			hbaseData.setCreationDate(
				k);
			hbaseData.setImageId(
				"ABCDEF");
			hbaseData.setOrignal(
				true);
			hbaseData = hbaseImageThumbnailDAO.get(
				hbaseData);

			if (hbaseData != null) {
				nbOfDataFromHbase++;
			}
			assertNull(
				hbaseData);
		}
		assertEquals(
			0,
			nbOfDataFromHbase);

	}

	@Test
	public void test018_shouldReturn1RecordWhenUsingFilter() {
		HbaseImageThumbnail hbaseData = new HbaseImageThumbnail();
		hbaseData.setCreationDate(
			LocalDateTime.now().toInstant(
				ZoneOffset.ofTotalSeconds(
					0)).toEpochMilli());

		hbaseData.setImageId(
			"ABCDEF");
		hbaseData.setImageName(
			"Mon Image");
		hbaseData.setPath(
			"Mon path");
		hbaseData.setThumbName(
			"Thumbnail.jpg");
		byte[] b = { 0, 1, 2, 3 };
		hbaseData.setThumbnail(
			b);
		hbaseData.setWidth(
			1024);
		hbaseData.setHeight(
			512);
		hbaseImageThumbnailDAO.put(
			hbaseData);

		List<HbaseImageThumbnail> scanValue = hbaseImageThumbnailDAO.getThumbNailsByDate(
			LocalDateTime.now().minusDays(
				2),
			LocalDateTime.now().plusDays(
				2),
			0,
			0);
		assertEquals(
			1,
			scanValue.size());
		hbaseImageThumbnailDAO.delete(
			hbaseData);

	}

	@Test
	public void test019_shouldReturn0RecordWhenTruncatingTable() {

	}
}
