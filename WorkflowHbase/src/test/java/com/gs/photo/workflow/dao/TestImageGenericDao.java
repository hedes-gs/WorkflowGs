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
	public void test001_shouldRecordInHbaseWithKey1ABCDEFVersion1() {
		HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 1);
		this.hbaseImageThumbnailDAO.put(hbaseData);
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
	}

	protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(short v) {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion(v)
			.withImageName("Mon Image")
			.withPath("Mon path")
			.withThumbnail(new byte[] { 0, 1, 2, 3, 4 })
			.withThumbName("Thumbnail_1.jpg")
			.withHeight(1024)
			.withWidth(768)
			.build();
		return hbaseData;
	}

	@Test
	public void test002_shouldRecordInHbaseWithKey1ABCDEFVersion2() {
		HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) 2);
		this.hbaseImageThumbnailDAO.put(hbaseData);
	}

	@Test
	public void test003_shouldHbaseDataVersion2EqualsToRecordedHbaseData() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 2)
			.build();
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(this.buildVersionHbaseImageThumbnail((short) 2),
			hbaseData);
	}

	@Test
	public void test004_shouldHbaseDataVersion1EqualsToRecordedHbaseData() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 1)
			.build();
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertEquals(this.buildVersionHbaseImageThumbnail((short) 1),
			hbaseData);
	}

	@Test
	public void test005_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndVersion1() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 1)
			.build();
		this.hbaseImageThumbnailDAO.delete(hbaseData);
	}

	@Test
	public void test006_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndVersion1() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 1)
			.build();
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertNull(hbaseData);
	}

	@Test
	public void test005_shouldNotRaiseExceptionWhenDeleteAndKeyIs1andImageIdIsABCDEFAndVersion2() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 2)
			.build();
		this.hbaseImageThumbnailDAO.delete(hbaseData);
	}

	@Test
	public void test006_shouldReturnNullAfterDeleteAndKeyIs1andImageIdIsABCDEFAndVersion2() {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(1)
			.withImageId("ABCDEF")
			.withVersion((short) 2)
			.build();
		hbaseData = this.hbaseImageThumbnailDAO.get(hbaseData);
		Assert.assertNull(hbaseData);
	}

	@Test
	public void test014_shouldRecordBulkOf1000Data() {
		List<HbaseImageThumbnail> data = new ArrayList<>(10000);
		for (int k = 0; k < 10000; k++) {
			HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail((short) k);
			data.add(hbaseData);
		}
		this.hbaseImageThumbnailDAO.put(data);
	}

	@Test
	public void test015_shouldReturn1000DataAfterBulkRecord() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
				.withCreationDate(1)
				.withImageId("ABCDEF")
				.withVersion((short) k)
				.build();
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
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
				.withCreationDate(1)
				.withImageId("ABCDEF")
				.withVersion((short) k)
				.build();
			data[k] = hbaseData;
		}
		this.hbaseImageThumbnailDAO.delete(data);
	}

	@Test
	public void test017_shouldReturn0DataAfterBulkDelete() {
		HbaseImageThumbnail[] data = new HbaseImageThumbnail[1000];
		int nbOfDataFromHbase = 0;
		for (int k = 0; k < data.length; k++) {
			HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
				.withCreationDate(1)
				.withImageId("ABCDEF")
				.withVersion((short) k)
				.build();
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
		HbaseImageThumbnail hbaseData = this.buildVersionHbaseImageThumbnail(
			LocalDateTime.now().toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli(),
			(short) 1);
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

	protected HbaseImageThumbnail buildVersionHbaseImageThumbnail(long creationDate, short v) {
		HbaseImageThumbnail hbaseData = HbaseImageThumbnail.builder()
			.withCreationDate(creationDate)
			.withImageId("ABCDEF")
			.withVersion(v)
			.withImageName("Mon Image")
			.withPath("Mon path")
			.withThumbnail(new byte[] { 0, 1, 2, 3, 4 })
			.withThumbName("Thumbnail_1.jpg")
			.withHeight(1024)
			.withWidth(768)
			.build();
		return hbaseData;
	}

}
