package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImageThumbnailDAO extends GenericDAO<HbaseImageThumbnail> {

	private static final byte[] FAMILY_THB_BYTES = "thb".getBytes();
	private static final byte[] FAMILY_SZ_BYTES = "sz".getBytes();
	private static final byte[] FAMILY_IMG_BYTES = "img".getBytes();

	private static final byte[] HEIGHT_BYTES = "height".getBytes();
	private static final byte[] WIDTH_BYTES = "width".getBytes();
	private static final byte[] PATH_BYTES = "path".getBytes();
	private static final byte[] TUMB_NAME_BYTES = "thumb_name".getBytes();
	private static final byte[] IMAGE_NAME_BYTES = "image_name".getBytes();
	private static final byte[] TUMBNAIL_BYTES = "thumbnail".getBytes();

	@Value("${hbase.table.prefix}")
	protected String tablePrefix;

	protected HbaseDataInformation<HbaseImageThumbnail> hbaseDataInformation;
	protected TableName tableName;

	@PostConstruct
	protected void init() throws IOException {
		this.hbaseDataInformation = new HbaseDataInformation<>(HbaseImageThumbnail.class, tablePrefix);
		buildHbaseDataInformation(
			HbaseImageThumbnail.class,
			hbaseDataInformation);
		tableName = createTableIfNeeded(
			hbaseDataInformation);
		hbaseDataInformation.setTable(
			tableName);
	}

	public void put(HbaseImageThumbnail[] hbaseData) {
		super.put(
			hbaseData,
			hbaseDataInformation);
	}

	public void put(HbaseImageThumbnail hbaseData) {
		super.put(
			hbaseData,
			hbaseDataInformation);
	}

	public void delete(HbaseImageThumbnail[] hbaseData) {
		super.delete(
			hbaseData,
			hbaseDataInformation);
	}

	public void delete(HbaseImageThumbnail hbaseData) {
		super.delete(
			hbaseData,
			hbaseDataInformation);
	}

	public HbaseImageThumbnail get(HbaseImageThumbnail hbaseData) {
		return super.get(
			hbaseData,
			hbaseDataInformation);
	}

	public List<HbaseImageThumbnail> getThumbNailsByDate(LocalDateTime firstDate, LocalDateTime lastDate, long minWidth,
			long minHeight) {
		List<HbaseImageThumbnail> retValue = new ArrayList<>();
		final long firstDateEpochMillis = firstDate.toInstant(
			ZoneOffset.ofTotalSeconds(
				0)).toEpochMilli();
		final long mastDateEpochMilli = lastDate.toInstant(
			ZoneOffset.ofTotalSeconds(
				0)).toEpochMilli();
		try (Table table = connection.getTable(
			hbaseDataInformation.getTable())) {
			Scan scan = new Scan();
			scan.addColumn(
				FAMILY_IMG_BYTES,
				IMAGE_NAME_BYTES);
			scan.addColumn(
				FAMILY_IMG_BYTES,
				TUMB_NAME_BYTES);
			scan.addColumn(
				FAMILY_IMG_BYTES,
				PATH_BYTES);
			scan.addColumn(
				FAMILY_SZ_BYTES,
				WIDTH_BYTES);
			scan.addColumn(
				FAMILY_SZ_BYTES,
				HEIGHT_BYTES);
			scan.addColumn(
				FAMILY_THB_BYTES,
				TUMBNAIL_BYTES);

			scan.setFilter(
				new FilterRowByLongAtAGivenOffset(0, firstDateEpochMillis, mastDateEpochMilli));
			ResultScanner rs = table.getScanner(
				scan);
			rs.forEach(
				(t) -> {

					HbaseImageThumbnail instance = new HbaseImageThumbnail();
					retValue.add(
						instance);
					hbaseDataInformation.build(
						instance,
						t);
				});
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return retValue;

	}

}
