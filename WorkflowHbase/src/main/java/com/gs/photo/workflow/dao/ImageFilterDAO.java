package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

@Component
public class ImageFilterDAO extends AbstractDAO {

	@Autowired
	protected Configuration hbaseConfiguration;
	protected Connection connection;

	@PostConstruct
	protected void init() throws IOException {
		// Now you need to login/authenticate using keytab:
		UserGroupInformation.setConfiguration(
			hbaseConfiguration);
		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
			"wf_hbase@GS.COM",
			"src/test/resources/config/wf_hbase.keytab");
		PrivilegedAction<Connection> action = new PrivilegedAction<Connection>() {

			@Override
			public Connection run() {
				try {
					return ConnectionFactory.createConnection(
						hbaseConfiguration);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		};
		this.connection = ugi.doAs(
			action);
	}

	public <T extends HbaseData> List<T> getThumbNailsByDate(LocalDateTime firstDate, LocalDateTime lastDate,
			long minWidth, long minHeight, Class<T> cl) {
		List<T> retValue = new ArrayList<>();
		String tableName = cl.getAnnotation(
			HbaseTableName.class).value();
		try (Table table = connection.getTable(
			TableName.valueOf(
				tableName))) {
			Scan scan = new Scan();
			scan.addColumn(
				"img".getBytes(),
				"image_name".getBytes());
			scan.addColumn(
				"img".getBytes(),
				"thumb_name".getBytes());
			scan.addColumn(
				"img".getBytes(),
				"path".getBytes());
			scan.addColumn(
				"sz".getBytes(),
				"width".getBytes());
			scan.addColumn(
				"sz".getBytes(),
				"height".getBytes());
			scan.addColumn(
				"thb".getBytes(),
				"thumbnail".getBytes());
			scan.setFilter(
				new FilterRowByLongAtAGivenOffset(
					0,
					firstDate.toInstant(
						ZoneOffset.ofTotalSeconds(
							0)).toEpochMilli(),
					lastDate.toInstant(
						ZoneOffset.ofTotalSeconds(
							0)).toEpochMilli()));
			ResultScanner rs = table.getScanner(
				scan);
			rs.forEach(
				(t) -> {
					try {
						T instance = cl.newInstance();
						retValue.add(
							instance);
						byte[] row = t.getRow();
						HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
							cl);
						hbaseDataInformation.getFieldsData().forEach(
							(hdfi) -> {
								Object v = null;
								if (hdfi.partOfRowKey) {
									v = hdfi.fromByte(
										row,
										hdfi.offset,
										hdfi.fixedWidth);
								} else {
									byte[] value = t.getValue(
										hdfi.columnFamily.getBytes(),
										hdfi.hbaseName.getBytes());
									v = hdfi.fromByte(
										value,
										0,
										value.length);
								}
								try {
									hdfi.field.set(
										instance,
										v);
								} catch (IllegalArgumentException | IllegalAccessException e) {
									e.printStackTrace();
								}
							});
					} catch (InstantiationException | IllegalAccessException e) {
						e.printStackTrace();
					}
				});
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return retValue;

	}

	@Override
	public <T extends HbaseData> void put(T hbaseData, Class<T> cl) {

	}

	@Override
	public <T extends HbaseData> void put(T[] hbaseData, Class<T> cl) {
	}

	@Override
	public <T extends HbaseData> T get(T hbaseData, Class<T> cl) {
		return null;
	}

	@Override
	public <T extends HbaseData> void delete(T hbaseData, Class<T> cl) {
	}

	@Override
	public <T extends HbaseData> void delete(T[] hbaseData, Class<T> cl) {
	}
}
