package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseImageThumbnailKey;

public abstract class AbstractHbaseStatsDAO<T extends HbaseImageThumbnailKey> extends GenericDAO<T> {

    protected static final String TABLE_FAMILY_FSTATS          = "fstats";
    protected static final String TABLE_FAMILY_IMGS            = "imgs";
    protected static final byte[] TABLE_FAMILY_IMGS_AS_BYTES   = AbstractHbaseStatsDAO.TABLE_FAMILY_IMGS
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[] TABLE_FAMILY_FSTATS_AS_BYTES = AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS
        .getBytes(Charset.forName("UTF-8"));
    protected static final String COLUMN_STAT_NAME             = "stats";
    protected static final byte[] COLUMN_STAT_AS_BYTES         = AbstractHbaseStatsDAO.COLUMN_STAT_NAME
        .getBytes(Charset.forName("UTF-8"));
    private static final int      KEY_MONTH_LENGTH             = 5;
    private static final int      KEY_DAY_LENGTH               = 5;
    private static final int      KEY_HOUR_LENGTH              = 5;
    private static final int      KEY_MN_LENGTH                = 6;
    private static final int      KEY_SEC_LENGTH               = 5;
    private static final int      KEY_YEAR_LENGTH              = 6;

    protected static final Logger LOGGER                       = LoggerFactory.getLogger(AbstractHbaseStatsDAO.class);

    public static enum KeyEnumType {
        ALL(
            "all"
        ), YEAR(
            "year"
        ), MONTH(
            "month"
        ), DAY(
            "day"
        ), HOUR(
            "hour"
        ), MINUTE(
            "minute"
        ), SECOND(
            "second"
        );

        String name;

        public String getName() { return this.name; }

        KeyEnumType(String name) { this.name = name; }

    };

    protected static final String                   KEY_YEAR_PREFIX   = "Y:";
    protected static final String                   KEY_MONTH_PREFIX  = AbstractHbaseStatsDAO.KEY_YEAR_PREFIX + "/M:";
    protected static final String                   KEY_DAY_PREFIX    = AbstractHbaseStatsDAO.KEY_MONTH_PREFIX + "/D:";
    protected static final String                   KEY_HOUR_PREFIX   = AbstractHbaseStatsDAO.KEY_DAY_PREFIX + "/H:";
    protected static final String                   KEY_MINUTE_PREFIX = AbstractHbaseStatsDAO.KEY_HOUR_PREFIX + "/Mn:";
    protected static final String                   KEY_SECOND_PREFIX = AbstractHbaseStatsDAO.KEY_MINUTE_PREFIX + "/S:";

    protected static final String                   YEAR_REGEXP       = "Y\\:([0-9]{4,4})[ ]+";
    protected static final String                   MONTH_REGEXP      = "Y\\:([0-9]{4,4})\\/M\\:([0-9]{2,2})[ ]+";
    protected static final String                   DAY_REGEXP        = "Y\\:([0-9]{4,4})\\/M\\:([0-9]{2,2})\\/D\\:([0-9]{2,2})[ ]+";
    protected static final String                   HOUR_REGEXP       = "Y\\:([0-9]{4,4})\\/M\\:([0-9]{2,2})\\/D\\:([0-9]{2,2})\\/H\\:([0-9]{2,2})[ ]+";
    protected static final String                   MINUTE_REGEXP     = "Y\\:([0-9]{4,4})\\/M\\:([0-9]{2,2})\\/D\\:([0-9]{2,2})\\/H\\:([0-9]{2,2})\\/Mn\\:([0-9]{2,2})[ ]+";
    protected static final String                   SECOND_REGEXP     = "Y\\:([0-9]{4,4})\\/M\\:([0-9]{2,2})\\/D\\:([0-9]{2,2})\\/H\\:([0-9]{2,2})\\/Mn\\:([0-9]{2,2})\\/S\\:([0-9]{2,2})";

    @SuppressWarnings(value = { "unchecked" })
    protected static final Map<KeyEnumType, String> TO_REGEXP         = new HashMap() {
                                                                          {
                                                                              this.put(
                                                                                  KeyEnumType.YEAR,
                                                                                  AbstractHbaseStatsDAO.YEAR_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.MONTH,
                                                                                  AbstractHbaseStatsDAO.MONTH_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.DAY,
                                                                                  AbstractHbaseStatsDAO.DAY_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.HOUR,
                                                                                  AbstractHbaseStatsDAO.HOUR_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.MINUTE,
                                                                                  AbstractHbaseStatsDAO.MINUTE_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.SECOND,
                                                                                  AbstractHbaseStatsDAO.SECOND_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.ALL,
                                                                                  AbstractHbaseStatsDAO.SECOND_REGEXP);

                                                                          }
                                                                      };

    public void truncate() throws IOException {
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        AbstractHbaseStatsDAO.LOGGER.warn("Truncate table {}", tableName);
        Admin admin = this.connection.getAdmin();
        admin.disableTable(tableName);
        admin.truncateTable(tableName, false);
        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
    }

    public long countImages(String key) throws IOException {
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        AbstractHbaseStatsDAO.LOGGER.info("countImages images of key {} ", key);
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().withStartRow(AbstractDAO.toBytes(key))
                .addFamily(AbstractDAO.FAMILY_INFOS_NAME_AS_BYTES); // we need only page visit stats, not a user info
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    // request next portion of data
                    for (Cell statCell : res.listCells()) {
                        // each returned cell contains web page as column qualifier and page visit count
                        // as value
                        // var webPage = Bytes.toString(CellUtil.cloneQualifier(statCell));
                        return Bytes.toLong(CellUtil.cloneValue(statCell));
                    }
                }
            }
        }
        return 0;
    }

    public Map<String, Long> getAll() throws IOException {
        AbstractHbaseStatsDAO.LOGGER.info("get images of key ");
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        Map<String, Long> retValue = new HashMap<>();
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().addFamily(AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES);
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    retValue.put(
                        new String(res.getRow()),
                        Bytes.toLong(
                            res.getValue(
                                AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES,
                                AbstractHbaseStatsDAO.COLUMN_STAT_AS_BYTES)));
                }
            }
        }
        return retValue;
    }

    public List<T> getImages(String key, int maxSize) throws IOException {
        AbstractHbaseStatsDAO.LOGGER.info("get images of key {}, maxSize {}", key, maxSize);
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        List<T> retValue = new ArrayList<>();
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().withStartRow(AbstractDAO.toBytes(key))
                .withStopRow(AbstractDAO.toBytes(key), true)
                .addFamily(AbstractDAO.FAMILY_IMGS_NAME_AS_BYTES)
                .setReversed(true);
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    for (Cell statCell : res.listCells()) {
                        T hbaseData = this.getHbaseDataInformation()
                            .newInstance();
                        retValue.add(
                            this.getHbaseDataInformation()
                                .buildOnlyKey(hbaseData, CellUtil.cloneQualifier(statCell)));
                    }

                    if (retValue.size() == maxSize) {
                        break;
                    }
                }
            } catch (
                InstantiationException |
                IllegalAccessException |
                IllegalArgumentException |
                InvocationTargetException |
                NoSuchMethodException |
                SecurityException e) {
                AbstractHbaseStatsDAO.LOGGER.warn("Error {} ", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return retValue;
    }

    @Override
    protected void createTablesIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
            TableName tn = AbstractDAO.createTableIfNeeded(
                admin,
                hdi.getTableName(),
                Arrays.asList(AbstractHbaseStatsDAO.TABLE_FAMILY_IMGS, AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS));
            hdi.setTable(tn);
        }
    }

    public static int getKeyLength(KeyEnumType keyType) {
        switch (keyType) {
            case SECOND:
            case ALL:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH + AbstractHbaseStatsDAO.KEY_MONTH_LENGTH
                    + AbstractHbaseStatsDAO.KEY_DAY_LENGTH + AbstractHbaseStatsDAO.KEY_HOUR_LENGTH
                    + AbstractHbaseStatsDAO.KEY_MN_LENGTH + AbstractHbaseStatsDAO.KEY_SEC_LENGTH;
            case MINUTE:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH + AbstractHbaseStatsDAO.KEY_MONTH_LENGTH
                    + AbstractHbaseStatsDAO.KEY_DAY_LENGTH + AbstractHbaseStatsDAO.KEY_HOUR_LENGTH
                    + AbstractHbaseStatsDAO.KEY_MN_LENGTH;
            case HOUR:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH + AbstractHbaseStatsDAO.KEY_MONTH_LENGTH
                    + AbstractHbaseStatsDAO.KEY_DAY_LENGTH + AbstractHbaseStatsDAO.KEY_HOUR_LENGTH;
            case DAY:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH + AbstractHbaseStatsDAO.KEY_MONTH_LENGTH
                    + AbstractHbaseStatsDAO.KEY_DAY_LENGTH;
            case MONTH:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH + AbstractHbaseStatsDAO.KEY_MONTH_LENGTH;
            case YEAR:
                return AbstractHbaseStatsDAO.KEY_YEAR_LENGTH;
            default: {
                throw new IllegalArgumentException(keyType + " not supported");
            }
        }
    }

    public static Map<KeyEnumType, String> toKey(OffsetDateTime ldt, KeyEnumType... types) {
        int maxLength = AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL);
        Map<KeyEnumType, String> retValue = new HashMap<>();
        String keyYear = String.format("Y:%4d", ldt.getYear());
        String keyMonth = String.format("%s/M:%02d", keyYear, ldt.getMonthValue());
        String keyDay = String.format("%s/D:%02d", keyMonth, ldt.getDayOfMonth());
        String keyHour = String.format("%s/H:%02d", keyDay, ldt.getHour());
        String keyMinute = String.format("%s/Mn:%02d", keyHour, ldt.getMinute());
        String keySeconde = String.format("%s/S:%02d", keyMinute, ldt.getSecond());

        for (KeyEnumType type : types) {
            switch (type) {
                case SECOND: {
                    retValue.put(type, keySeconde);
                    break;
                }
                case MINUTE: {
                    retValue.put(type, StringUtils.rightPad(keyMinute, maxLength));
                    break;
                }
                case HOUR: {
                    retValue.put(type, StringUtils.rightPad(keyHour, maxLength));
                    break;
                }
                case DAY: {
                    retValue.put(type, StringUtils.rightPad(keyDay, maxLength));
                    break;
                }
                case MONTH: {
                    retValue.put(type, StringUtils.rightPad(keyMonth, maxLength));
                    break;
                }
                case YEAR: {
                    retValue.put(type, StringUtils.rightPad(keyYear, maxLength));
                    break;
                }
                case ALL: {
                    retValue.put(KeyEnumType.YEAR, StringUtils.rightPad(keyYear, maxLength));
                    retValue.put(KeyEnumType.MONTH, StringUtils.rightPad(keyMonth, maxLength));
                    retValue.put(KeyEnumType.DAY, StringUtils.rightPad(keyDay, maxLength));
                    retValue.put(KeyEnumType.HOUR, StringUtils.rightPad(keyHour, maxLength));
                    retValue.put(KeyEnumType.SECOND, StringUtils.rightPad(keySeconde, maxLength));
                    retValue.put(KeyEnumType.MINUTE, StringUtils.rightPad(keyMinute, maxLength));
                    retValue.put(KeyEnumType.ALL, StringUtils.rightPad(keySeconde, maxLength));
                    break;
                }
            }
        }
        return retValue;
    }

    public static final String getRegexForIntervall(KeyEnumType type) {
        return AbstractHbaseStatsDAO.TO_REGEXP.get(type);
    }

    public AbstractHbaseStatsDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
