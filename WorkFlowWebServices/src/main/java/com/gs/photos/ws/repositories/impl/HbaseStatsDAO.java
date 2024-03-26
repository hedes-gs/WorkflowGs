package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.common.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.dtos.MinMaxDatesDto;

@Component
public class HbaseStatsDAO extends AbstractHbaseStatsDAO<HbaseImageThumbnailKey> implements IHbaseStatsDAO {
    protected static String       REG_EXP = "Y:([0-9]+)\\/M\\:([0-9]+)[ ]*\\/D\\:([0-9]+)[ ]*\\/H\\:([0-9]+)[ ]*\\/Mn\\:([0-9]+)[ ]*\\/S\\:([0-9]+)[ ]*";
    protected static final Logger LOGGER  = LoggerFactory.getLogger(HbaseStatsDAO.class);

    @Override
    public MinMaxDatesDto getMinMaxDates() {

        try {
            RecursiveTask<OffsetDateTime> min = new RecursiveTask<OffsetDateTime>() {
                @Override
                protected OffsetDateTime compute() { return HbaseStatsDAO.this.getMin(); }
            };
            RecursiveTask<OffsetDateTime> max = new RecursiveTask<OffsetDateTime>() {
                @Override
                protected OffsetDateTime compute() { return HbaseStatsDAO.this.getMax(); }
            };

            ForkJoinTask.invokeAll(min, max);
            return MinMaxDatesDto.builder()
                .withMaxDate(max.get())
                .withMinDate(min.get())
                .build();
        } catch (
            InterruptedException |
            ExecutionException e) {
            HbaseStatsDAO.LOGGER.warn(" Erreur, ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<MinMaxDatesDto> getDatesBetween(
        OffsetDateTime startTime,
        OffsetDateTime stopTime,
        KeyEnumType intervallType
    ) throws IOException {
        List<MinMaxDatesDto> listOfMinMaxDatesDto = new ArrayList<>();
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        Filter f;
        byte[] startKey = null;
        byte[] stopKey = null;
        startKey = AbstractHbaseStatsDAO.toKey(startTime, intervallType)
            .get(intervallType)
            .getBytes(Charset.forName("UTF-8"));
        stopKey = AbstractHbaseStatsDAO.toKey(stopTime, intervallType)
            .get(intervallType)
            .getBytes(Charset.forName("UTF-8"));
        f = new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(intervallType),
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL));
        Pattern intervallTypePattern = Pattern.compile(AbstractHbaseStatsDAO.getRegexForIntervall(intervallType));
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().withStartRow(startKey)
                .withStopRow(stopKey, true);
            statsScan.setFilter(f);
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    byte[] row = res.getRow();
                    String key = new String(row, Charset.forName("UTF-8"));
                    OffsetDateTime retValue = this.buildOffsetDateTimeFromKey(intervallTypePattern, key);
                    OffsetDateTime endDate;
                    OffsetDateTime minValue;
                    int countNumber = 0;

                    switch (intervallType) {
                        case YEAR:
                            minValue = retValue.with(TemporalAdjusters.firstDayOfYear())
                                .withHour(00)
                                .withMinute(00)
                                .withSecond(0);
                            endDate = retValue.with(TemporalAdjusters.lastDayOfYear())
                                .withHour(23)
                                .withMinute(59)
                                .withSecond(59);
                            break;
                        case MONTH:
                            minValue = retValue.with(TemporalAdjusters.firstDayOfMonth())
                                .withHour(00)
                                .withMinute(00)
                                .withSecond(0);
                            endDate = retValue.with(TemporalAdjusters.lastDayOfMonth())
                                .withHour(23)
                                .withMinute(59)
                                .withSecond(59);
                            break;
                        case DAY:
                            minValue = retValue.withHour(00)
                                .withMinute(00)
                                .withSecond(00);
                            endDate = retValue.withHour(23)
                                .withMinute(59)
                                .withSecond(59);
                            break;
                        case HOUR:
                            minValue = retValue.withMinute(00)
                                .withSecond(00);
                            endDate = retValue.withMinute(59)
                                .withSecond(59);
                            break;
                        case MINUTE:
                            minValue = retValue.withSecond(00);
                            endDate = retValue.withSecond(59);
                            break;
                        case SECOND:
                            minValue = retValue;
                            endDate = retValue;
                            break;
                        default:
                            throw new IllegalArgumentException("unmanaged option " + intervallType);
                    }

                    listOfMinMaxDatesDto.add(
                        MinMaxDatesDto.builder()
                            .withCountNumber((int) this.countImages(minValue, endDate, intervallType))
                            .withIntervallType(intervallType.getName())
                            .withMinDate(minValue)
                            .withMaxDate(endDate)
                            .build());
                }
            }
        }
        return listOfMinMaxDatesDto;
    }

    @Override
    public long countImages(OffsetDateTime min, OffsetDateTime max, KeyEnumType type) throws IOException {
        final String minAsString = AbstractHbaseStatsDAO.toKey(min, type)
            .get(type);
        final String maxAsString = AbstractHbaseStatsDAO.toKey(max, type)
            .get(type);

        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        HbaseStatsDAO.LOGGER.info("Start of countImages between min {} and max {} - type is {} ", min, max, type);
        long retValue = 0;
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().withStartRow(AbstractDAO.toBytes(minAsString))
                .withStopRow(AbstractDAO.toBytes(maxAsString), true)
                .addFamily(AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES)
                .setFilter(
                    new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(type),
                            Pattern.CASE_INSENSITIVE)));
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                retValue = StreamSupport.stream(scanner.spliterator(), false)
                    .peek(
                        (res) -> HbaseStatsDAO.LOGGER.info(
                            "Found row : {}, nb {} ",
                            new String(res.getRow()),
                            Bytes.toLong(
                                CellUtil.cloneValue(
                                    res.getColumnLatestCell(
                                        AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES,
                                        AbstractHbaseStatsDAO.COLUMN_STAT_AS_BYTES)))))
                    .mapToLong(
                        (res) -> Bytes.toLong(
                            CellUtil.cloneValue(
                                res.getColumnLatestCell(
                                    AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES,
                                    AbstractHbaseStatsDAO.COLUMN_STAT_AS_BYTES))))
                    .sum();
            }
        }
        HbaseStatsDAO.LOGGER
            .info("-> end of countImages between min {} and max {} : found {} elements ", min, max, retValue);
        return retValue;

    }

    @Override
    public long countImages(KeyEnumType type) throws IOException {
        long retValue = 0;
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().addFamily(AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES)
                .setFilter(
                    new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(type),
                            Pattern.CASE_INSENSITIVE)));
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    for (Cell statCell : res.listCells()) {
                        final long long1 = Bytes.toLong(CellUtil.cloneValue(statCell));
                        HbaseStatsDAO.LOGGER
                            .info("countImages at intervall {} at {} : {}", type, new String(res.getRow()), long1);
                        retValue = retValue + long1;
                    }
                }
            }
        }
        return retValue;
    }

    @Override
    public long countImages(String min, String max) throws IOException {
        TableName tableName = this.getHbaseDataInformation()
            .getTable();
        HbaseStatsDAO.LOGGER.info("countImages between min {} and max {} ", min, max);
        long retValue = 0;
        try (
            Table t = this.connection.getTable(tableName)) {
            Scan statsScan = new Scan().withStartRow(AbstractDAO.toBytes(min))
                .withStopRow(AbstractDAO.toBytes(max), true)
                .addFamily(AbstractHbaseStatsDAO.TABLE_FAMILY_FSTATS_AS_BYTES)
                .setFilter(
                    new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL),
                            Pattern.CASE_INSENSITIVE)));
            try (
                ResultScanner scanner = t.getScanner(statsScan)) {
                for (Result res : scanner) {
                    for (Cell statCell : res.listCells()) {
                        final long long1 = Bytes.toLong(CellUtil.cloneValue(statCell));
                        HbaseStatsDAO.LOGGER.info(
                            "countImages between min {} and max {} at {} : {}",
                            min,
                            max,
                            new String(res.getRow()),
                            long1);
                        retValue = retValue + long1;
                    }
                }
            }
        }
        return retValue;
    }

    protected OffsetDateTime getMin() {
        TableName tableName;
        Pattern p = Pattern.compile(AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL));
        OffsetDateTime retValue = OffsetDateTime.MAX;
        try {
            HbaseStatsDAO.LOGGER.info(
                "get first year, exploring {} rows ",
                this.countWithCoprocessorJob(this.getHbaseDataInformation()));
            tableName = this.getHbaseDataInformation()
                .getTable();
            try (
                Table t = this.connection.getTable(tableName)) {
                Scan statsScan = new Scan();
                Filter filter = new FilterList(
                    new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL),
                            Pattern.CASE_INSENSITIVE)),
                    new PageFilter(10));

                statsScan.setFilter(filter)
                    .withStartRow(
                        AbstractDAO.toBytes(
                            AbstractHbaseStatsDAO.toKey(DateTimeHelper.toLocalDateTime(0), KeyEnumType.ALL)
                                .get(KeyEnumType.ALL)));

                try (
                    ResultScanner scanner = t.getScanner(statsScan)) {
                    for (Result res : scanner) {
                        String key = Bytes.toString(res.getRow());
                        try {
                            OffsetDateTime ldt = this.buildOffsetDateTimeFromKey(p, key);
                            if (retValue.isAfter(ldt)) {
                                retValue = ldt;
                            }
                        } catch (Exception e) {
                            HbaseStatsDAO.LOGGER.info(
                                "Key {} does not match {}",
                                key,
                                AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL));
                        }
                    }
                    HbaseStatsDAO.LOGGER.info("get first found {} ", retValue);
                    return retValue;
                }
            }
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

    }

    protected OffsetDateTime buildOffsetDateTimeFromKey(Pattern p, String key) {
        Matcher m = p.matcher(key);
        if (m.matches()) {
            OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(0);

            for (int i = 1; i <= m.groupCount(); i++) {
                int currentValue = Integer.parseUnsignedInt(m.group(i));
                switch (i) {
                    case 1:
                        ldt = ldt.withYear(currentValue);
                        break;
                    case 2:
                        ldt = ldt.withMonth(currentValue);
                        break;
                    case 3:
                        ldt = ldt.withDayOfMonth(currentValue);
                        break;
                    case 4:
                        ldt = ldt.withHour(currentValue);
                        break;
                    case 5:
                        ldt = ldt.withMinute(currentValue);
                        break;
                    case 6:
                        ldt = ldt.withSecond(currentValue);
                        break;
                }
            }
            return ldt;
        } else {
            throw new RuntimeException("Unable to parse " + key);
        }
    }

    protected OffsetDateTime getMax() {
        HbaseStatsDAO.LOGGER.info("get last year");
        TableName tableName;
        Pattern p = Pattern.compile(AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL));
        OffsetDateTime retValue = OffsetDateTime.MIN;
        try {
            HbaseStatsDAO.LOGGER
                .info("get max year, exploring {} rows ", this.countWithCoprocessorJob(this.getHbaseDataInformation()));

            tableName = this.getHbaseDataInformation()
                .getTable();
            try (
                Table t = this.connection.getTable(tableName)) {
                Scan statsScan = new Scan().setReversed(true);
                Filter filter = new FilterList(
                    new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator(AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL),
                            Pattern.CASE_INSENSITIVE)),
                    new PageFilter(10));
                statsScan.setFilter(filter)
                    .withStartRow(
                        AbstractDAO.toBytes(
                            AbstractHbaseStatsDAO.toKey(DateTimeHelper.toLocalDateTime(Long.MAX_VALUE), KeyEnumType.ALL)
                                .get(KeyEnumType.ALL)));
                try (
                    ResultScanner scanner = t.getScanner(statsScan)) {
                    for (Result res : scanner) {
                        String key = Bytes.toString(res.getRow());
                        HbaseStatsDAO.LOGGER.info("get last year, exam {} vs {} ", key, retValue);
                        try {
                            OffsetDateTime ldt = this.buildOffsetDateTimeFromKey(p, key);
                            if (retValue.isBefore(ldt)) {
                                retValue = ldt;
                            }
                        } catch (Exception e) {
                            HbaseStatsDAO.LOGGER.info(
                                "getMax -> Key {} does not match {}",
                                key,
                                AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.ALL));
                        }
                    }
                    HbaseStatsDAO.LOGGER.info("get last found {} ", retValue);
                    return retValue;
                }
            }
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void decrement(OffsetDateTime creationDate) { // TODO Auto-generated method stub
    }

    @Override
    public void delete(HbaseImageThumbnailKey hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                         // stub
    }

    public HbaseStatsDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
