package com.gs.photo.workflow.hbase.dao;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.workflow.DateTimeHelper;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;

public class AbstractHbaseStatsDAOTest {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractHbaseStatsDAOTest.class);

    @Test
    public void testToKey() {

        OffsetDateTime odt = OffsetDateTime.of(
            2020,
            05,
            9,
            10,
            01,
            2,
            0,
            DateTimeHelper.ZONE_ID_EUROPE_PARIS.getRules()
                .getOffset(LocalDateTime.now()));

        List<String> listOfValue = new ArrayList<>();
        listOfValue.addAll(
            AbstractHbaseStatsDAO.toKey(odt, KeyEnumType.ALL)
                .values());
        odt = odt.plusMinutes(5);
        listOfValue.addAll(
            AbstractHbaseStatsDAO.toKey(odt, KeyEnumType.ALL)
                .values());
        odt = odt.plusYears(1);
        listOfValue.addAll(
            AbstractHbaseStatsDAO.toKey(odt, KeyEnumType.ALL)
                .values());
        odt = odt.plusYears(1);
        listOfValue.addAll(
            AbstractHbaseStatsDAO.toKey(odt, KeyEnumType.ALL)
                .values());

        Collections.sort(listOfValue);

        listOfValue.forEach((s) -> AbstractHbaseStatsDAOTest.LOGGER.info(" found '{}' ", s));
    }

    @Test
    public void test000_shouldGet6groupsWhenRegexIsSeconde() {
        String mn = "Y:2022/M:05/D:09/H:10/Mn:06/S:02";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.SECOND);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(6, m.groupCount());
        Assert.assertEquals("2022", m.group(1));
        Assert.assertEquals("05", m.group(2));
        Assert.assertEquals("09", m.group(3));
        Assert.assertEquals("10", m.group(4));
        Assert.assertEquals("06", m.group(5));
        Assert.assertEquals("02", m.group(6));
    }

    @Test
    public void test001_shouldGet5groupsWhenRegexIsMinute() {
        String mn = "Y:2020/M:05/D:09/H:10/Mn:01     ";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.MINUTE);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(5, m.groupCount());
        Assert.assertEquals("2020", m.group(1));
        Assert.assertEquals("05", m.group(2));
        Assert.assertEquals("09", m.group(3));
        Assert.assertEquals("10", m.group(4));
        Assert.assertEquals("01", m.group(5));
    }

    @Test
    public void test002_shouldGet4groupsWhenRegexIsHour() {
        String mn = "Y:2022/M:05/D:09/H:10           ";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.HOUR);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(4, m.groupCount());
        Assert.assertEquals("2022", m.group(1));
        Assert.assertEquals("05", m.group(2));
        Assert.assertEquals("09", m.group(3));
        Assert.assertEquals("10", m.group(4));
    }

    @Test
    public void test003_shouldGet3groupsWhenRegexIsDay() {
        String mn = "Y:2022/M:05/D:09                ";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.DAY);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(3, m.groupCount());
        Assert.assertEquals("2022", m.group(1));
        Assert.assertEquals("05", m.group(2));
        Assert.assertEquals("09", m.group(3));
    }

    @Test
    public void test004_shouldGet2groupsWhenRegexIsMonth() {
        String mn = "Y:2022/M:05                     ";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.MONTH);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(2, m.groupCount());
        Assert.assertEquals("2022", m.group(1));
        Assert.assertEquals("05", m.group(2));
    }

    @Test
    public void test005_shouldGet1groupsWhenRegexIsYear() {
        String mn = "Y:2022                          ";
        Assert.assertEquals(mn.length(), AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL));
        String regex = AbstractHbaseStatsDAO.getRegexForIntervall(KeyEnumType.YEAR);
        AbstractHbaseStatsDAOTest.LOGGER.info("... {} ", regex);
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mn);
        Assert.assertTrue(m.matches());
        Assert.assertEquals(1, m.groupCount());
        Assert.assertEquals("2022", m.group(1));
    }

}
