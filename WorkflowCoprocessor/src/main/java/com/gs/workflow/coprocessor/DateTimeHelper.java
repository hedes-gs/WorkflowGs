package com.gs.workflow.coprocessor;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeHelper {

    private static final int      KEY_MONTH_LENGTH = 5;
    private static final int      KEY_DAY_LENGTH   = 5;
    private static final int      KEY_HOUR_LENGTH  = 5;
    private static final int      KEY_MN_LENGTH    = 6;
    private static final int      KEY_SEC_LENGTH   = 5;
    private static final int      KEY_YEAR_LENGTH  = 6;

    protected static final Logger LOGGER           = LoggerFactory.getLogger(DateTimeHelper.class);

    public static enum KeyEnumType {
        ALL, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    };

    protected static final String                   KEY_YEAR_PREFIX   = "Y:";
    protected static final String                   KEY_MONTH_PREFIX  = DateTimeHelper.KEY_YEAR_PREFIX + "/M:";
    protected static final String                   KEY_DAY_PREFIX    = DateTimeHelper.KEY_MONTH_PREFIX + "/D:";
    protected static final String                   KEY_HOUR_PREFIX   = DateTimeHelper.KEY_DAY_PREFIX + "/H:";
    protected static final String                   KEY_MINUTE_PREFIX = DateTimeHelper.KEY_HOUR_PREFIX + "/Mn:";
    protected static final String                   KEY_SECOND_PREFIX = DateTimeHelper.KEY_MINUTE_PREFIX + "/S:";

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
                                                                                  DateTimeHelper.YEAR_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.MONTH,
                                                                                  DateTimeHelper.MONTH_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.DAY,
                                                                                  DateTimeHelper.DAY_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.HOUR,
                                                                                  DateTimeHelper.HOUR_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.MINUTE,
                                                                                  DateTimeHelper.MINUTE_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.SECOND,
                                                                                  DateTimeHelper.SECOND_REGEXP);
                                                                              this.put(
                                                                                  KeyEnumType.ALL,
                                                                                  DateTimeHelper.SECOND_REGEXP);

                                                                          }
                                                                      };

    public static int getKeyLength(KeyEnumType keyType) {
        switch (keyType) {
            case SECOND:
            case ALL:
                return DateTimeHelper.KEY_YEAR_LENGTH + DateTimeHelper.KEY_MONTH_LENGTH + DateTimeHelper.KEY_DAY_LENGTH
                    + DateTimeHelper.KEY_HOUR_LENGTH + DateTimeHelper.KEY_MN_LENGTH + DateTimeHelper.KEY_SEC_LENGTH;
            case MINUTE:
                return DateTimeHelper.KEY_YEAR_LENGTH + DateTimeHelper.KEY_MONTH_LENGTH + DateTimeHelper.KEY_DAY_LENGTH
                    + DateTimeHelper.KEY_HOUR_LENGTH + DateTimeHelper.KEY_MN_LENGTH;
            case HOUR:
                return DateTimeHelper.KEY_YEAR_LENGTH + DateTimeHelper.KEY_MONTH_LENGTH + DateTimeHelper.KEY_DAY_LENGTH
                    + DateTimeHelper.KEY_HOUR_LENGTH;
            case DAY:
                return DateTimeHelper.KEY_YEAR_LENGTH + DateTimeHelper.KEY_MONTH_LENGTH + DateTimeHelper.KEY_DAY_LENGTH;
            case MONTH:
                return DateTimeHelper.KEY_YEAR_LENGTH + DateTimeHelper.KEY_MONTH_LENGTH;
            case YEAR:
                return DateTimeHelper.KEY_YEAR_LENGTH;
            default: {
                throw new IllegalArgumentException(keyType + " not supported");
            }
        }
    }

    public static Map<KeyEnumType, String> toKey(OffsetDateTime ldt, KeyEnumType... types) {
        int maxLength = DateTimeHelper.getKeyLength(KeyEnumType.ALL);
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

    public static final String getRegexForIntervall(KeyEnumType type) { return DateTimeHelper.TO_REGEXP.get(type); }

    public static OffsetDateTime toLocalDateTime(long epochMillis) {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        OffsetDateTime odt = OffsetDateTime.ofInstant(instant, ZoneId.of("Europe/Paris"));
        return odt;
    }

    public static byte[] toBytes(String imageId) { return imageId.getBytes(Charset.forName("UTF-8")); }

}
