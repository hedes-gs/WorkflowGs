package com.gs.photo.workflow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateTimeHelper {

	public static final ZoneId            ZONE_ID_EUROPE_PARIS           = ZoneId.of("Europe/Paris");
	public static final DateTimeFormatter EXIF_VALUE_DATE_TIME_FORMATTER = DateTimeFormatter
		.ofPattern("yyyy:MM:dd HH:mm:ss");

	public static final String toDateTimeAsString(long epochMillis) {
		Instant instant = Instant.ofEpochMilli(epochMillis);
		OffsetDateTime odt = OffsetDateTime.ofInstant(instant,
			DateTimeHelper.ZONE_ID_EUROPE_PARIS);
		return odt.format(DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER);
	}

	public static final String toString(OffsetDateTime odt) {
		return odt.format(DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER);
	}

	public static final OffsetDateTime toOffsetDateTime(String date) {
		LocalDateTime ldt = LocalDateTime.parse(date,
			DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER);
		OffsetDateTime returnValue = ldt.atZone(DateTimeHelper.ZONE_ID_EUROPE_PARIS).toOffsetDateTime();
		return returnValue;
	}

	public static final long toEpochMillis(String date) {
		LocalDateTime ldt = LocalDateTime.parse(date,
			DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER);
		OffsetDateTime odt = ldt.atZone(DateTimeHelper.ZONE_ID_EUROPE_PARIS).toOffsetDateTime();

		return odt.toInstant().toEpochMilli();
	}

	public static OffsetDateTime toLocalDateTime(long epochMillis) {
		Instant instant = Instant.ofEpochMilli(epochMillis);
		OffsetDateTime odt = OffsetDateTime.ofInstant(instant,
			DateTimeHelper.ZONE_ID_EUROPE_PARIS);
		return odt;
	}

	public static long toEpochMillis(OffsetDateTime odt) {
		return odt.toInstant().toEpochMilli();
	}

}
