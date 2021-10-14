package com.github.dzlog.util;

import org.springframework.data.util.Pair;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author melin 2021/7/19 5:25 下午
 */
public class TimeUtils {
    private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMddHH");

    private static DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int PARTTIION_UNIT = 15;

    private static final Long JULIAN_DAY_OF_EPOCH = 2440588L;

    private static final long HOURS_PER_DAY = 24L;

    public static final long MINUTES_PER_HOUR = 60L;

    public static final long SECONDS_PER_MINUTE = 60L;

    public static final long MICROS_PER_MILLIS = 1000L;

    public static final long MILLIS_PER_SECOND = 1000L;

    public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS;

    public static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;

    private static final long MICROS_PER_HOUR = MINUTES_PER_HOUR * MICROS_PER_MINUTE;

    private static final long MICROS_PER_DAY = HOURS_PER_DAY * MICROS_PER_HOUR;

    /**
     * 当前分区值：yyyyMMddHHmm
     * @return
     */
    public static String getCurrentHivePartition() {
        LocalDateTime dateTime = LocalDateTime.now();
        LocalTime time = LocalTime.now();
        int minute = time.getMinute() / PARTTIION_UNIT * PARTTIION_UNIT;
        String index = minute == 0 ? "00" : Integer.toString(minute);
        String prefix = df.format(dateTime);
        return prefix + index;
    }

    public static String formatTimestamp(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                        TimeZone.getDefault().toZoneId());
        return timestampFormat.format(dateTime);
    }

    public static Pair<Integer, Long> toJulianDay(Long micros) {
        long julianUs = micros + JULIAN_DAY_OF_EPOCH * MICROS_PER_DAY;
        long days = julianUs / MICROS_PER_DAY;
        long us = julianUs % MICROS_PER_DAY;
        return Pair.of(Long.valueOf(days).intValue(), TimeUnit.MICROSECONDS.toNanos(us));
    }

    public static String addMinute(String dateTime, int minute) throws Exception{
        LocalDateTime time = LocalDateTime.parse(dateTime, dateFormat);
        return dateFormat.format(time.plusMinutes(minute));
    }

}
