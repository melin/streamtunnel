package com.github.dzlog.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * @author melin 2021/7/19 5:25 下午
 */
public class TimeUtils {
    private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMddHH");

    private static DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int unit = 15;

    public static String getCurrentDate() {
        LocalDateTime dateTime = LocalDateTime.now();
        LocalTime time = LocalTime.now();
        int minute = time.getMinute() / unit * unit;
        String index = minute == 0 ? "00" : Integer.toString(minute);
        String prefix = df.format(dateTime);
        return prefix + index;
    }

    public static String formatTimestamp(long timestamp) {
        LocalDateTime dateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                        TimeZone.getDefault().toZoneId());
        return timestampFormat.format(dateTime);
    }

    public static String addMinute(String dateTime, int minute) throws Exception{
        LocalDateTime time = LocalDateTime.parse(dateTime, dateFormat);
        return dateFormat.format(time.plusMinutes(minute));
    }

}
