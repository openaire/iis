package eu.dnetlib.iis.wf.export.actionmanager.common;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateTimeUtils {

    public static final String DATE_TIME_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static String format(LocalDateTime localDateTime) {
        return localDateTime.atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_DEFAULT));
    }

}
