package eu.dnetlib.iis.wf.export.actionmanager.common;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Common datetime related utility class.
 *
 * @author pjacewicz
 */
public class DateTimeUtils {

    public static final String DATE_TIME_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     * Formats a given local datetime with default format and UTC timezone.
     *
     * @param localDateTime LocalDateTime to format.
     * @return String representing formatted local datetime.
     */
    public static String format(LocalDateTime localDateTime) {
        return localDateTime.atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_DEFAULT));
    }

}
