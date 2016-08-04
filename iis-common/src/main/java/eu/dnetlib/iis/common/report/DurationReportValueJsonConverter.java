package eu.dnetlib.iis.common.report;

import org.apache.commons.lang3.time.DurationFormatUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * Converter of {@link ReportEntry} values with {@link ReportEntryType#DURATION} 
 * type to json
 * 
 * @author madryk
 */
public class DurationReportValueJsonConverter implements ReportValueJsonConverter {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true only for {@link ReportEntryType#DURATION} type
     */
    @Override
    public boolean isApplicable(ReportEntryType reportEntryType) {
        return reportEntryType == ReportEntryType.DURATION;
    }

    /**
     * Converts {@link ReportEntry#getValue()} to json.
     * Resulting json will contain properties with duration saved in miliseconds and
     * duration ssaved in human readable format
     */
    @Override
    public JsonElement convertValue(ReportEntry reportEntry) {
        
        long duration = Long.valueOf(reportEntry.getValue().toString());
        
        JsonObject durationJson = new JsonObject();
        
        durationJson.addProperty("miliseconds", duration);
        durationJson.addProperty("humanReadable", DurationFormatUtils.formatDuration(duration, "Hh mm'm' ss's'"));
        
        return durationJson;
    }

}
