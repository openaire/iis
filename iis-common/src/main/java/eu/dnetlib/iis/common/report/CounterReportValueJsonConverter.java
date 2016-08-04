package eu.dnetlib.iis.common.report;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * Converter of {@link ReportEntry} values with {@link ReportEntryType#COUNTER} 
 * type to json
 * 
 * @author madryk
 */
public class CounterReportValueJsonConverter implements ReportValueJsonConverter {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true only for {@link ReportEntryType#COUNTER} type
     */
    @Override
    public boolean isApplicable(ReportEntryType reportEntryType) {
        return reportEntryType == ReportEntryType.COUNTER;
    }

    /**
     * Converts {@link ReportEntry#getValue()} to primitive number json
     */
    @Override
    public JsonElement convertValue(ReportEntry reportEntry) {
        
        long value = Long.valueOf(reportEntry.getValue().toString());
        
        return new JsonPrimitive(value);
    }

}
