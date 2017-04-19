package eu.dnetlib.iis.wf.report;

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
     * Returns true for {@link ReportEntryType#COUNTER} type, false otherwise 
     */
    @Override
    public boolean isApplicable(ReportEntryType reportEntryType) {
        return reportEntryType == ReportEntryType.COUNTER;
    }

    /**
     * Converts {@link ReportEntry#getValue()} to a json primitive containing number
     */
    @Override
    public JsonElement convertValue(ReportEntry reportEntry) {
        
        long value = Long.valueOf(reportEntry.getValue().toString());
        
        return new JsonPrimitive(value);
    }

}
