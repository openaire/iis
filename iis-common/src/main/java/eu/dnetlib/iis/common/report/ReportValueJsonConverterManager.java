package eu.dnetlib.iis.common.report;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;

import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Converter of {@link ReportEntry} values to json using list
 * of {@link ReportValueJsonConverter}
 * 
 * @author madryk
 */
public class ReportValueJsonConverterManager {

    private List<ReportValueJsonConverter> converters = ImmutableList.of(
            new CounterReportValueJsonConverter(), 
            new DurationReportValueJsonConverter());
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link ReportEntry#getValue()} to json using first
     * applicable underlying {@link ReportValueJsonConverter}.
     * 
     * @see #setConverters(List)
     */
    public JsonElement convertValue(ReportEntry reportEntry) {
        
        for (ReportValueJsonConverter converter : converters) {
            if (converter.isApplicable(reportEntry.getType())) {
                return converter.convertValue(reportEntry);
            }
        }
        
        throw new IllegalArgumentException("No suitable converter of report entry with type " + reportEntry.getType());
    }


    //------------------------ SETTERS --------------------------
    
    public void setConverters(List<ReportValueJsonConverter> converters) {
        this.converters = converters;
    }
}
