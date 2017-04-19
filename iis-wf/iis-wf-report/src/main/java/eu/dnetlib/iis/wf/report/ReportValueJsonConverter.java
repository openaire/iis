package eu.dnetlib.iis.wf.report;

import com.google.gson.JsonElement;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * Converter of {@link ReportEntry} value to json node 
 * 
 * @author madryk
 */
public interface ReportValueJsonConverter {

    /**
     * Returns true if {@link ReportEntry} with given {@link ReportEntryType}
     * is supported by this converter
     */
    boolean isApplicable(ReportEntryType reportEntryType);
    
    /**
     * Converts {@link ReportEntry#getValue()} to json node
     */
    JsonElement convertValue(ReportEntry reportEntry);
}
