package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;

/**
 * Static class with metadata conversion utilities.
 * 
 * @author mhorst
 *
 */
public abstract class MetadataConverterUtils {

    private MetadataConverterUtils() {}
    
    
    /**
     * Extracts values from {@link StructuredProperty} list. Checks DataInfo
     * element whether this piece of information should be approved.
     * 
     */
    public static List<String> extractValues(Collection<StructuredProperty> source, FieldApprover fieldApprover) {
        if (CollectionUtils.isNotEmpty(source)) {
            List<String> results = new ArrayList<String>(source.size());
            for (StructuredProperty current : source) {
                if (fieldApprover.approve(current.getDataInfo())) {
                    results.add(current.getValue());
                }
            }
            return results;
        } else {
            return Collections.emptyList();
        }
    }
    
    
    /**
     * Extracts year value from date.
     * 
     * @param date
     * @return year String value or null when provided date in invalid format
     */
    public static String extractYear(String date) {
        // expected date format: yyyy-MM-dd
        if (StringUtils.isNotBlank(date) && date.indexOf('-') == 4) {
            return date.substring(0, date.indexOf('-'));    
        } else {
            return null;
        }
    }
    
    /**
     * Extracts year integer value from date.
     * 
     * @param date
     * @param log
     * @return year int value or null when provided date in invalid format
     */
    public static Integer extractYear(String date, Logger log) {
        String year = extractYear(date);
        if (year != null) {
            try {
                return Integer.valueOf(year);    
            } catch (NumberFormatException e) {
                log.warn("unsupported, non integer, format of year value: " + date);
                return null;
            }
        } else {
            return null;
        }
    }
    
}
