package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
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
     * Does not accept null fieldApprover or source collection. Skips null and empty values stored in this collection.
     * 
     */
    public static List<String> extractValues(Collection<StructuredProperty> source, FieldApprover fieldApprover) {
        return source.stream()
                .filter(x -> fieldApprover.approve(x.getDataInfo()))
                .map(StructuredProperty::getValue)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }
    
    /**
     * Extracts year out of the date defined in yyyy-MM-dd with possible single digit month and day part.
     */
    public static Year extractYearOrNull(String date, Logger log) {
        if (StringUtils.isNotBlank(date)) {
            if (date.indexOf('-') == 4) {
                try {
                    return Year.parse(date.substring(0, date.indexOf('-')), DateTimeFormatter.ofPattern("yyyy"));
                } catch (DateTimeParseException e) {
                    log.warn("unsupported, non integer, format of year value: " + date);
                    return null;
                }
            } else {
                log.warn("unsupported format of year value: " + date);
            }
        }
        return null;
    }
    
}
