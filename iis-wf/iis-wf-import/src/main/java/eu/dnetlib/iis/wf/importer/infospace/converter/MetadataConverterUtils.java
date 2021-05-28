package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Extracts year out of the date defined in 'yyyy' or 'yyyy-MM-dd' format with possible single digit month and day part.
     */
    public static Year extractYearOrNull(String date, Logger log) {
        if (StringUtils.isNotBlank(date)) {
            return parseYearOrGetNull(date.indexOf('-') == 4 ? date.substring(0, date.indexOf('-')) : date, log);
        } else {
            return null;    
        }
    }
    
    /**
     * Parses year privided in 'yyyy' format. Returns null whenever year defined in an invalid format.
     */
    private static Year parseYearOrGetNull(String year, Logger log) {
        try {
            return Year.parse(year, DateTimeFormatter.ofPattern("yyyy"));
        } catch (DateTimeParseException e) {
            log.warn("unsupported, non 'yyyy', format of year value: " + year);
            return null;
        }
    }

    /**
     * Returns pid list constructed from a {@link Result#instance#pid} and {@link Result#instance#alternateIdentifier}
     * fields with duplicates resolution.
     */
    public static List<StructuredProperty> extractPid(Result result) {
        if (Objects.isNull(result.getInstance())) {
            return Collections.emptyList();
        }

        return Stream.concat(result.getInstance().stream()
                        .map(x -> Optional.ofNullable(x.getPid()).map(Collection::stream))
                        .filter(Optional::isPresent)
                        .flatMap(Optional::get),
                result.getInstance().stream()
                        .map(x -> Optional.ofNullable(x.getAlternateIdentifier()).map(Collection::stream))
                        .filter(Optional::isPresent)
                        .flatMap(Optional::get))
                .distinct()
                .collect(Collectors.toList());
    }
}
