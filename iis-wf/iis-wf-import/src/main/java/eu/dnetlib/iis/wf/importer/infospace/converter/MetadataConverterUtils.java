package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

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
     * Returns pid list constructed from a {@link Result}. Pid list is constructed from {@link Instance#pid} fields if they're not empty.
     * Otherwise it's constructed from {@link Instance#alternateIdentifier} fields. In any other case an empty list is
     * returned.
     */
    public static List<StructuredProperty> extractPid(Result result) {
        if (Objects.isNull(result.getInstance())) {
            return Collections.emptyList();
        }

        List<StructuredProperty> pid = result.getInstance().stream()
                .map(x -> Optional.ofNullable(x.getPid()).map(Collection::stream))
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(pid)) {
            return pid;
        }

        return result.getInstance().stream()
                .map(x -> Optional.ofNullable(x.getAlternateIdentifier()).map(Collection::stream))
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .collect(Collectors.toList());
    }
}
