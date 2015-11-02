package eu.dnetlib.iis.workflows.collapsers.origins;

import java.util.List;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import com.google.common.collect.Lists;

/**
 * Collapses a map of objects coming from various origins
 * by choosing one representative from the origin with 
 * the largest confidence.
 * 
 * @author Dominika Tkaczyk
 */
public class OriginConfidenceCollapser<S extends IndexedRecord, T extends IndexedRecord> extends OriginCollapser<S, T> {

    @Override
    protected List<T> collapseBetweenOrigins(Map<String, List<T>> objects) {
        for (String origin : origins) {
            if (objects.get(origin) != null && !objects.get(origin).isEmpty()) {
                return Lists.newArrayList(objects.get(origin).get(0));
            }
        }     
        return null;
    }

}
