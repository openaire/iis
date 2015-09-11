package eu.dnetlib.iis.workflows.collapsers.origins;

import eu.dnetlib.iis.workflows.collapsers.CollapserUtils;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.IndexedRecord;
import org.python.google.common.collect.Lists;

/**
 * Collapses a map of objects coming from various origins 
 * by merging them into a single object.
 * The order of merging is determined by the origin confidence.
 * 
 * @author Dominika Tkaczyk
 */
public class OriginConfidenceMergingCollapser<S extends IndexedRecord, T extends IndexedRecord> extends OriginCollapser<S, T> {

    @Override
    protected List<T> collapseBetweenOrigins(Map<String, List<T>> objects) {
        T merged = null;
        for (String origin : origins) {
            if (objects.get(origin) != null) {
                for (T record : objects.get(origin)) {
                    if (merged == null) {
                        merged = record;
                    } else {
                        merged = CollapserUtils.merge(merged, record);
                    }
                }
            }
        }
        return Lists.newArrayList(merged);
    }

}
