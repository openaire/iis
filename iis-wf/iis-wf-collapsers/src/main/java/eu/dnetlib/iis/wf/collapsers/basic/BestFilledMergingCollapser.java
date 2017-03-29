package eu.dnetlib.iis.wf.collapsers.basic;

import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;

import eu.dnetlib.iis.wf.collapsers.CollapserUtils;

/**
 * Collapses objects by merging them into a single object. 
 * The order of merging is determined by the number 
 * of (significant) filled (not null) fields in objects.
 * 
 * @author Dominika Tkaczyk
 */
public class BestFilledMergingCollapser<T extends IndexedRecord> extends AbstractSignificantFieldsCollapser<T> {

    @Override
    protected List<T> collapseNonEmpty(List<T> objects) {
        CollapserUtils.sortByFilledDataFields(objects, getFields());
        T merged = objects.get(0);

        for (T object : objects) {
            merged = CollapserUtils.merge(merged, object);
            if (CollapserUtils.getNumberOfFilledFields(merged, null) 
                    == merged.getSchema().getFields().size()) {
                break;
            }
        }
        
        return Collections.singletonList(merged);
    }
      
}
