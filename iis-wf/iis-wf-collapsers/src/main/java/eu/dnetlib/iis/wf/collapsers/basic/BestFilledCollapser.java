package eu.dnetlib.iis.wf.collapsers.basic;

import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;

import eu.dnetlib.iis.wf.collapsers.CollapserUtils;

/**
 * Collapses objects by choosing one representative 
 * from the list of objects to collapse.
 * The chosen object is the one with the largest number 
 * of (significant) filled (not null) fields.
 * 
 * @author Dominika Tkaczyk
 */
public class BestFilledCollapser<T extends IndexedRecord> extends AbstractSignificantFieldsCollapser<T> {

    @Override
    protected List<T> collapseNonEmpty(List<T> objects) {
        T best = objects.get(0);
        for (T object : objects) {
            if (CollapserUtils.getNumberOfFilledFields(best, getFields()) 
                    < CollapserUtils.getNumberOfFilledFields(object, getFields())) {
                best = object;
            }
        }
        
        return Collections.singletonList(best);
    }

}
