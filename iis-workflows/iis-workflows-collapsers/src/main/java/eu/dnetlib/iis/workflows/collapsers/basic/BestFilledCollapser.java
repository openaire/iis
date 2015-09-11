package eu.dnetlib.iis.workflows.collapsers.basic;

import eu.dnetlib.iis.workflows.collapsers.CollapserUtils;

import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.python.google.common.collect.Lists;

/**
 * Collapses objects by choosing one representative 
 * from the list of objects to collapse.
 * The chosen object is the one with the largest number 
 * of (significant) filled (not null) fields.
 * 
 * @author Dominika Tkaczyk
 */
public class BestFilledCollapser<T extends IndexedRecord> extends SignificantFieldsCollapser<T> {

    @Override
    protected List<T> collapseNonEmpty(List<T> objects) {
        T best = objects.get(0);
        for (T object : objects) {
            if (CollapserUtils.getNumberOfFilledFields(best, fields) 
                    < CollapserUtils.getNumberOfFilledFields(object, fields)) {
                best = object;
            }
        }
        
        return Lists.newArrayList(best);
    }

}
