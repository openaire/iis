package eu.dnetlib.iis.wf.collapsers.basic;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Abstract class for collapsing objects based on the presence 
 * of (significant) filled (not null) fields.
 *
 * @author Dominika Tkaczyk
 */
public abstract class AbstractSignificantFieldsCollapser<T extends IndexedRecord> extends AbstractSimpleCollapser<T> {
    
    public static final String ALL_SIGNIFICANT_FIELDS_VALUE = "$ALL$";
    
    private List<String> fields;

    /**
     * Sets the list of significant object fields (used later during collapsing 
     * for determining the object "confidence").
     * 
     * Subclasses may override this method in order to read more parameters
     * from job configuration.
     * 
     * @param context task attempt context
     */
    @Override
    public void setup(TaskAttemptContext context) {
        if (!ALL_SIGNIFICANT_FIELDS_VALUE.equals(context.getConfiguration().get("significant_fields"))) {
            fields = Arrays.asList(context.getConfiguration().get("significant_fields").split(","));
        }
    }
    
    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
        
}
