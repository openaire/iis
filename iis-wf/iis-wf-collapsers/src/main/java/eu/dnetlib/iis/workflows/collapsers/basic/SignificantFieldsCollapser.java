package eu.dnetlib.iis.workflows.collapsers.basic;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * Abstract class for collapsing objects based on the presence 
 * of (significant) filled (not null) fields.
 *
 * @author Dominika Tkaczyk
 */
public abstract class SignificantFieldsCollapser<T extends IndexedRecord> extends SimpleCollapser<T> {
    
    public static final String ALL_SIGNIFICANT_FIELDS_VALUE = "$ALL$";
    
    protected List<String> fields;

    /**
     * Sets the list of significant object fields (used later during collapsing 
     * for determining the object "confidence").
     * 
     * Subclasses may override this method in order to read more parameters
     * from job configuration.
     * 
     * @param configuration job configuration
     */
    @Override
    public void setup(Configuration configuration) {
        if (!ALL_SIGNIFICANT_FIELDS_VALUE.equals(configuration.get("significant_fields"))) {
            fields = Arrays.asList(configuration.get("significant_fields").split(","));
        }
    }
    
    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
        
}
