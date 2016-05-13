package eu.dnetlib.iis.wf.importer.infospace;

import org.apache.hadoop.mapreduce.JobContext;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * Set of information space importer utilities.
 * 
 * @author mhorst
 *
 */
public class ImportInformationSpaceUtils {

    private ImportInformationSpaceUtils() {}
    
    
    /**
     * Retrieves parameter from context when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     */
    public static String getParamValue(String paramName, JobContext context) {
        String paramValue = context.getConfiguration().get(paramName);
        if (paramValue != null && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(paramValue)) {
            return paramValue;
        } else {
            return null;
        }
    }
    
}
