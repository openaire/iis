package eu.dnetlib.iis.common;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * Utility class holding parameter names and method simplifying access to parameters from hadoop context.
 * @author mhorst
 *
 */
public abstract class WorkflowRuntimeParameters {

	private WorkflowRuntimeParameters() {}
	
	public static final char DEFAULT_CSV_DELIMITER = ',';
	
	public static final String UNDEFINED_NONEMPTY_VALUE = "$UNDEFINED$";
	
	/**
     * Retrieves parameter from context when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     */
    public static String getParamValue(String paramName, JobContext context) {
        String paramValue = context.getConfiguration().get(paramName);
        if (paramValue != null && !UNDEFINED_NONEMPTY_VALUE.equals(paramValue)) {
            return paramValue;
        } else {
            return null;
        }
    }
	
}
