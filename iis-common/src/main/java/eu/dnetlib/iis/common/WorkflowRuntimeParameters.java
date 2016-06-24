package eu.dnetlib.iis.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

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
     * Retrieves parameter from hadoop context configuration when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     */
    public static String getParamValue(String paramName, Configuration configuration) {
        String paramValue = configuration.get(paramName);
        if (StringUtils.isNotBlank(paramValue) && !UNDEFINED_NONEMPTY_VALUE.equals(paramValue)) {
            return paramValue;
        } else {
            return null;
        }
    }
	
    /**
     * Retrieves {@link Integer} parameter from hadoop context configuration when set to non-empty value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     * Null is returned when parameter was not set.
     * @throws {@link NumberFormatException} if parameter value does not contain a parsable integer
     */
    public static Integer getIntegerParamValue(String paramName, Configuration configuration) throws NumberFormatException {
        String paramValue = configuration.get(paramName);
        if (StringUtils.isNotBlank(paramValue) && !UNDEFINED_NONEMPTY_VALUE.equals(paramValue)) {
            return Integer.parseInt(paramValue);
        } else {
            return null;
        }
    }
    
    /**
     * Retrieves parameter from hadoop context configuration when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     * If requested parameter was not set, fallback parameter is retrieved using the same logic.
     */
    public static String getParamValue(String paramName, String fallbackParamName, Configuration configuration) {
        String resultCandidate = getParamValue(paramName, configuration);
        return resultCandidate!=null?resultCandidate:getParamValue(fallbackParamName, configuration);
    }
    
}
