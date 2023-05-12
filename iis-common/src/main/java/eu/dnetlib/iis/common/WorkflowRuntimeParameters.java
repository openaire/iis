package eu.dnetlib.iis.common;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class holding parameter names and method simplifying access to parameters from hadoop context.
 * @author mhorst
 *
 */
public final class WorkflowRuntimeParameters {

    public static final String OOZIE_ACTION_OUTPUT_FILENAME = "oozie.action.output.properties";
    
	public static final char DEFAULT_CSV_DELIMITER = ',';
	
	public static final String UNDEFINED_NONEMPTY_VALUE = "$UNDEFINED$";
	
    // default values
    public static final String DNET_SERVICE_READ_TIMEOUT_DEFAULT_VALUE = "60000";
    public static final String DNET_SERVICE_CONNECTION_TIMEOUT_DEFAULT_VALUE = "60000";
    // parameter names
    public static final String DNET_SERVICE_CLIENT_READ_TIMEOUT = "dnet.service.client.read.timeout";
    public static final String DNET_SERVICE_CLIENT_CONNECTION_TIMEOUT = "dnet.service.client.connection.timeout";

    // ----------------- CONSTRUCTORS -----------------------------
    
    private WorkflowRuntimeParameters() {}
    
    /**
	 * Returns value provided at input if not blank and different than
	 * {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}, returns null otherwise.
	 */
    public static String getValueOrNullIfNotValid(String value) {
		return (StringUtils.isNotBlank(value) && !UNDEFINED_NONEMPTY_VALUE.equals(value) ? value : null);
    }
    
	/**
     * Retrieves parameter from hadoop context configuration when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     */
    public static String getParamValue(String paramName, Configuration configuration) {
        return getValueOrNullIfNotValid(configuration.get(paramName));
    }
	
    /**
     * Retrieves {@link Integer} parameter from hadoop context configuration when set to non-empty value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     * Null is returned when parameter was not set.
     * @throws {@link NumberFormatException} if parameter value does not contain a parsable integer
     */
    public static Integer getIntegerParamValue(String paramName, Configuration configuration) throws NumberFormatException {
        String paramValue = getParamValue(paramName, configuration);
        return paramValue!=null?Integer.valueOf(paramValue):null;
    }
    
    /**
     * Retrieves parameter from hadoop context configuration when set to value different than {@link WorkflowRuntimeParameters#UNDEFINED_NONEMPTY_VALUE}.
     * If requested parameter was not set, fallback parameter is retrieved using the same logic.
     */
    public static String getParamValue(String paramName, String fallbackParamName, Configuration configuration) {
        String resultCandidate = getParamValue(paramName, configuration);
        return resultCandidate!=null?resultCandidate:getParamValue(fallbackParamName, configuration);
    }
    
    /**
     * Provides parameter value. Returns default value when entry not found among parameters.
     * 
     * @param paramName parameter name
     * @param defaultValue parameter default value to be returned when entry not found among parameters
     * @param parameters map of parameters
     */
    public static String getParamValue(String paramName, String defaultValue, Map<String, String> parameters) {
        return parameters.containsKey(paramName)?parameters.get(paramName):defaultValue;
    }
    
    /**
     * Provides parameter value. Returns default value when entry not found among parameters.
     * 
     * @param paramName parameter name
     * @param defaultValue parameter default value to be returned when entry not found among parameters
     * @param parameters map of parameters
     */
    public static String getParamValueWithUndefinedCheck(String paramName, String defaultValue, Map<String, String> parameters) {
    	return getValueOrNullIfNotValid(parameters.get(paramName));
    }
}
