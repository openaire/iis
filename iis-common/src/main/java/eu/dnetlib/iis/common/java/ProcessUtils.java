package eu.dnetlib.iis.common.java;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * {@link Process} related utility class.
 * @author mhorst
 *
 */
public class ProcessUtils {

    // ------------- CONSTRUCTORS ----------------
    
    private ProcessUtils() {}
    
    // ------------- LOGIC -----------------------
    
	/**
	 * Returns parameter value retrived from parameters or context.
	 * @param paramName
	 * @param hadoopConf
	 * @param parameters
	 * @return parameter value
	 */
	public static String getParameterValue(String paramName, 
			Configuration hadoopConf,
			Map<String, String> parameters) {
		if (parameters!=null && !parameters.isEmpty()) {
			String result = null;
			result = parameters.get(paramName);
			if (result!=null) {
				return result;
			}
		}
		if (hadoopConf!=null) {
			return hadoopConf.get(paramName);	
		} else {
			return null;
		}
	}
}
