package eu.dnetlib.iis.workflows.export.actionmanager.module;

import org.apache.hadoop.conf.Configuration;

/**
 * Action builder factory providing {@link ActionBuilderModule} objects.
 * @author mhorst
 *
 * @param <T>
 */
public interface ActionBuilderFactory<T> {

	/**
	 * Instantiates action builder module.
	 * @param predefinedTrust
	 * @param trustLevelThreshold
	 * @param config hadoop configuration holding runtime parameters
	 * @return
	 */
	ActionBuilderModule<T> instantiate(String predefinedTrust, Float trustLevelThreshold, 
			Configuration config);
	/**
	 * Provides algorithm name.
	 * @return algorithm name
	 */
	AlgorithmName getAlgorithName();
	
}
