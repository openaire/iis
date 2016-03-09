package eu.dnetlib.iis.wf.export.actionmanager;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmMapper;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.MappingNotDefinedException;

/**
 * Generates properties with action set identifiers as values.
 * @author mhorst
 *
 */
public class AlgorithmToActionSetMapper {

	public static AlgorithmMapper<String> instantiate(Configuration config) {
		String defaultActionSetId = null;
		if (config.get(EXPORT_ACTION_SETID)!=null && 
				!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
						config.get(EXPORT_ACTION_SETID))) {
			defaultActionSetId = config.get(
					EXPORT_ACTION_SETID);	
		}
		final Map<AlgorithmName, String> algoToActionSetMap = new HashMap<AlgorithmName, String>();
		for (AlgorithmName currentAlgorithm : AlgorithmName.values()) {
			String propertyKey = EXPORT_ACTION_SETID + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + 
					currentAlgorithm.name();
			if (config.get(propertyKey)!=null && 
					!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
							config.get(propertyKey))) {
				algoToActionSetMap.put(currentAlgorithm,
						config.get(propertyKey));
			} else {
				algoToActionSetMap.put(currentAlgorithm,
						defaultActionSetId);
			}
		}
		return new AlgorithmMapper<String>() {
			@Override
			public String getValue(AlgorithmName algorithmName)
					throws MappingNotDefinedException {
				String actionSetId = algoToActionSetMap.get(algorithmName);
				if (actionSetId!=null && 
						!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
								actionSetId)) {
					return actionSetId;
				} else {
					throw new MappingNotDefinedException(
							"no action set identifier defined "
							+ "for algorithm: " + algorithmName.name());
				}
			}
		};
	}

}
