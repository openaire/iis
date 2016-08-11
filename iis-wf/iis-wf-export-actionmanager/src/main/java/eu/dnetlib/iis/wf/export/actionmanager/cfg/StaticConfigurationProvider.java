package eu.dnetlib.iis.wf.export.actionmanager.cfg;

import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Agent.AGENT_TYPE;

/**
 * Static configuration provider.
 * @author mhorst
 *
 */
public class StaticConfigurationProvider {
	
	public static final String ACTION_TRUST_0_9 = "0.9";
	
	public static final Agent AGENT_DEFAULT = new Agent("iis", "information inference service", AGENT_TYPE.service);

	
}
