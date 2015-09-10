package eu.dnetlib.iis.export.actionmanager.cfg;

import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Provenance;

/**
 * Configuration provider interface.
 * @author mhorst
 *
 */
public interface ActionManagerConfigurationProvider {

	/**
	 * Provides agent.
	 * @return agent
	 */
	Agent provideAgent();
	
	/**
	 * Provides provenance.
	 * @return provenance
	 */
	Provenance provideProvenance();
	
	/**
	 * Provides action trust.
	 * @return action trust
	 */
	String provideActionTrust();
	
	/**
	 * Provides namespace prefix.
	 * @return namespace prefix
	 */
	String provideNamespacePrefix();
	
}
