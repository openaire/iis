package eu.dnetlib.iis.export.actionmanager.cfg;

import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Agent.AGENT_TYPE;
import eu.dnetlib.actionmanager.common.Provenance;

/**
 * Static configuration provider.
 * @author mhorst
 *
 */
public class StaticConfigurationProvider implements ActionManagerConfigurationProvider {

	public static final String NAMESPACE_PREFIX_DEFAULT = "iis";
	
	public static final String NAMESPACE_PREFIX_WOS = "webcrawl____";
	
	public static final String NAMESPACE_PREFIX_DATACITE = "datacite____";
	
	public static final Provenance PROVENANCE_DEFAULT = Provenance.sysimport_mining_repository;
	
	public static final String ACTION_TRUST_0_9 = "0.9";
	
	public static final Agent AGENT_DEFAULT = new Agent("iis", 
			"information inference service", 
			AGENT_TYPE.service);
	
	private final Agent agent;
	
	private final Provenance provenance;
	
	private final String actionTrust;
	
	private final String namespacePrefix;
	
//	/**
//	 * Default constructor.
//	 */
//	public StaticConfigurationProvider() {
//		this(AGENT_DEFAULT,
//				PROVENANCE_DEFAULT, 
//				ACTION_TRUST_DEFAULT, 
//				NAMESPACE_PREFIX_DEFAULT);
//	}
	
	/**
	 * Default constructor.
	 * @param agent
	 * @param provenance
	 * @param actionTrust
	 * @param namespacePrefix
	 */
	public StaticConfigurationProvider(Agent agent, 
			Provenance provenance, String actionTrust, String namespacePrefix) {
		this.agent = agent;
		this.provenance = provenance;
		this.actionTrust = actionTrust;
		this.namespacePrefix = namespacePrefix;
	}

	@Override
	public Agent provideAgent() {
		return agent;
	}

	@Override
	public Provenance provideProvenance() {
//		TODO set proper provenance for IIS
		return provenance;
	}
	
	@Override
	public String provideActionTrust() {
		return actionTrust;
	}

	@Override
	public String provideNamespacePrefix() {
		return namespacePrefix;
	}
	
}
