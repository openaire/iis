package eu.dnetlib.iis.wf.export.actionmanager.actionset;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import eu.dnetlib.actionmanager.rmi.ActionManagerService;
import eu.dnetlib.actionmanager.set.ActionManagerSet;
import eu.dnetlib.actionmanager.set.ActionManagerSet.ImpactTypes;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Simple process removing and recreating action set.
 * @author mhorst
 *
 */
public class ActionSetManagementProcess implements Process {
	
	private final Logger log = Logger.getLogger(this.getClass());

	public static final String PARAM_ACTION_MANAGER_SERVICE_LOCATION = "export.actionmanager.service.location";
	
	public static final String PARAM_ACTION_SET_ID = EXPORT_ACTION_SETID;

	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		String actionManagerLocation = ProcessUtils.getParameterValue(
				PARAM_ACTION_MANAGER_SERVICE_LOCATION, 
				conf, parameters);
		String actionSetId = ProcessUtils.getParameterValue(
				PARAM_ACTION_SET_ID, 
				conf, parameters);
		if (actionManagerLocation!=null 
				&& !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(actionManagerLocation) 
				&& actionSetId!=null) {
			W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
			eprBuilder.address(actionManagerLocation);
			eprBuilder.build();
			ActionManagerService actionManager = new JaxwsServiceResolverImpl().getService(
					ActionManagerService.class, eprBuilder.build());
//			checking whether given action set already exists
			List<ActionManagerSet> sets = actionManager.ListSets();
			boolean foundSet = false;
			if (sets!=null) {
				for (ActionManagerSet set : sets) {
					if (actionSetId.equals(set.getId())) {
						foundSet = true;
						break;
					}
				}	
			}
			if (!foundSet) {
//				creating new IIS action set
				ActionManagerSet set = new ActionManagerSet();
				set.setId(actionSetId);
				set.setName(actionSetId);
				set.setImpact(ImpactTypes.ONLY_INSERT);
				String createdId = actionManager.createSet(set);
				log.warn("created action set with external id: " + createdId + ","
						+ "set id/name: " + actionSetId);
			} else {
				log.warn("set " + actionSetId + " was already created");
			}
		} else {
			log.warn("skipping action set management! Either '" + 
					PARAM_ACTION_MANAGER_SERVICE_LOCATION + "' parameter or '" +
					PARAM_ACTION_SET_ID + "' parameter was not provided!");
		}
	}
}
