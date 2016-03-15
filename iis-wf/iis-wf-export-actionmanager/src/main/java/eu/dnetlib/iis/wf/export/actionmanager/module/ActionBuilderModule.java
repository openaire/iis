package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.List;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;

/**
 * Action builder module.
 * @author mhorst
 *
 * @param <T>
 */
public interface ActionBuilderModule<T> {

	/**
	 * Creates collection of actions.
	 * @param object
	 * @param agent action agent
	 * @param actionSetId action set identifier
	 * This instance should be used in read-only mode.
	 * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
	 * @return collection of actions
	 */
	List<AtomicAction> build(T object, Agent agent, String actionSetId) throws TrustLevelThresholdExceededException;
	
}

