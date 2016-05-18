package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.List;

import eu.dnetlib.actionmanager.actions.AtomicAction;

/**
 * Action builder module.
 * @author mhorst
 *
 * @param <T>
 */
public interface ActionBuilderModule<T> {

	/**
	 * Creates collection of actions.
	 * @param object avro object
	 * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
	 */
	List<AtomicAction> build(T object) throws TrustLevelThresholdExceededException;
	
}

