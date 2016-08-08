package eu.dnetlib.iis.wf.export.actionmanager.api;

import java.io.Closeable;
import java.util.Collection;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;

/**
 * Action manager service facade.
 * 
 * @author mhorst
 *
 */
public interface ActionManagerServiceFacade extends Closeable {

	
	/**
	 * Stores actions in action manager.
	 * @param actions
	 * @param provenance
	 * @param trust
	 * @param nsprefix
	 * @throws ActionManagerException
	 */
	void storeAction(Collection<AtomicAction> actions, Provenance provenance, String trust, String nsprefix) throws ActionManagerException;
	
}
