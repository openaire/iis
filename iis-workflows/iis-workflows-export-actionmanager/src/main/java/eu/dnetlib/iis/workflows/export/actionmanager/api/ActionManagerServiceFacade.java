package eu.dnetlib.iis.workflows.export.actionmanager.api;

import java.util.Collection;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;

/**
 * Direct action manager service facade.
 * @author mhorst
 *
 */
public interface ActionManagerServiceFacade {

	
	/**
	 * Stores actions in action manager.
	 * @param actions
	 * @param provenance
	 * @param trust
	 * @param nsprefix
	 * @return number ofactions successfully stored
	 * @throws ActionManagerException
	 */
	void storeAction(
			Collection<AtomicAction> actions,
			Provenance provenance,
			String trust,
			String nsprefix) throws ActionManagerException;
	
	/**
	 * Stores {@link XsltInfoPackageAction} in action manager.
	 * @param action
	 * @throws ActionManagerException
	 */
	void storeAction(XsltInfoPackageAction action) throws ActionManagerException;
	
	/**
	 * Performs finalization operations over action manager.
	 * @throws ActionManagerException
	 */
	void close() throws ActionManagerException;
	
}
