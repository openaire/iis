package eu.dnetlib.iis.workflows.export.actionmanager.api;

import java.util.Collection;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import eu.dnetlib.actionmanager.actions.AbstractAction;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;

/**
 * Action manager service mock.
 * Prepared for testing purposes.
 * @author mhorst
 *
 */
public class ActionManagerServiceFacadeMock implements
		ActionManagerServiceFacade {

	private final Logger log = Logger.getLogger(ActionManagerServiceFacadeMock.class);
	
	private Level predefinedLevel = Level.WARN;

	@Override
	public void storeAction(
			Collection<AtomicAction> actions, Provenance provenance, String trust,
			String nsprefix) throws ActionManagerException {
		if (actions!=null) {
			for (AbstractAction action : actions) {
				log.log(predefinedLevel, "creating action: " + action.toString());		
			}
		}
	}

	@Override
	public void close() throws ActionManagerException {
		log.log(predefinedLevel, "closing action manager facade");
	}

}
