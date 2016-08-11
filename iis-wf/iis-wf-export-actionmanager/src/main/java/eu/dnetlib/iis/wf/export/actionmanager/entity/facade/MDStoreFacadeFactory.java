package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import java.util.Map;

/**
 * MDStore service facade factory.
 * 
 * @author mhorst
 *
 */
public interface MDStoreFacadeFactory {

    /**
     * Instantiates {@link MDStoreFacade} for given parameters.
     */
    MDStoreFacade create(Map<String, String> parameters);
}
