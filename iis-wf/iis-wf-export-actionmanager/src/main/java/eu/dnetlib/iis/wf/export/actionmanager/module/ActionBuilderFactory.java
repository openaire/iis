package eu.dnetlib.iis.wf.export.actionmanager.module;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.common.Agent;

/**
 * Action builder factory providing {@link ActionBuilderModule} objects.
 * 
 * @author mhorst
 *
 * @param <T> avro input type
 */
public interface ActionBuilderFactory<T> {

    /**
     * Instantiates action builder module.
     * 
     * @param config hadoop configuration holding runtime parameters
     * @param agent actionmanager agent details
     * @param actionSetId actionset identifier
     */
    ActionBuilderModule<T> instantiate(Configuration config, Agent agent, String actionSetId);

    /**
     * Provides algorithm name.
     */
    AlgorithmName getAlgorithName();
}
