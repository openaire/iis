package eu.dnetlib.iis.wf.export.actionmanager.module;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * Action builder factory providing {@link ActionBuilderModule} objects.
 * 
 * @author mhorst
 *
 * @param <S> avro input type
 * @param <T> target {@link Oaf} model object
 */
public interface ActionBuilderFactory<S extends SpecificRecord, T extends Oaf> {

    /**
     * Instantiates action builder module.
     * 
     * @param config hadoop configuration holding runtime parameters
     * @param agent actionmanager agent details
     * @param actionSetId actionset identifier
     */
    ActionBuilderModule<S, T> instantiate(Configuration config);

    /**
     * Provides algorithm name.
     */
    AlgorithmName getAlgorithName();
}
