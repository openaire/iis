package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.List;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * Action builder module.
 * 
 * @author mhorst
 *
 * @param <S> avro input type
 * @param <T> target {@link Oaf} model object
 */
public interface ActionBuilderModule<S extends SpecificRecord , T extends Oaf> {

    /**
     * Creates collection of actions.
     * 
     * @param object avro input object
     * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
     */
    List<AtomicAction<T>> build(S object) throws TrustLevelThresholdExceededException;

}
