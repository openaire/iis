package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.specific.SpecificRecord;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * Abstract entity builder module.
 * 
 * @author mhorst
 *
 */
public abstract class AbstractEntityBuilderModule<S extends SpecificRecord, T extends Oaf> extends AbstractBuilderModule<S, T>{



    // ------------------------ ABSTRACT ------------------------------
    
    public AbstractEntityBuilderModule(Float trustLevelThreshold, String inferenceProvenance) {
        super(trustLevelThreshold, inferenceProvenance);
    }

    /**
     * Converts source object into {@link Oaf} result.
     */
    abstract protected T convert(S object) throws TrustLevelThresholdExceededException;
    
    /**
     * Provides result class.
     */
    abstract protected Class<T> getResultClass();
    
    
    // ------------------------ LOGIC ---------------------------------
   
    @Override
    public List<AtomicAction<T>> build(S object) throws TrustLevelThresholdExceededException {
        T result = convert(object);
        if (result != null) {
            AtomicAction<T> action = new AtomicAction<>();
            action.setClazz(getResultClass());
            action.setPayload(result);
            return Arrays.asList(action);
        } else {
            return Collections.emptyList();
        }
    }
    
}
