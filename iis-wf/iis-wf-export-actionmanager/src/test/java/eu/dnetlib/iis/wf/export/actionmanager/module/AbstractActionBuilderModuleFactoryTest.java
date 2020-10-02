package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author mhorst
 *
 */
public abstract class AbstractActionBuilderModuleFactoryTest<S extends SpecificRecordBase, T extends Oaf> {


    protected final float trustLevelThreshold = 0.5f;
    
    protected final Configuration config;
    
    protected final AlgorithmName expectedAlgorithmName;
    
    protected final ActionBuilderFactory<S, T> factory;
    
    
    // -------------------------------- CONSTRUCTORS ----------------------------------
    
    public AbstractActionBuilderModuleFactoryTest(Class<? extends ActionBuilderFactory<S, T>> factoryClass, 
            AlgorithmName expectedAlgorithmName) throws Exception {
        this.factory = factoryClass.getConstructor().newInstance();
        this.expectedAlgorithmName = expectedAlgorithmName;
        this.config = new Configuration();
    }
    
    // -------------------------------- TESTS -----------------------------------------

    @BeforeEach
    public void init() {
        this.config.set(EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + expectedAlgorithmName.name(), 
                String.valueOf(trustLevelThreshold));
    }
    
    @Test
    public void testGetAlgorithmName() {
        // execute & assert
        assertSame(expectedAlgorithmName, factory.getAlgorithName());
    }
    

    @Test
    public void testBuildNullObject() {
        // given
        ActionBuilderModule<S, T> module = factory.instantiate(config);
        // execute
        assertThrows(NullPointerException.class, () -> module.build(null));
    }

}
