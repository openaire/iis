package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;
import static org.junit.Assert.assertTrue;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.actionmanager.common.Agent;

/**
 * @author mhorst
 *
 */
public abstract class AbstractActionBuilderModuleFactoryTest<T extends SpecificRecordBase> {


    protected final float trustLevelThreshold = 0.5f;
    
    protected final Configuration config;

    protected final String actionSetId = "someActionSetId";

    protected final Agent agent = new Agent("agentId", "agent name", Agent.AGENT_TYPE.service);
    
    protected final AlgorithmName expectedAlgorithmName;
    
    protected final ActionBuilderFactory<T> factory;
    
    
    // -------------------------------- CONSTRUCTORS ----------------------------------
    
    public AbstractActionBuilderModuleFactoryTest(Class<? extends ActionBuilderFactory<T>> factoryClass, 
            AlgorithmName expectedAlgorithmName) throws Exception {
        this.factory = factoryClass.getConstructor().newInstance();
        this.expectedAlgorithmName = expectedAlgorithmName;
        this.config = new Configuration();
    }
    
    // -------------------------------- TESTS -----------------------------------------

    @Before
    public void init() throws Exception {
        this.config.set(EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + expectedAlgorithmName.name(), 
                String.valueOf(trustLevelThreshold));
    }
    
    @Test
    public void testGetAlgorithmName() {
        // execute & assert
        assertTrue(expectedAlgorithmName == factory.getAlgorithName());
    }
    
    @Test(expected = NullPointerException.class)
    public void testInstantiateNullAgent() throws Exception {
        // execute
        factory.instantiate(config, null, actionSetId);
    }
    
    @Test(expected = NullPointerException.class)
    public void testInstantiateNullActionSetId() throws Exception {
        // execute
        factory.instantiate(config, agent, null);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullObject() throws Exception {
        // given
        ActionBuilderModule<T> module = factory.instantiate(config, agent, actionSetId);
        // execute
        module.build(null);
    }

}
