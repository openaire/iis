package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * Test class for {@link BuilderModuleHelper}.
 * @author mhorst
 */
public class BuilderModuleHelperTest {
    
    String collectedFromKey = "some-repo-id";
    String relType = "someRelType"; 
    String subRelType = "someSubRelType";
    String relClass = "someRelClass";
    
    float confidenceLevel = 0.4f;
    String inferenceProvenance = "some-inference-provenance";

    @Test
    public void testBuildCollectedFromKey() throws Exception {
        // execute
        KeyValue result = BuilderModuleHelper.buildCollectedFromKeyValue(collectedFromKey);
        
        // assert        
        assertNotNull(result);
        assertEquals(StaticConfigurationProvider.COLLECTED_FROM_VALUE, result.getValue());
        assertEquals(collectedFromKey, result.getKey());
    }
    
    @Test
    public void testBuildWithNullCollectedFromKey() throws Exception {
        // execute
        KeyValue result = BuilderModuleHelper.buildCollectedFromKeyValue(null);
        
        // assert
        assertNotNull(result);
        assertEquals(StaticConfigurationProvider.COLLECTED_FROM_VALUE, result.getValue());
        assertNull(result.getKey());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelShort() throws Exception {
        // given
        String trustLevel = "0.1";
        
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(trustLevel, inferenceProvenance);
        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals(trustLevel, result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelShortWithNullProvenance() throws Exception {
        // given
        String trustLevel = "0.1";
        
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(trustLevel, null);
        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals(trustLevel, result.getTrust());
        assertNull(result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelShortWithNullTrustLevel() throws Exception {
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(null, inferenceProvenance);
        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertNull(result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevel() throws Exception {
        // given
        boolean inferred = true;
        String trustLevel = "0.1";
        String provenanceClass = "someProvenanceClass";
        
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(inferred, trustLevel, inferenceProvenance, provenanceClass);
        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals(trustLevel, result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassid());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelWithNullProvenance() throws Exception {
        // given
        boolean inferred = false;
        String trustLevel = "0.1";
        String provenanceClass = "someProvenanceClass";
        
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(inferred, trustLevel, null, provenanceClass);
        // assert
        assertNotNull(result);
        assertFalse(result.getInferred());
        assertEquals(trustLevel, result.getTrust());
        assertNull(result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassid());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelWithNullTrustLevel() throws Exception {
        // given
        boolean inferred = false;
        String provenanceClass = "someProvenanceClass";
        
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(inferred, null, inferenceProvenance, provenanceClass);
        // assert
        assertNotNull(result);
        assertFalse(result.getInferred());
        assertNull(result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassid());
        assertEquals(provenanceClass, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForTrustLevelWithNullProvenanceClass() throws Exception {
        // given
        boolean inferred = false;
        String trustLevel = "0.1";

        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForTrustLevel(inferred, trustLevel, inferenceProvenance, null);
        // assert
        assertNotNull(result);
        assertFalse(result.getInferred());
        assertEquals(trustLevel, result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertNull(result.getProvenanceaction().getClassid());
        assertNull(result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForConfidenceLevel() throws Exception {
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, inferenceProvenance);

        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals("0.36", result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForConfidenceLevelWithNegativeConfidenceLevel() throws Exception {
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForConfidenceLevel(-1, inferenceProvenance);

        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals("-0.9", result.getTrust());
        assertEquals(inferenceProvenance, result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testBuildInferenceForConfidenceLevelWithNullInferencceProvenance() throws Exception {
        // execute
        DataInfo result = BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, null);

        // assert
        assertNotNull(result);
        assertTrue(result.getInferred());
        assertEquals("0.36", result.getTrust());
        assertNull(result.getInferenceprovenance());
        assertNotNull(result.getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getProvenanceaction().getSchemename());
    }
    
    @Test
    public void testCreateRelation() throws Exception {
        // given
        String source = "srcId1";
        String target = "targetId1";
        DataInfo dataInfo = BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, inferenceProvenance);
        KeyValue keyValue = new KeyValue();
        String propertyKey = "propKey";
        String propertyValue = "propVal";
        keyValue.setKey(propertyKey);
        keyValue.setValue(propertyValue);
        List<KeyValue> properties = Collections.singletonList(keyValue);
        
        // execute
        Relation result = BuilderModuleHelper.createRelation(source, target, relType, subRelType, relClass, properties,
                dataInfo, collectedFromKey);
        
        // assert        
        assertNotNull(result);
        assertEquals(source, result.getSource());
        assertEquals(target, result.getTarget());
        assertEquals(relType, result.getRelType());
        assertEquals(subRelType, result.getSubRelType());
        assertEquals(relClass, result.getRelClass());
        
        assertNotNull(result.getProperties());
        assertEquals(1, result.getProperties().size());
        assertEquals(propertyKey, result.getProperties().get(0).getKey());
        assertEquals(propertyValue, result.getProperties().get(0).getValue());
        
        assertNotNull(result.getDataInfo());
        assertEquals("0.36", result.getDataInfo().getTrust());
        assertEquals(inferenceProvenance, result.getDataInfo().getInferenceprovenance());
        assertNotNull(result.getDataInfo().getProvenanceaction());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getDataInfo().getProvenanceaction().getClassid());
        assertEquals(InfoSpaceConstants.SEMANTIC_CLASS_IIS, result.getDataInfo().getProvenanceaction().getClassname());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getDataInfo().getProvenanceaction().getSchemeid());
        assertEquals(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS, result.getDataInfo().getProvenanceaction().getSchemename());
        
        assertNotNull(result.getCollectedfrom());
        assertEquals(1, result.getCollectedfrom().size());
        assertEquals(StaticConfigurationProvider.COLLECTED_FROM_VALUE, result.getCollectedfrom().get(0).getValue());
        assertEquals(collectedFromKey, result.getCollectedfrom().get(0).getKey());
        
        assertNotNull(result.getLastupdatetimestamp());
    }
    
}
