package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ResultProtos.Result.Context;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;

/**
 * @author mhorst
 *
 */
public abstract class AbstractDocumentToConceptsActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToConceptIds> {

    // ----------------------- CONSTRUCTORS -------------------
    
    public AbstractDocumentToConceptsActionBuilderModuleFactoryTest(
            Class<? extends ActionBuilderFactory<DocumentToConceptIds>> factoryClass, AlgorithmName expectedAlgorithmName) throws Exception {
        super(factoryClass, expectedAlgorithmName);
    }
    
    // ----------------------- TESTS --------------------------

    @Test
    public void testBuildEmptyConcepts() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToConceptIds> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(
                DocumentToConceptIds.newBuilder().setConcepts(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }
    
    @Test
    public void testBuild() throws Exception {
     // given
        String docId = "documentId";
        String conceptId = "conceptId";
        float confidenceLevel = 1f;
        ActionBuilderModule<DocumentToConceptIds> module =  factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildDocumentToConceptIds(docId, conceptId, confidenceLevel));

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertEquals(agent, action.getAgent());
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(Type.result.toString(), action.getTargetColumnFamily());
        assertOaf(action.getTargetValue(), docId, conceptId, confidenceLevel);
    }

    // ----------------------- PRIVATE --------------------------

    private static DocumentToConceptIds buildDocumentToConceptIds(String docId, String conceptId, float confidenceLevel) {
        DocumentToConceptIds.Builder builder = DocumentToConceptIds.newBuilder();
        builder.setDocumentId(docId);
        Concept concept = Concept.newBuilder().setId(conceptId).setConfidenceLevel(confidenceLevel).build();
        builder.setConcepts(Lists.newArrayList(concept));
        return builder.build();
    }

    private void assertOaf(byte[] oafBytes, String docId, String contextId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.entity == oaf.getKind());
        assertNotNull(oaf.getEntity());
        assertEquals(docId, oaf.getEntity().getId());
        assertNotNull(oaf.getEntity().getResult());
        assertNotNull(oaf.getEntity().getResult().getMetadata());
        assertEquals(1, oaf.getEntity().getResult().getMetadata().getContextCount());
        Context context = oaf.getEntity().getResult().getMetadata().getContextList().get(0);
        assertNotNull(context);
        assertEquals(contextId, context.getId());
        assertNotNull(context.getDataInfo());

        float normalizedTrust = confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(context.getDataInfo().getTrust()), 0.0001);
    }

}
