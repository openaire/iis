package eu.dnetlib.iis.wf.export.actionmanager.module;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.export.schemas.Concept;
import eu.dnetlib.iis.export.schemas.DocumentToConceptIds;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author mhorst
 *
 */
public abstract class AbstractDocumentToConceptsActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToConceptIds, Result> {

    // ----------------------- CONSTRUCTORS -------------------
    
    public AbstractDocumentToConceptsActionBuilderModuleFactoryTest(
            Class<? extends ActionBuilderFactory<DocumentToConceptIds, Result>> factoryClass, AlgorithmName expectedAlgorithmName) throws Exception {
        super(factoryClass, expectedAlgorithmName);
    }
    
    // ----------------------- TESTS --------------------------

    @Test
    public void testBuildEmptyConcepts() throws Exception {
     // given
        String docId = "documentId";
        ActionBuilderModule<DocumentToConceptIds, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(
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
        ActionBuilderModule<DocumentToConceptIds, Result> module =  factory.instantiate(config);
        
        // execute
        List<AtomicAction<Result>> actions = module.build(buildDocumentToConceptIds(docId, conceptId, confidenceLevel));

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction<Result> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Result.class, action.getClazz());
        assertOaf(action.getPayload(), docId, conceptId, confidenceLevel);
    }

    // ----------------------- PRIVATE --------------------------

    private static DocumentToConceptIds buildDocumentToConceptIds(String docId, String conceptId, float confidenceLevel) {
        DocumentToConceptIds.Builder builder = DocumentToConceptIds.newBuilder();
        builder.setDocumentId(docId);
        Concept concept = Concept.newBuilder().setId(conceptId).setConfidenceLevel(confidenceLevel).build();
        builder.setConcepts(Lists.newArrayList(concept));
        return builder.build();
    }

    private void assertOaf(Result result, String docId, String contextId, float confidenceLevel) throws InvalidProtocolBufferException {
        assertNotNull(result);
        assertEquals(docId, result.getId());

        assertEquals(1, result.getContext().size());
        Context context = result.getContext().get(0);
        assertNotNull(context);
        assertEquals(contextId, context.getId());
        assertNotNull(context.getDataInfo().get(0));

        float normalizedTrust = confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(context.getDataInfo().get(0).getTrust()), 0.0001);
    }

}
