package eu.dnetlib.iis.wf.export.actionmanager.module;

import com.google.protobuf.InvalidProtocolBufferException;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;

import org.junit.jupiter.api.Test;

import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_DOCUMENTSSIMILARITY_THRESHOLD;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class DocumentSimilarityActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentSimilarity, Relation> {


    private final float similarityThreshold = 0.5f;

    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentSimilarityActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentSimilarityActionBuilderModuleFactory.class, AlgorithmName.document_similarities_standard);
    }

    // ----------------------- TESTS --------------------------

    @Test
    public void testBuildBelowThreshold() throws Exception {
        // given
        DocumentSimilarity docSim = buildDocSim("docId", "otherDocId", 0.1f);
        config.set(EXPORT_DOCUMENTSSIMILARITY_THRESHOLD, String.valueOf(similarityThreshold));
        
        ActionBuilderModule<DocumentSimilarity, Relation> module = factory.instantiate(config);
        
        // execute
        List<AtomicAction<Relation>> results =  module.build(docSim);
        
        //assert
        assertTrue(results.isEmpty());
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String otherDocId = "otherDocId";
        float similarity = 1f;
        DocumentSimilarity docSim = buildDocSim(docId, otherDocId, similarity);
        ActionBuilderModule<DocumentSimilarity, Relation> module = factory.instantiate(config);
        
        // execute
        List<AtomicAction<Relation>> actions = module.build(docSim);
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        assertOaf(action.getPayload(), docId, otherDocId, similarity, 
                OafConstants.REL_CLASS_HAS_AMONG_TOP_N);
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        assertOaf(action.getPayload(), otherDocId, docId, similarity, 
                OafConstants.REL_CLASS_IS_AMONG_TOP_N);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentSimilarity buildDocSim(String docId, String otherDocId, float similarity) {
        DocumentSimilarity.Builder builder = DocumentSimilarity.newBuilder();
        builder.setDocumentId(docId);
        builder.setOtherDocumentId(otherDocId);
        builder.setSimilarity(similarity);
        return builder.build();
    }

    private void assertOaf(Relation relation, String source, String target, float similarity, 
            String affiliationRelationName) throws InvalidProtocolBufferException {
        assertNotNull(relation);

        assertEquals("resultResult", relation.getRelType());
        assertEquals("similarity", relation.getSubRelType());
        assertEquals(affiliationRelationName, relation.getRelClass());
        assertEquals(source, relation.getSource());
        assertEquals(target, relation.getTarget());

        assertNotNull(relation.getDataInfo());

        assertNotNull(relation.getProperties());
        assertEquals(1, relation.getProperties().size());
        KeyValue similarityLevel = relation.getProperties().get(0);
        assertEquals("similarityLevel", similarityLevel.getKey());
        assertEquals(BuilderModuleHelper.getDecimalFormat().format(similarity), similarityLevel.getValue());
    }

}
