package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_DOCUMENTSSIMILARITY_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.Similarity;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;

/**
 * @author mhorst
 *
 */
public class DocumentSimilarityActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentSimilarity> {


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
        
        ActionBuilderModule<DocumentSimilarity> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> results =  module.build(docSim);
        
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
        ActionBuilderModule<DocumentSimilarity> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(docSim);
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(otherDocId, action.getTargetColumn());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(RelType.resultResult.toString() + '_' + SubRelType.similarity + '_'
                + Similarity.RelName.hasAmongTopNSimilarDocuments, action.getTargetColumnFamily());
        assertOaf(action.getTargetValue(), docId, otherDocId, similarity, 
                Similarity.RelName.hasAmongTopNSimilarDocuments.toString());
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(agent, action.getAgent());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetColumn());
        assertEquals(otherDocId, action.getTargetRowKey());
        assertEquals(RelType.resultResult.toString() + '_' + SubRelType.similarity + '_'
                + Similarity.RelName.isAmongTopNSimilarDocuments, action.getTargetColumnFamily());
        assertOaf(action.getTargetValue(), otherDocId, docId, similarity, 
                Similarity.RelName.isAmongTopNSimilarDocuments.toString());
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentSimilarity buildDocSim(String docId, String otherDocId, float similarity) {
        DocumentSimilarity.Builder builder = DocumentSimilarity.newBuilder();
        builder.setDocumentId(docId);
        builder.setOtherDocumentId(otherDocId);
        builder.setSimilarity(similarity);
        return builder.build();
    }

    private void assertOaf(byte[] oafBytes, String source, String target, float similarity, 
            String affiliationRelationName) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.relation == oaf.getKind());
        assertTrue(RelType.resultResult == oaf.getRel().getRelType());
        assertTrue(SubRelType.similarity == oaf.getRel().getSubRelType());
        assertEquals(affiliationRelationName, oaf.getRel().getRelClass());
        assertEquals(source, oaf.getRel().getSource());
        assertEquals(target, oaf.getRel().getTarget());

        assertNotNull(oaf.getDataInfo());

        float normalizedTrust = similarity * HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(oaf.getDataInfo().getTrust()), 0.0001);
    }

}
