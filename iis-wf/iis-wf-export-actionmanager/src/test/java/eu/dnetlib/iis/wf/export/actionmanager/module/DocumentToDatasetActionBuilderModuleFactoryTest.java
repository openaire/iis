package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.PublicationDataset;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;

/**
 * @author mhorst
 *
 */
public class DocumentToDatasetActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToDataSet> {


    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentToDatasetActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToDataSetActionBuilderModuleFactory.class, AlgorithmName.document_referencedDatasets);
    }

    // ----------------------- TESTS --------------------------

    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        DocumentToDataSet documentToDataset = buildDocumentToDataset("documentId", "datasetId", 0.4f);
        ActionBuilderModule<DocumentToDataSet> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(documentToDataset);
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String datasetId = "datasetId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToDataSet> module = factory.instantiate(config, agent, actionSetId);

        // execute
        List<AtomicAction> actions = module.build(buildDocumentToDataset(docId, datasetId, matchStrength));

        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(datasetId, action.getTargetColumn());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(RelType.resultResult.toString() + '_' + SubRelType.publicationDataset + '_'
                + PublicationDataset.RelName.isRelatedTo, action.getTargetColumnFamily());
        
        Expectations expectations = new Expectations(docId, datasetId, matchStrength, 
                KindProtos.Kind.relation, RelType.resultResult, SubRelType.publicationDataset, 
                PublicationDataset.RelName.isRelatedTo.toString());
        assertOafRel(action.getTargetValue(), expectations);
        
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(agent, action.getAgent());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetColumn());
        assertEquals(datasetId, action.getTargetRowKey());
        assertEquals(RelType.resultResult.toString() + '_' + SubRelType.publicationDataset + '_'
                + PublicationDataset.RelName.isRelatedTo, action.getTargetColumnFamily());
        expectations.setSource(datasetId);
        expectations.setTarget(docId);
        assertOafRel(action.getTargetValue(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToDataSet buildDocumentToDataset(String docId, String datasetId, 
            float confidenceLevel) {
        DocumentToDataSet.Builder builder = DocumentToDataSet.newBuilder();
        builder.setDocumentId(docId);
        builder.setDatasetId(datasetId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
