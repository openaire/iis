package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;
import org.junit.jupiter.api.Test;

import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class DocumentToDatasetActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToDataSet, Relation> {


    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentToDatasetActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToDataSetActionBuilderModuleFactory.class, AlgorithmName.document_referencedDatasets);
    }

    // ----------------------- TESTS --------------------------

    
    @Test
    public void testBuildBelowThreshold() {
        // given
        DocumentToDataSet documentToDataset = buildDocumentToDataset("documentId", "datasetId", 0.4f);
        ActionBuilderModule<DocumentToDataSet, Relation> module = factory.instantiate(config);
        
        // execute
        assertThrows(TrustLevelThresholdExceededException.class, () -> module.build(documentToDataset));
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String datasetId = "datasetId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToDataSet, Relation> module = factory.instantiate(config);

        // execute
        List<AtomicAction<Relation>> actions = module.build(buildDocumentToDataset(docId, datasetId, matchStrength));

        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());

        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        Expectations expectations = new Expectations(docId, datasetId, matchStrength, 
                "resultResult", "publicationDataset", "isRelatedTo");
        assertOafRel(action.getPayload(), expectations);
        
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        expectations.setSource(datasetId);
        expectations.setTarget(docId);
        assertOafRel(action.getPayload(), expectations);
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
