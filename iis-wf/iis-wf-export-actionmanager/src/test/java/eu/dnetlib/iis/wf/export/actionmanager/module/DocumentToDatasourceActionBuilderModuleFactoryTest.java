package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.datasource.schemas.DocumentToDatasource;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;
import org.junit.jupiter.api.Test;

import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class DocumentToDatasourceActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToDatasource, Relation> {


    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentToDatasourceActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToDatasourceActionBuilderModuleFactory.class, AlgorithmName.document_referencedDatasources);
    }

    // ----------------------- TESTS --------------------------

    
    @Test
    public void testBuildBelowThreshold() {
        // given
        DocumentToDatasource documentToDataset = buildDocumentToDataset("documentId", "datasourceId", 0.4f);
        ActionBuilderModule<DocumentToDatasource, Relation> module = factory.instantiate(config);
        
        // execute
        assertThrows(TrustLevelThresholdExceededException.class, () -> module.build(documentToDataset));
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String datasourceId = "datasourceId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToDatasource, Relation> module = factory.instantiate(config);

        // execute
        List<AtomicAction<Relation>> actions = module.build(buildDocumentToDataset(docId, datasourceId, matchStrength));

        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());

        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        Expectations expectations = new Expectations(docId, datasourceId, matchStrength, 
                OafConstants.REL_TYPE_RESULT_DATASOURCE, OafConstants.SUBREL_TYPE_RELATIONSHIP, 
                OafConstants.REL_CLASS_REFERENCES);
        assertOafRel(action.getPayload(), expectations);
        
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        expectations.setSource(datasourceId);
        expectations.setTarget(docId);
        expectations.setRelationClass(OafConstants.REL_CLASS_IS_REFERENCED_BY);
        assertOafRel(action.getPayload(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToDatasource buildDocumentToDataset(String docId, String datasourceId, 
            float confidenceLevel) {
        DocumentToDatasource.Builder builder = DocumentToDatasource.newBuilder();
        builder.setDocumentId(docId);
        builder.setDatasourceId(datasourceId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
