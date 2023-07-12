package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.service.schemas.DocumentToService;
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
public class DocumentToServiceActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToService, Relation> {


    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentToServiceActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToServiceActionBuilderModuleFactory.class, AlgorithmName.document_eoscServices);
    }

    // ----------------------- TESTS --------------------------

    
    @Test
    public void testBuildBelowThreshold() {
        // given
    	DocumentToService documentToDataset = buildDocumentToDataset("documentId", "serviceId", 0.4f);
        ActionBuilderModule<DocumentToService, Relation> module = factory.instantiate(config);
        
        // execute
        assertThrows(TrustLevelThresholdExceededException.class, () -> module.build(documentToDataset));
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String serviceId = "serviceId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToService, Relation> module = factory.instantiate(config);

        // execute
        List<AtomicAction<Relation>> actions = module.build(buildDocumentToDataset(docId, serviceId, matchStrength));

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());

        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        Expectations expectations = new Expectations(docId, serviceId, matchStrength, 
                OafConstants.REL_TYPE_RESULT_SERVICE, OafConstants.SUBREL_TYPE_RELATIONSHIP, 
                OafConstants.REL_CLASS_ISRELATEDTO);
        assertOafRel(action.getPayload(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToService buildDocumentToDataset(String docId, String serviceId, 
            float confidenceLevel) {
    	DocumentToService.Builder builder = DocumentToService.newBuilder();
        builder.setDocumentId(docId);
        builder.setServiceId(serviceId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
