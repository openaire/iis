package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.wf.export.actionmanager.IdentifierFactory;
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
public class DocumentToPatentActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToPatent, Relation> {


    // ----------------------- CONSTRUCTORS -------------------
    
    public DocumentToPatentActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToPatentActionBuilderModuleFactory.class, AlgorithmName.document_patent);
    }

    // ----------------------- TESTS --------------------------

    
    @Test
    public void testBuildBelowThreshold() {
        // given
        DocumentToPatent documentToPatent = buildDocumentToPatent("documentId", "patentId", 0.4f);
        ActionBuilderModule<DocumentToPatent, Relation> module = factory.instantiate(config);
        
        // execute
        assertThrows(TrustLevelThresholdExceededException.class, () -> module.build(documentToPatent));
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String lensId = "lensId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToPatent, Relation> module = factory.instantiate(config);

        // execute
        List<AtomicAction<Relation>> actions = module.build(buildDocumentToPatent(docId, lensId, matchStrength));

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());

        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        String patentId = "50|lens________::" +  IdentifierFactory.md5(lensId);
        Expectations expectations = new Expectations(docId, patentId, matchStrength, 
                OafConstants.REL_TYPE_RESULT_RESULT, OafConstants.SUBREL_TYPE_RELATIONSHIP, 
                OafConstants.REL_CLASS_ISRELATEDTO);
        assertOafRel(action.getPayload(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToPatent buildDocumentToPatent(String docId, String lensId, 
            float confidenceLevel) {
        DocumentToPatent.Builder builder = DocumentToPatent.newBuilder();
        builder.setDocumentId(docId);
        builder.setLensId(lensId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
