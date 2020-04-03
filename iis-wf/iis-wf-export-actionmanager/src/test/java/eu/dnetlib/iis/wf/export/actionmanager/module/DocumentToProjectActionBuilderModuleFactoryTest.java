package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;

/**
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToProject, Relation> {

    // ----------------------- CONSTRUCTORS --------------------------

    public DocumentToProjectActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToProjectActionBuilderModuleFactory.class, AlgorithmName.document_referencedProjects);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        DocumentToProject documentToProject = buildDocumentToProject("documentId", "projectId", 0.4f);
        ActionBuilderModule<DocumentToProject, Relation> module = factory.instantiate(config);
        
        // execute
        module.build(documentToProject);
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String projectId = "projectId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToProject, Relation> module = factory.instantiate(config);
        
        // execute
        List<AtomicAction<Relation>> actions = module.build(buildDocumentToProject(docId, projectId, matchStrength));
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        Expectations expectations = new Expectations(docId, projectId, matchStrength, 
                "resultProject", "outcome", "isProducedBy");
        assertOafRel(action.getPayload(), expectations);
        
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        expectations.setSource(projectId);
        expectations.setTarget(docId);
        expectations.setRelationClass("produces");
        assertOafRel(action.getPayload(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToProject buildDocumentToProject(String docId, String projectId, 
            float confidenceLevel) {
        DocumentToProject.Builder builder = DocumentToProject.newBuilder();
        builder.setDocumentId(docId);
        builder.setProjectId(projectId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
